package api

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type principalKey struct{}

// Principal is the authenticated caller.
type Principal struct {
	Subject string
	Role    string
	Scopes  map[string]bool
}

// HasScope reports whether the caller holds scope.
func (p *Principal) HasScope(scope string) bool {
	if p == nil {
		return false
	}
	return p.Scopes["*"] || p.Scopes[scope]
}

// PrincipalFrom returns the caller stored by the auth middleware.
func PrincipalFrom(ctx context.Context) *Principal {
	p, _ := ctx.Value(principalKey{}).(*Principal)
	return p
}

// Role scopes. Service decides and reads decisions; analyst reads decisions and
// lists; admin manages rules and lists.
var RoleScopes = map[string][]string{
	"service": {"decisions:write", "decisions:read", "rules:read"},
	"analyst": {"decisions:read", "rules:read", "lists:read"},
	"admin":   {"decisions:write", "decisions:read", "rules:read", "rules:write", "lists:read", "lists:write"},
}

// AuthConfig configures HS256 JWT validation.
type AuthConfig struct {
	Secret   string
	Issuer   string
	Audience string
}

// Auth validates bearer tokens. An empty secret disables auth for local tests
// and grants the wildcard scope (documented dev-only behaviour).
type Auth struct {
	secret   []byte
	issuer   string
	audience string
	disabled bool
}

// NewAuth builds an authenticator.
func NewAuth(cfg AuthConfig) *Auth {
	return &Auth{
		secret: []byte(cfg.Secret), issuer: cfg.Issuer, audience: cfg.Audience,
		disabled: strings.TrimSpace(cfg.Secret) == "",
	}
}

// Enabled reports whether tokens are required.
func (a *Auth) Enabled() bool { return !a.disabled }

// Authenticate parses the bearer token and stores the principal.
func (a *Auth) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.disabled {
			p := &Principal{Subject: "dev", Role: "admin", Scopes: map[string]bool{"*": true}}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
			return
		}
		header := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(header, prefix) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="fade"`)
			writeProblem(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Unauthenticated", "a bearer token is required")
			return
		}
		raw := strings.TrimSpace(strings.TrimPrefix(header, prefix))
		claims := jwt.MapClaims{}
		opts := []jwt.ParserOption{
			jwt.WithValidMethods([]string{"HS256"}),
			jwt.WithExpirationRequired(),
			jwt.WithLeeway(5 * time.Second),
		}
		if a.issuer != "" {
			opts = append(opts, jwt.WithIssuer(a.issuer))
		}
		if a.audience != "" {
			opts = append(opts, jwt.WithAudience(a.audience))
		}
		parsed, err := jwt.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
			return a.secret, nil
		}, opts...)
		if err != nil || !parsed.Valid {
			w.Header().Set("WWW-Authenticate", `Bearer realm="fade", error="invalid_token"`)
			writeProblem(w, r, http.StatusUnauthorized, "INVALID_TOKEN", "Invalid token", "the bearer token is invalid or expired")
			return
		}
		role, _ := claims["role"].(string)
		if _, ok := RoleScopes[role]; !ok {
			writeProblem(w, r, http.StatusForbidden, "FORBIDDEN", "Forbidden", "token role is not recognised")
			return
		}
		p := &Principal{Subject: claimString(claims, "sub"), Role: role, Scopes: map[string]bool{}}
		for _, s := range RoleScopes[role] {
			p.Scopes[s] = true
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	})
}

func claimString(claims jwt.MapClaims, key string) string {
	s, _ := claims[key].(string)
	return s
}

// Require enforces a scope.
func (a *Auth) Require(scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !PrincipalFrom(r.Context()).HasScope(scope) {
				writeProblem(w, r, http.StatusForbidden, "FORBIDDEN", "Forbidden",
					"token is missing required scope "+scope)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// SignToken creates a dev/test token for a role.
func (a *Auth) SignToken(subject, role string, ttl time.Duration) (string, error) {
	if _, ok := RoleScopes[role]; !ok {
		return "", fmt.Errorf("unknown role %q", role)
	}
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":  subject,
		"role": role,
		"iat":  now.Unix(),
		"exp":  now.Add(ttl).Unix(),
	}
	if a.issuer != "" {
		claims["iss"] = a.issuer
	}
	if a.audience != "" {
		claims["aud"] = a.audience
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(a.secret)
}

// ConstantTimeEqual compares two secrets without leaking length timing.
func ConstantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
