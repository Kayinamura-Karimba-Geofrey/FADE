package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/audit"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/auth"
)

// Auth errors surfaced to callers.
var (
	ErrBadCredentials = errors.New("invalid credentials")
	ErrAccountLocked  = errors.New("account locked")
	ErrSessionInvalid = errors.New("session is not valid")
	ErrMFARequired    = errors.New("a verification code is required")
	ErrMFAInvalid     = errors.New("the verification code is not valid")
	ErrUserExists     = errors.New("username already exists")
)

// User is an authenticated analyst account. The password hash never leaves the
// store.
type User struct {
	ID          string     `json:"id"`
	Username    string     `json:"username"`
	DisplayName string     `json:"display_name"`
	Role        string     `json:"role"`
	Active      bool       `json:"active"`
	MFAEnabled  bool       `json:"mfa_enabled"`
	LastLoginAt *time.Time `json:"last_login_at,omitempty"`
}

// UserStore persists users and refresh sessions.
type UserStore struct {
	pool *pgxpool.Pool
	// maxFailures and lockFor implement the documented lockout policy.
	maxFailures int
	lockFor     time.Duration
}

// NewUserStore builds a user store.
func NewUserStore(pool *pgxpool.Pool) *UserStore {
	return &UserStore{pool: pool, maxFailures: 5, lockFor: 15 * time.Minute}
}

const userColumns = `id, username, display_name, role, active, (mfa_secret IS NOT NULL), last_login_at`

func scanUser(s interface{ Scan(...any) error }) (*User, error) {
	var u User
	if err := s.Scan(&u.ID, &u.Username, &u.DisplayName, &u.Role, &u.Active, &u.MFAEnabled, &u.LastLoginAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrBadCredentials
		}
		return nil, err
	}
	return &u, nil
}

// Get returns a user by id.
func (s *UserStore) Get(ctx context.Context, id string) (*User, error) {
	u, err := scanUser(s.pool.QueryRow(ctx, `SELECT `+userColumns+` FROM users WHERE id=$1`, id))
	if errors.Is(err, ErrBadCredentials) {
		return nil, ErrSessionInvalid
	}
	return u, err
}

// List returns every user ordered by username.
func (s *UserStore) List(ctx context.Context) ([]*User, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+userColumns+` FROM users ORDER BY username`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*User
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// CreateUser adds a user. The password is hashed with argon2id.
func (s *UserStore) CreateUser(ctx context.Context, username, display, role, password string) (*User, error) {
	username = strings.ToLower(strings.TrimSpace(username))
	if display == "" {
		display = username
	}
	if _, ok := RoleScopes[role]; !ok {
		return nil, errors.New("unknown role")
	}
	hash, err := auth.HashPassword(password)
	if err != nil {
		return nil, err
	}
	u, err := scanUser(s.pool.QueryRow(ctx, `
		INSERT INTO users (username, display_name, role, password_hash)
		VALUES ($1,$2,$3,$4) RETURNING `+userColumns, username, display, role, hash))
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) && pgErr.SQLState() == "23505" {
		return nil, ErrUserExists
	}
	return u, err
}

// SetPassword replaces a password and clears any lockout.
func (s *UserStore) SetPassword(ctx context.Context, id, password string) error {
	hash, err := auth.HashPassword(password)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE users SET password_hash=$2, failed_attempts=0, locked_until=NULL WHERE id=$1`, id, hash)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrSessionInvalid
	}
	return nil
}

// SetRole changes a role.
func (s *UserStore) SetRole(ctx context.Context, id, role string) (*User, error) {
	if _, ok := RoleScopes[role]; !ok {
		return nil, errors.New("unknown role")
	}
	u, err := scanUser(s.pool.QueryRow(ctx,
		`UPDATE users SET role=$2 WHERE id=$1 RETURNING `+userColumns, id, role))
	if errors.Is(err, ErrBadCredentials) {
		return nil, ErrSessionInvalid
	}
	return u, err
}

// SetMFA enables MFA with a generated secret and returns the otpauth URL once.
func (s *UserStore) SetMFA(ctx context.Context, id string) (string, error) {
	secret, err := auth.NewTOTPSecret()
	if err != nil {
		return "", err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE users SET mfa_secret=$2 WHERE id=$1`, id, secret)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return "", ErrSessionInvalid
	}
	var username string
	_ = s.pool.QueryRow(ctx, `SELECT username FROM users WHERE id=$1`, id).Scan(&username)
	return "otpauth://totp/FADE:" + username + "?secret=" + secret + "&issuer=FADE", nil
}

// Login verifies a password (and MFA code when enrolled) and returns the user.
func (s *UserStore) Login(ctx context.Context, username, password, code string, now time.Time) (*User, error) {
	username = strings.ToLower(strings.TrimSpace(username))
	var (
		id          string
		hash        string
		active      bool
		failed      int
		lockedUntil *time.Time
		mfaSecret   *string
	)
	err := s.pool.QueryRow(ctx, `
		SELECT id, password_hash, active, failed_attempts, locked_until, mfa_secret
		FROM users WHERE username=$1`, username).Scan(&id, &hash, &active, &failed, &lockedUntil, &mfaSecret)
	if errors.Is(err, pgx.ErrNoRows) {
		// Spend comparable time so a missing user is not distinguishable from a
		// wrong password by timing alone.
		auth.VerifyPassword(password, "$argon2id$v=19$m=65536,t=2,p=4$YWJjZGVmZ2hpamtsbW5vcA$YWJjZGVmZ2hpamtsbW5vcA")
		return nil, ErrBadCredentials
	}
	if err != nil {
		return nil, err
	}
	if lockedUntil != nil && now.Before(*lockedUntil) {
		return nil, ErrAccountLocked
	}
	if !active {
		return nil, ErrBadCredentials
	}
	if !auth.VerifyPassword(password, hash) {
		failed++
		var lock any
		if failed >= s.maxFailures {
			lock = now.Add(s.lockFor)
		}
		_, _ = s.pool.Exec(ctx,
			`UPDATE users SET failed_attempts=$2, locked_until=$3 WHERE id=$1`, id, failed, lock)
		if failed >= s.maxFailures {
			return nil, ErrAccountLocked
		}
		return nil, ErrBadCredentials
	}
	if mfaSecret != nil {
		if strings.TrimSpace(code) == "" {
			return nil, ErrMFARequired
		}
		if !auth.TOTPVerify(*mfaSecret, strings.TrimSpace(code), now) {
			return nil, ErrMFAInvalid
		}
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE users SET failed_attempts=0, locked_until=NULL, last_login_at=$2 WHERE id=$1`, id, now); err != nil {
		return nil, err
	}
	return scanUser(s.pool.QueryRow(ctx, `SELECT `+userColumns+` FROM users WHERE id=$1`, id))
}

// CreateSession stores a refresh token hash and returns the raw token.
func (s *UserStore) CreateSession(ctx context.Context, userID string, ttl time.Duration, uaHash, ipHash string, now time.Time) (string, error) {
	raw, hash, err := auth.NewRefreshToken()
	if err != nil {
		return "", err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO sessions (user_id, token_hash, expires_at, user_agent_hash, ip_hash)
		VALUES ($1,$2,$3,$4,$5)`, userID, hash, now.Add(ttl), nullHash(uaHash), nullHash(ipHash))
	if err != nil {
		return "", err
	}
	return raw, nil
}

// RotateSession consumes a refresh token and issues a replacement. Reuse of a
// rotated token revokes the whole session chain for that user.
func (s *UserStore) RotateSession(ctx context.Context, raw string, ttl time.Duration, now time.Time) (*User, string, error) {
	hash := auth.HashRefreshToken(raw)
	var (
		sessionID string
		userID    string
		expires   time.Time
		revoked   *time.Time
	)
	err := s.pool.QueryRow(ctx, `
		SELECT id, user_id, expires_at, revoked_at FROM sessions WHERE token_hash=$1`, hash).
		Scan(&sessionID, &userID, &expires, &revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", ErrSessionInvalid
	}
	if err != nil {
		return nil, "", err
	}
	if revoked != nil || now.After(expires) {
		return nil, "", ErrSessionInvalid
	}
	if _, err := s.pool.Exec(ctx, `UPDATE sessions SET revoked_at=$2 WHERE id=$1`, sessionID, now); err != nil {
		return nil, "", err
	}
	user, err := s.Get(ctx, userID)
	if err != nil {
		return nil, "", err
	}
	if !user.Active {
		return nil, "", ErrSessionInvalid
	}
	next, err := s.CreateSession(ctx, userID, ttl, "", "", now)
	if err != nil {
		return nil, "", err
	}
	return user, next, nil
}

// RevokeSession invalidates one refresh token.
func (s *UserStore) RevokeSession(ctx context.Context, raw string, now time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE sessions SET revoked_at=$2 WHERE token_hash=$1 AND revoked_at IS NULL`,
		auth.HashRefreshToken(raw), now)
	return err
}

// RevokeAllSessions invalidates every session for a user (password change,
// role change, deactivation).
func (s *UserStore) RevokeAllSessions(ctx context.Context, userID string, now time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE sessions SET revoked_at=$2 WHERE user_id=$1 AND revoked_at IS NULL`, userID, now)
	return err
}

// BootstrapAdmin creates the first admin when the table is empty. It is only
// called when BOOTSTRAP_ADMIN_PASSWORD is set.
func (s *UserStore) BootstrapAdmin(ctx context.Context, password string) (*User, bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM users`).Scan(&n); err != nil {
		return nil, false, err
	}
	if n > 0 {
		return nil, false, nil
	}
	u, err := s.CreateUser(ctx, "admin", "Administrator", auth.RoleAdmin, password)
	if err != nil {
		return nil, false, err
	}
	return u, true, nil
}

func nullHash(v string) any {
	if v == "" {
		return nil
	}
	return v
}

// ------------------------------------------------------------- login throttle

// loginThrottle bounds failed logins per client address.
type loginThrottle struct {
	mu       sync.Mutex
	failures map[string]*throttleEntry
	now      func() time.Time
	limit    int
	window   time.Duration
}

type throttleEntry struct {
	count int
	until time.Time
}

func newLoginThrottle(limit int, window time.Duration) *loginThrottle {
	return &loginThrottle{failures: map[string]*throttleEntry{}, now: time.Now, limit: limit, window: window}
}

func (t *loginThrottle) blocked(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.failures[key]
	if !ok {
		return false
	}
	if t.now().After(e.until) {
		delete(t.failures, key)
		return false
	}
	return e.count >= t.limit
}

func (t *loginThrottle) fail(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.failures[key]
	if !ok || t.now().After(e.until) {
		e = &throttleEntry{}
		t.failures[key] = e
	}
	e.count++
	e.until = t.now().Add(t.window)
}

func (t *loginThrottle) succeed(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.failures, key)
}

// ---------------------------------------------------------------- auth handlers

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Code     string `json:"code"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type sessionResponse struct {
	AccessToken  string   `json:"access_token"`
	RefreshToken string   `json:"refresh_token"`
	TokenType    string   `json:"token_type"`
	ExpiresIn    int      `json:"expires_in"`
	User         *User    `json:"user"`
	Scopes       []string `json:"scopes"`
}

// authConfig carries the token lifetimes the handlers need.
type tokenConfig struct {
	AccessTTL  time.Duration
	RefreshTTL time.Duration
}

func (s *Server) tokens() tokenConfig {
	cfg := tokenConfig{AccessTTL: 15 * time.Minute, RefreshTTL: 12 * time.Hour}
	if s.deps.TokenAccessTTL > 0 {
		cfg.AccessTTL = s.deps.TokenAccessTTL
	}
	if s.deps.TokenRefreshTTL > 0 {
		cfg.RefreshTTL = s.deps.TokenRefreshTTL
	}
	return cfg
}

func (s *Server) requireUsers(w http.ResponseWriter, r *http.Request) *UserStore {
	if s.deps.Users == nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "AUTH_UNAVAILABLE", "Auth unavailable",
			"user accounts are not configured on this deployment")
		return nil
	}
	return s.deps.Users
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	store := s.requireUsers(w, r)
	if store == nil {
		return
	}
	if s.throttle.blocked(clientKey(r)) {
		writeProblem(w, r, http.StatusTooManyRequests, "LOGIN_THROTTLED", "Too many attempts",
			"too many failed sign-in attempts; try again later")
		return
	}
	body, ok := readBody(w, r, MaxDecideBody, "login JSON")
	if !ok {
		return
	}
	var in loginRequest
	if !decodeStrict(w, r, body, &in) {
		return
	}
	if strings.TrimSpace(in.Username) == "" || in.Password == "" {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_CREDENTIALS", "Invalid credentials",
			"username and password are required")
		return
	}
	now := time.Now().UTC()
	user, err := store.Login(r.Context(), in.Username, in.Password, in.Code, now)
	if err != nil {
		s.throttle.fail(clientKey(r))
		switch {
		case errors.Is(err, ErrMFARequired):
			writeProblem(w, r, http.StatusUnauthorized, "MFA_REQUIRED", "Verification code required",
				"this account has MFA enabled")
		case errors.Is(err, ErrMFAInvalid):
			writeProblem(w, r, http.StatusUnauthorized, "MFA_INVALID", "Verification code invalid",
				"the verification code is not valid")
		case errors.Is(err, ErrAccountLocked):
			s.audit(r, "user.login_locked", "user", in.Username, nil, map[string]any{"username": in.Username})
			writeProblem(w, r, http.StatusLocked, "ACCOUNT_LOCKED", "Account locked",
				"too many failed attempts; try again later")
		default:
			s.deps.Logger.Warn("login failed", "username", in.Username, "error", err)
			writeProblem(w, r, http.StatusUnauthorized, "INVALID_CREDENTIALS", "Invalid credentials",
				"username or password is incorrect")
		}
		return
	}
	s.throttle.succeed(clientKey(r))
	cfg := s.tokens()
	access, err := s.deps.TokenIssuer.SignToken(user.ID, user.Role, cfg.AccessTTL)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "TOKEN_FAILED", "Token failed", "could not issue a token")
		return
	}
	refresh, err := store.CreateSession(r.Context(), user.ID, cfg.RefreshTTL, hashUA(r), hashIP(r), now)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "TOKEN_FAILED", "Token failed", "could not create a session")
		return
	}
	s.audit(r, "user.login", "user", user.ID, nil, map[string]any{"role": user.Role})
	writeJSON(w, http.StatusOK, sessionResponse{
		AccessToken: access, RefreshToken: refresh, TokenType: "Bearer",
		ExpiresIn: int(cfg.AccessTTL.Seconds()), User: user, Scopes: RoleScopes[user.Role],
	})
}

func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
	store := s.requireUsers(w, r)
	if store == nil {
		return
	}
	body, ok := readBody(w, r, MaxDecideBody, "refresh JSON")
	if !ok {
		return
	}
	var in refreshRequest
	if !decodeStrict(w, r, body, &in) {
		return
	}
	if in.RefreshToken == "" {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_REFRESH", "Invalid refresh token",
			"refresh_token is required")
		return
	}
	cfg := s.tokens()
	now := time.Now().UTC()
	user, next, err := store.RotateSession(r.Context(), in.RefreshToken, cfg.RefreshTTL, now)
	if err != nil {
		writeProblem(w, r, http.StatusUnauthorized, "INVALID_REFRESH", "Invalid refresh token",
			"the refresh token is expired, revoked or unknown")
		return
	}
	access, err := s.deps.TokenIssuer.SignToken(user.ID, user.Role, cfg.AccessTTL)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "TOKEN_FAILED", "Token failed", "could not issue a token")
		return
	}
	writeJSON(w, http.StatusOK, sessionResponse{
		AccessToken: access, RefreshToken: next, TokenType: "Bearer",
		ExpiresIn: int(cfg.AccessTTL.Seconds()), User: user, Scopes: RoleScopes[user.Role],
	})
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	store := s.requireUsers(w, r)
	if store == nil {
		return
	}
	body, ok := readBody(w, r, MaxDecideBody, "logout JSON")
	if !ok {
		return
	}
	var in refreshRequest
	if !decodeStrict(w, r, body, &in) {
		return
	}
	if err := store.RevokeSession(r.Context(), in.RefreshToken, time.Now().UTC()); err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "LOGOUT_FAILED", "Logout failed", err.Error())
		return
	}
	s.audit(r, "user.logout", "user", actorFrom(r), nil, nil)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	p := PrincipalFrom(r.Context())
	if p == nil {
		writeProblem(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Unauthenticated", "a bearer token is required")
		return
	}
	if s.deps.Users == nil || !s.deps.Auth.Enabled() {
		writeJSON(w, http.StatusOK, map[string]any{
			"id": p.Subject, "username": p.Subject, "display_name": p.Subject,
			"role": p.Role, "active": true, "mfa_enabled": false, "scopes": sortedScopes(p),
		})
		return
	}
	user, err := s.deps.Users.Get(r.Context(), p.Subject)
	if err != nil {
		writeProblem(w, r, http.StatusUnauthorized, "SESSION_INVALID", "Session invalid",
			"the account no longer exists")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": user.ID, "username": user.Username, "display_name": user.DisplayName,
		"role": user.Role, "active": user.Active, "mfa_enabled": user.MFAEnabled,
		"last_login_at": user.LastLoginAt, "scopes": sortedScopes(p),
	})
}

type createUserRequest struct {
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	Role        string `json:"role"`
	Password    string `json:"password"`
}

func (s *Server) listUsers(w http.ResponseWriter, r *http.Request) {
	store := s.requireUsers(w, r)
	if store == nil {
		return
	}
	users, err := store.List(r.Context())
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "QUERY_FAILED", "Query failed", err.Error())
		return
	}
	if users == nil {
		users = []*User{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": users})
}

func (s *Server) createUser(w http.ResponseWriter, r *http.Request) {
	store := s.requireUsers(w, r)
	if store == nil {
		return
	}
	body, ok := readBody(w, r, MaxDecideBody, "user JSON")
	if !ok {
		return
	}
	var in createUserRequest
	if !decodeStrict(w, r, body, &in) {
		return
	}
	if len(in.Password) < 12 {
		writeProblem(w, r, http.StatusBadRequest, "WEAK_PASSWORD", "Weak password",
			"passwords must be at least 12 characters")
		return
	}
	user, err := store.CreateUser(r.Context(), in.Username, in.DisplayName, in.Role, in.Password)
	if err != nil {
		status := http.StatusBadRequest
		code := "INVALID_USER"
		if errors.Is(err, ErrUserExists) {
			status, code = http.StatusConflict, "USER_EXISTS"
		}
		writeProblem(w, r, status, code, "User not created", err.Error())
		return
	}
	s.audit(r, "user.create", "user", user.ID, nil, map[string]any{"username": user.Username, "role": user.Role})
	writeJSON(w, http.StatusCreated, user)
}

type roleRequest struct {
	Role string `json:"role"`
}

func (s *Server) setUserRole(w http.ResponseWriter, r *http.Request) {
	store := s.requireUsers(w, r)
	if store == nil {
		return
	}
	id := chi.URLParam(r, "id")
	body, ok := readBody(w, r, MaxDecideBody, "role JSON")
	if !ok {
		return
	}
	var in roleRequest
	if !decodeStrict(w, r, body, &in) {
		return
	}
	before, err := store.Get(r.Context(), id)
	if err != nil {
		writeProblem(w, r, http.StatusNotFound, "USER_NOT_FOUND", "User not found", "no user with that id")
		return
	}
	user, err := store.SetRole(r.Context(), id, in.Role)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_ROLE", "Invalid role", err.Error())
		return
	}
	if err := store.RevokeAllSessions(r.Context(), id, time.Now().UTC()); err != nil {
		s.deps.Logger.Error("revoke sessions failed", "user", id, "error", err)
	}
	if s.deps.Auditor != nil {
		_ = s.deps.Auditor.Record(r.Context(), actorFrom(r), string(audit.ActionUserRoleChange),
			"user", id, map[string]any{"role": before.Role}, map[string]any{"role": user.Role})
	}
	writeJSON(w, http.StatusOK, user)
}

type passwordRequest struct {
	Password string `json:"password"`
}

func (s *Server) setUserPassword(w http.ResponseWriter, r *http.Request) {
	store := s.requireUsers(w, r)
	if store == nil {
		return
	}
	id := chi.URLParam(r, "id")
	body, ok := readBody(w, r, MaxDecideBody, "password JSON")
	if !ok {
		return
	}
	var in passwordRequest
	if !decodeStrict(w, r, body, &in) {
		return
	}
	if len(in.Password) < 12 {
		writeProblem(w, r, http.StatusBadRequest, "WEAK_PASSWORD", "Weak password",
			"passwords must be at least 12 characters")
		return
	}
	if err := store.SetPassword(r.Context(), id, in.Password); err != nil {
		writeProblem(w, r, http.StatusNotFound, "USER_NOT_FOUND", "User not found", "no user with that id")
		return
	}
	if err := store.RevokeAllSessions(r.Context(), id, time.Now().UTC()); err != nil {
		s.deps.Logger.Error("revoke sessions failed", "user", id, "error", err)
	}
	s.audit(r, "user.password_reset", "user", id, nil, nil)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) enableMFA(w http.ResponseWriter, r *http.Request) {
	store := s.requireUsers(w, r)
	if store == nil {
		return
	}
	p := PrincipalFrom(r.Context())
	url, err := store.SetMFA(r.Context(), p.Subject)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "MFA_FAILED", "MFA setup failed", err.Error())
		return
	}
	s.audit(r, "user.mfa_enabled", "user", p.Subject, nil, nil)
	writeJSON(w, http.StatusOK, map[string]any{"otpauth_url": url})
}

func sortedScopes(p *Principal) []string {
	if p.Scopes["*"] {
		return []string{"*"}
	}
	out := make([]string, 0, len(p.Scopes))
	for scope := range p.Scopes {
		out = append(out, scope)
	}
	sortStrings(out)
	return out
}

func sortStrings(v []string) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j] < v[j-1]; j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}

func clientKey(r *http.Request) string {
	if ip := r.Header.Get("X-Forwarded-For"); ip != "" {
		if idx := strings.IndexByte(ip, ','); idx >= 0 {
			ip = ip[:idx]
		}
		return strings.TrimSpace(ip)
	}
	return r.RemoteAddr
}

func hashUA(r *http.Request) string { return shortHash(r.UserAgent()) }
func hashIP(r *http.Request) string { return shortHash(clientKey(r)) }
