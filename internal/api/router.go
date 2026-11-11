package api

import (
	"context"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// Body limits.
const (
	MaxDecideBody = 1 << 20
	MaxBatchBody  = 5 << 20
	MaxRuleBody   = 1 << 20
)

// ReadinessTimeout bounds the readiness probe.
const ReadinessTimeout = 2 * time.Second

// Pinger is one readiness dependency.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Check names a readiness dependency.
type Check struct {
	Name   string
	Pinger Pinger
}

// Deps are the router dependencies.
type Deps struct {
	Logger     *slog.Logger
	Metrics    *obs.Metrics
	Pipeline   *pipeline.Pipeline
	Rules      *rules.Manager
	Auth       *Auth
	Limiter    *RateLimiter
	Auditor    Auditor
	Checks     []Check
	Version    string
	CORSOrigin string
}

// Server holds the handler dependencies.
type Server struct {
	deps Deps
}

// NewServer builds a Server.
func NewServer(d Deps) *Server {
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.Auth == nil {
		d.Auth = NewAuth(AuthConfig{})
	}
	if d.Limiter == nil {
		d.Limiter = NewRateLimiter(nil)
	}
	if d.Auditor == nil {
		d.Auditor = NoopAuditor{Logger: d.Logger}
	}
	if d.Version == "" {
		d.Version = "dev"
	}
	return &Server{deps: d}
}

// NewRouter assembles the chi router.
func NewRouter(d Deps) http.Handler {
	s := NewServer(d)
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(echoRequestID)
	r.Use(middleware.RealIP)
	r.Use(s.recoverer)
	r.Use(s.cors)
	r.Use(middleware.Compress(5, "application/json"))
	r.Use(obs.HTTPMiddleware(d.Metrics))
	r.Use(obs.RequestLogger(d.Logger))

	r.Get("/healthz", s.healthz)
	r.Get("/readyz", s.readyz)
	r.Get("/metrics", d.Metrics.Handler().ServeHTTP)
	r.Get("/version", s.version)

	r.Group(func(r chi.Router) {
		r.Use(s.deps.Auth.Authenticate)
		r.Use(s.deps.Limiter.Middleware)

		r.With(s.deps.Auth.Require("decisions:write")).Post("/v1/decide", s.decide)
		r.With(s.deps.Auth.Require("decisions:write")).Post("/v1/decide/batch", s.decideBatch)
		r.With(s.deps.Auth.Require("decisions:read")).Get("/v1/decisions", s.listDecisions)
		r.With(s.deps.Auth.Require("decisions:read")).Get("/v1/decisions/{id}", s.getDecision)
		r.With(s.deps.Auth.Require("decisions:read")).Get("/v1/metrics/rules", s.ruleMetrics)

		r.With(s.deps.Auth.Require("rules:write")).Post("/v1/rules/reload", s.reloadRules)
		r.With(s.deps.Auth.Require("rules:read")).Get("/v1/rules/active", s.activeRules)
		r.With(s.deps.Auth.Require("rules:read")).Get("/v1/rules/versions", s.ruleVersions)
		r.With(s.deps.Auth.Require("rules:write")).Put("/v1/rules/{id}", s.putRule)
		r.With(s.deps.Auth.Require("rules:write")).Post("/v1/rules/validate", s.validateRules)

		r.With(s.deps.Auth.Require("lists:read")).Get("/v1/lists", s.listLists)
		r.With(s.deps.Auth.Require("lists:write")).Post("/v1/lists", s.addList)
		r.With(s.deps.Auth.Require("lists:write")).Delete("/v1/lists/{id}", s.deleteList)
	})

	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		// Preflight reaches the catch-all because it names no concrete route.
		if r.Method == http.MethodOptions {
			s.cors(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})).ServeHTTP(w, r)
			return
		}
		writeProblem(w, r, http.StatusNotFound, "NOT_FOUND", "Not found", "no route for "+r.Method+" "+r.URL.Path)
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, r, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed",
			r.Method+" is not supported for this route")
	})
	return r
}

func echoRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if id := middleware.GetReqID(r.Context()); id != "" {
			w.Header().Set("X-Request-ID", id)
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.deps.Logger.Error("http panic", "panic", rec,
					"request_id", middleware.GetReqID(r.Context()), "stack", string(debug.Stack()))
				writeProblem(w, r, http.StatusInternalServerError, "INTERNAL", "Internal error",
					"the request could not be completed")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) cors(next http.Handler) http.Handler {
	origin := strings.TrimSpace(s.deps.CORSOrigin)
	if origin == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Origin"), origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Idempotency-Key, X-Fail-Policy, X-Request-ID")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Max-Age", "300")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusBody struct {
	Status string `json:"status"`
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusBody{Status: "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), ReadinessTimeout)
	defer cancel()
	state := "ok"
	code := http.StatusOK
	failed := ""
	if s.deps.Rules == nil || s.deps.Rules.Degraded() {
		state, code, failed = "degraded", http.StatusServiceUnavailable, "rules"
	}
	for _, c := range s.deps.Checks {
		if err := c.Pinger.Ping(ctx); err != nil {
			s.deps.Logger.Error("readiness check failed", "check", c.Name, "error", err)
			state, code, failed = "unavailable", http.StatusServiceUnavailable, c.Name
			break
		}
	}
	version := ""
	if s.deps.Rules != nil {
		version = s.deps.Rules.Source()
	}
	writeJSON(w, code, map[string]string{"status": state, "failed": failed, "rules": version, "version": s.deps.Version})
}

func (s *Server) version(w http.ResponseWriter, _ *http.Request) {
	ruleVersion := 0
	if s.deps.Rules != nil {
		ruleVersion = s.deps.Rules.Current().Version
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "decision-api", "version": s.deps.Version,
		"api": "v1", "rule_set_version": ruleVersion,
	})
}
