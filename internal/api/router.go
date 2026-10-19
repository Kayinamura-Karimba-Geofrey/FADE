// Package api wires the decision-api HTTP surface: health, readiness and
// Prometheus metrics endpoints on a chi router.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
)

// ReadinessTimeout bounds the aggregate readiness probe.
const ReadinessTimeout = 2 * time.Second

// Pinger is a single readiness dependency (Postgres, Redis, ...).
type Pinger interface {
	Ping(ctx context.Context) error
}

// Check names a readiness dependency for structured logs and error bodies.
type Check struct {
	Name   string
	Pinger Pinger
}

// Server holds the handler dependencies.
type Server struct {
	logger *slog.Logger
	checks []Check
}

// NewServer builds a Server with the given readiness checks (may be empty).
func NewServer(logger *slog.Logger, checks ...Check) *Server {
	return &Server{logger: logger, checks: checks}
}

type statusResponse struct {
	Status string `json:"status"`
}

type failureResponse struct {
	Status string `json:"status"`
	Failed string `json:"failed"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// healthz always answers 200 while the process is alive.
func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

// readyz answers 200 only when every readiness check succeeds within
// ReadinessTimeout, otherwise 503 with the name of the failing check.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), ReadinessTimeout)
	defer cancel()

	for _, c := range s.checks {
		if err := c.Pinger.Ping(ctx); err != nil {
			s.logger.Error("readiness check failed", "check", c.Name, "err", err)
			writeJSON(w, http.StatusServiceUnavailable, failureResponse{
				Status: "unavailable",
				Failed: c.Name,
			})
			return
		}
	}
	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

// NewRouter assembles the chi router: request ID, recovery, structured
// request logging, FADE metrics and the health/readiness/metrics routes.
func NewRouter(logger *slog.Logger, metrics *obs.Metrics, checks ...Check) http.Handler {
	s := NewServer(logger, checks...)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(obs.RequestLogger(logger))
	r.Use(obs.HTTPMiddleware(metrics))

	r.Get("/healthz", s.healthz)
	r.Get("/readyz", s.readyz)
	r.Get("/metrics", metrics.Handler().ServeHTTP)

	return r
}
