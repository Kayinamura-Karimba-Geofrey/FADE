package obs

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// wrapResponseWriter records the status code written by the handler.
type wrapResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *wrapResponseWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *wrapResponseWriter) Status() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

// HTTPMiddleware records fade_http_requests_total{method,path,status} and
// fade_http_request_duration_seconds{method,path}. The path label uses the
// matched chi route pattern to keep cardinality bounded.
func HTTPMiddleware(m *Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			m.HTTPInflight.Inc()
			ww := &wrapResponseWriter{ResponseWriter: w}
			defer func() {
				m.HTTPInflight.Dec()
				route := chi.RouteContext(r.Context()).RoutePattern()
				if route == "" {
					route = "unmatched"
				}
				m.HTTPRequests.WithLabelValues(route, strconv.Itoa(ww.Status())).Inc()
				m.HTTPDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
			}()
			next.ServeHTTP(ww, r)
		})
	}
}

// RequestLogger logs one structured line per request.
func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := &wrapResponseWriter{ResponseWriter: w}
			next.ServeHTTP(ww, r)

			route := chi.RouteContext(r.Context()).RoutePattern()
			logger.LogAttrs(r.Context(), slog.LevelInfo, "http_request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.String("route", route),
				slog.Int("status", ww.Status()),
				slog.Duration("duration", time.Since(start)),
				slog.String("request_id", middleware.GetReqID(r.Context())),
			)
		})
	}
}
