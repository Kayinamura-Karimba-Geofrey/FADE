package obs

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics bundles the Prometheus registry and the collectors shared by FADE
// services. Every service gets its own registry so /metrics stays isolated.
type Metrics struct {
	Registry     *prometheus.Registry
	HTTPRequests *prometheus.CounterVec
	HTTPDuration *prometheus.HistogramVec
	Up           *prometheus.GaugeVec
}

// NewMetrics creates a fresh registry pre-populated with the FADE collectors.
func NewMetrics(service string) *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		Registry: reg,
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fade_http_requests_total",
			Help: "Total number of HTTP requests handled, by method, path and status.",
		}, []string{"method", "path", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fade_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds, by method and path.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "path"}),
		Up: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "fade_service_up",
			Help: "Whether the service is up (1) or down (0).",
		}, []string{"service"}),
	}

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.HTTPRequests,
		m.HTTPDuration,
		m.Up,
	)
	m.Up.WithLabelValues(service).Set(1)
	return m
}

// Handler serves the registry over HTTP.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
