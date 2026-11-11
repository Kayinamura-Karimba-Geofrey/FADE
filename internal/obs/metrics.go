package obs

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StageBuckets are the Phase 3 decision-stage latency buckets (200 µs - 200 ms).
var StageBuckets = []float64{0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2}

// Metrics bundles the Prometheus registry and the collectors shared by FADE
// services. Each service gets its own registry.
type Metrics struct {
	Registry *prometheus.Registry

	HTTPRequests *prometheus.CounterVec
	HTTPDuration *prometheus.HistogramVec
	HTTPInflight prometheus.Gauge
	Up           *prometheus.GaugeVec

	Decisions         *prometheus.CounterVec
	RuleHits          *prometheus.CounterVec
	DecisionLatency   *prometheus.HistogramVec
	Shed              prometheus.Counter
	RulesReload       *prometheus.CounterVec
	RulesRollback     prometheus.Counter
	RuleSetVersion    prometheus.Gauge
	PersistQueueDepth prometheus.Gauge
	SpoolBytes        prometheus.Gauge
	PersistBatches    *prometheus.CounterVec
	IdempotentReplays prometheus.Counter
	FailPolicy        *prometheus.CounterVec
}

// NewMetrics creates a fresh registry pre-populated with the FADE collectors.
func NewMetrics(service string) *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fade_http_requests_total",
			Help: "HTTP requests by matched route and status.",
		}, []string{"route", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fade_http_request_duration_seconds",
			Help:    "HTTP request duration by matched route.",
			Buckets: StageBuckets,
		}, []string{"route"}),
		HTTPInflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fade_http_inflight",
			Help: "HTTP requests currently being served.",
		}),
		Up: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "fade_service_up",
			Help: "Whether the service is up (1) or down (0).",
		}, []string{"service"}),
		Decisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fade_decisions_total",
			Help: "Decisions produced by decision class and degradation flag.",
		}, []string{"decision", "degraded"}),
		RuleHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fade_rule_hits_total",
			Help: "Per-rule hits by rule id and mode.",
		}, []string{"rule_id", "mode"}),
		DecisionLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fade_decision_latency_seconds",
			Help:    "Decision pipeline stage latency.",
			Buckets: StageBuckets,
		}, []string{"stage"}),
		Shed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fade_shed_total",
			Help: "Requests rejected by the concurrency semaphore.",
		}),
		RulesReload: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fade_rules_reload_total",
			Help: "Rule reload attempts by result.",
		}, []string{"result"}),
		RulesRollback: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fade_rules_rollback_total",
			Help: "Automatic rule-set rollbacks.",
		}),
		RuleSetVersion: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fade_rule_set_version",
			Help: "Active rule-set version.",
		}),
		PersistQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fade_persist_queue_depth",
			Help: "Decisions waiting in the async persistence queue.",
		}),
		SpoolBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fade_spool_bytes",
			Help: "Bytes in the decision persistence spool.",
		}),
		PersistBatches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fade_persist_batches_total",
			Help: "Persistence batches by result.",
		}, []string{"result"}),
		IdempotentReplays: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fade_idempotent_replays_total",
			Help: "Responses served from the idempotency cache.",
		}),
		FailPolicy: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fade_fail_policy_total",
			Help: "Degraded decisions by cause and fail mode.",
		}, []string{"cause", "mode"}),
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.HTTPRequests, m.HTTPDuration, m.HTTPInflight, m.Up,
		m.Decisions, m.RuleHits, m.DecisionLatency, m.Shed,
		m.RulesReload, m.RulesRollback, m.RuleSetVersion,
		m.PersistQueueDepth, m.SpoolBytes, m.PersistBatches,
		m.IdempotentReplays, m.FailPolicy,
	)
	m.Up.WithLabelValues(service).Set(1)
	return m
}

// Handler serves the registry over HTTP.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

// ObserveStage records one pipeline stage. Nil-safe for tests.
func (m *Metrics) ObserveStage(stage string, seconds float64) {
	if m != nil {
		m.DecisionLatency.WithLabelValues(stage).Observe(seconds)
	}
}
