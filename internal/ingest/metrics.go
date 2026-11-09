package ingest

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds ingestion collectors. They are registered into the worker's
// private registry by NewMetrics.
type Metrics struct {
	Events        *prometheus.CounterVec
	Processing    prometheus.Histogram
	WindowCapped  prometheus.Counter
	ConsumerLag   prometheus.Gauge
	DeadLettered  *prometheus.CounterVec
	Commits       *prometheus.CounterVec
	FeatureOutput prometheus.Counter
	RedisScripts  *prometheus.CounterVec
	RedisDuration *prometheus.HistogramVec
	CycleDuration prometheus.Histogram
	DLQDuration   prometheus.Histogram
	Retries       prometheus.Counter
	ParseFailures prometheus.Counter
}

// NewMetrics registers the Phase 2 collectors.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fade_feature_events_total",
			Help: "Events handled by the feature worker, by status.",
		}, []string{"status"}),
		Processing: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fade_event_processing_seconds",
			Help:    "End-to-end event processing time, including Redis, cycle detection and feature output.",
			Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.008, 0.01, 0.015, 0.025, 0.05, 0.1, 0.25},
		}),
		WindowCapped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fade_window_capped_total",
			Help: "Window ZSET entries dropped because a cap was reached.",
		}),
		ConsumerLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fade_consumer_lag",
			Help: "Sum of partition high-watermark minus last fetched offset.",
		}),
		DeadLettered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fade_events_dead_lettered_total",
			Help: "Events published to the dead-letter topic, by reason.",
		}, []string{"reason"}),
		Commits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fade_kafka_commits_total",
			Help: "Kafka offset commit attempts, by result.",
		}, []string{"result"}),
		FeatureOutput: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fade_feature_output_total",
			Help: "Feature vectors published to the feature output topic.",
		}),
		RedisScripts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fade_redis_scripts_total",
			Help: "Redis feature script executions, by result.",
		}, []string{"result"}),
		RedisDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fade_redis_operation_seconds",
			Help:    "Redis script latency.",
			Buckets: []float64{0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1},
		}, []string{"operation"}),
		CycleDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fade_cycle_detection_seconds",
			Help:    "Cycle detection latency.",
			Buckets: []float64{0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.003, 0.005, 0.01},
		}),
		DLQDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fade_dlq_publish_seconds",
			Help:    "Dead-letter publish latency.",
			Buckets: prometheus.DefBuckets,
		}),
		Retries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fade_event_retries_total",
			Help: "Transient processing failures retried by the worker.",
		}),
		ParseFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fade_event_parse_failures_total",
			Help: "Events rejected by strict contract validation.",
		}),
	}
	reg.MustRegister(m.Events, m.Processing, m.WindowCapped, m.ConsumerLag,
		m.DeadLettered, m.Commits, m.FeatureOutput, m.RedisScripts, m.RedisDuration,
		m.CycleDuration, m.DLQDuration, m.Retries, m.ParseFailures)
	return m
}
