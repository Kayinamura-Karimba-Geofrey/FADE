// Command feature-worker consumes raw transactions from Redpanda, maintains
// the Redis feature store, and emits optional feature vectors.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/redis/go-redis/v9"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/ingest"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
)

// version is overridden at link time.
var version = "0.3.0"

const shutdownTimeout = 15 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "feature-worker:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Getenv("FEATURE_CONFIG"), os.LookupEnv)
	if err != nil {
		return err
	}
	logger := obs.NewLogger(cfg.LogLevel)
	logger.Info("starting feature-worker", "version", version,
		"topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID, "redis", cfg.RedisAddr)

	root, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(root)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer func() { _ = rdb.Close() }()
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		pingCancel()
		return fmt.Errorf("redis %s: %w", cfg.RedisAddr, err)
	}
	pingCancel()

	obsMetrics := obs.NewMetrics("feature-worker")
	metrics := ingest.NewMetrics(obsMetrics.Registry)

	client, err := ingest.NewClient(cfg)
	if err != nil {
		return err
	}
	hooks := features.Hooks{
		Capped: func(n int) { metrics.WindowCapped.Add(float64(n)) },
		Redis: func(d time.Duration) {
			metrics.RedisDuration.WithLabelValues("script").Observe(d.Seconds())
			metrics.RedisScripts.WithLabelValues("ok").Inc()
		},
		Cycle: func(d time.Duration) { metrics.CycleDuration.Observe(d.Seconds()) },
	}
	store := features.NewStore(rdb, cfg.Features, cfg.LateTolerance, cfg.DedupeTTL, hooks)
	processor := ingest.NewProcessor(store, ingest.NewKgoPublisher(client), metrics, logger,
		cfg.KafkaDLQTopic, cfg.FeatureOutputTopic, version)
	consumer := ingest.NewConsumer(client, processor, metrics, logger,
		cfg.WorkersPerPartition, cfg.MaxInflight)
	defer consumer.Close()

	router := newRouter(obsMetrics, logger, rdb, consumer)
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("feature-worker http listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(ctx) }()

	select {
	case <-root.Done():
		logger.Info("shutdown requested")
	case err := <-runErr:
		if err != nil {
			logger.Error("consumer stopped", "error", err)
			cancel()
		}
	case err := <-serverErr:
		logger.Error("http server stopped", "error", err)
		cancel()
		err = fmt.Errorf("http server: %w", err)
		shutdown(srv, logger)
		return err
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown error", "error", err)
	}
	logger.Info("feature-worker stopped", "version", version)
	return nil
}

func shutdown(srv *http.Server, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("http shutdown error", "error", err)
	}
}

func newRouter(metrics *obs.Metrics, logger *slog.Logger, rdb *redis.Client, consumer *ingest.Consumer) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Recoverer)
	r.Use(obs.HTTPMiddleware(metrics))

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	r.Get("/readyz", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()
		checks := map[string]string{}
		status := http.StatusOK
		if err := rdb.Ping(ctx).Err(); err != nil {
			checks["redis"] = err.Error()
			status = http.StatusServiceUnavailable
		}
		if err := consumer.Ping(ctx); err != nil {
			checks["kafka"] = err.Error()
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status == http.StatusOK {
			_, _ = w.Write([]byte(`{"status":"ready"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "not_ready", "checks": checks})
	})
	r.Handle("/metrics", metrics.Handler())
	return r
}
