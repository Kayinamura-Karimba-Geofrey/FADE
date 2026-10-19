// Command decision-api serves the FADE decision HTTP API: liveness,
// readiness and Prometheus metrics, with structured JSON logging and
// graceful shutdown on SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/api"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/store"
)

// version is the build version; overridden at link time via -ldflags.
var version = "0.1.0"

// shutdownTimeout is the graceful-shutdown budget on SIGINT/SIGTERM.
const shutdownTimeout = 10 * time.Second

func main() {
	cfg := loadConfig()
	logger := obs.NewLogger(cfg.LogLevel)
	logger.Info("starting decision-api", "version", version, "addr", cfg.HTTPAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := store.NewPostgresPool(ctx, cfg.PostgresDSN())
	if err != nil {
		logger.Error("postgres unavailable", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	rdb, err := store.NewRedisClient(ctx, cfg.RedisAddr)
	if err != nil {
		logger.Error("redis unavailable", "err", err)
		os.Exit(1)
	}
	defer func() { _ = rdb.Close() }()

	metrics := obs.NewMetrics("decision-api")
	router := api.NewRouter(logger, metrics,
		api.Check{Name: "postgres", Pinger: pool},
		api.Check{Name: "redis", Pinger: store.RedisPinger{Client: rdb}},
	)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		logger.Info("shutting down", "timeout", shutdownTimeout.String())
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown error", "err", err)
		}
	}()

	logger.Info("decision-api listening", "version", version)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server error", "err", err)
		os.Exit(1)
	}
	logger.Info("decision-api stopped", "version", version)
}
