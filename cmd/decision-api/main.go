// Command decision-api serves the FADE synchronous decision API.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/api"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/store"
)

const shutdownTimeout = 15 * time.Second

var version = "0.4.0"

func main() {
	if err := run(); err != nil {
		slog.Error("decision-api failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	logger := obs.NewLogger(cfg.LogLevel)
	logger.Info("starting decision-api", "version", version, "addr", cfg.HTTPAddr,
		"rules", cfg.RulesDir, "spool", cfg.SpoolDir)

	root, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(root)
	defer cancel()

	metrics := obs.NewMetrics("decision-api")
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB,
	})
	defer func() { _ = rdb.Close() }()

	// Postgres is optional: the API keeps deciding and spools persistence.
	var pgStore *pipeline.PostgresStore
	var decisionStore pipeline.Store
	var persist *rules.PostgresPersister
	// rulePersister stays a nil interface when Postgres is unavailable; a typed
	// nil pointer in a Persister interface is not nil and would panic on load.
	var rulePersister rules.Persister
	pool, err := store.NewPostgresPool(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Warn("postgres unavailable; history and rule persistence degraded", "error", err)
	} else {
		defer pool.Close()
		pgStore = pipeline.NewPostgresStore(pool)
		decisionStore = pgStore
		if err := pgStore.EnsureSchema(ctx); err != nil {
			logger.Warn("decision schema ensure failed", "error", err)
		}
		persist, err = rules.NewPostgresPersister(ctx, cfg.PostgresDSN, cfg.SpoolDir, logger,
			rules.PersistHooks{
				OnResult: func(result string) { metrics.PersistBatches.WithLabelValues(result).Inc() },
				OnSpoolBytes: func(n int64) {
					if n > 0 {
						metrics.SpoolBytes.Set(float64(n))
					}
				},
			})
		if err != nil {
			logger.Warn("rule persistence unavailable", "error", err)
			persist = nil
		} else {
			rulePersister = persist
		}
	}
	if persist != nil {
		defer persist.Close()
	}

	manager := rules.NewManager(cfg.RulesDir, logger, rulePersister, rules.Hooks{
		OnReload: func(result string, _ int) { metrics.RulesReload.WithLabelValues(result).Inc() },
		OnRollback: func(from, to int) {
			metrics.RulesRollback.Inc()
			logger.Error("rule set rolled back", "from", from, "to", to)
		},
		OnVersion: func(v int) { metrics.RuleSetVersion.Set(float64(v)) },
	})
	if err := manager.LoadStartup(ctx); err != nil {
		logger.Warn("starting with degraded rule state", "error", err)
	}
	metrics.RuleSetVersion.Set(float64(manager.Current().Version))
	if err := manager.Watch(ctx); err != nil {
		return err
	}
	manager.StartRollbackMonitor(ctx)

	featureStore := features.NewStore(rdb, cfg.Features, 60*time.Second, 24*time.Hour, features.Hooks{})
	var listSource pipeline.ListSource = pipeline.NewMemoryListSource()
	if pgStore != nil {
		listSource = pgStore
	}
	lists := pipeline.NewListCache(listSource, logger)
	lists.Start(ctx)

	var publisher pipeline.Publisher
	if len(cfg.KafkaBrokers) > 0 {
		if kp, err := pipeline.NewKafkaPublisher(cfg.KafkaBrokers, cfg.DecisionTopic); err == nil {
			publisher = kp
			defer kp.Close()
		} else {
			logger.Warn("kafka publisher disabled", "error", err)
		}
	}
	persister := pipeline.NewAsyncPersister(decisionStore, publisher, cfg.SpoolDir, logger, metrics)
	persister.Start(ctx)
	defer persister.Close()

	p := pipeline.New(pipeline.Deps{
		Rules: manager, Features: featureStore, Idem: pipeline.NewRedisIdempotency(rdb),
		Fallback: pipeline.NewMemoryIdempotency(), Lists: lists, Sink: persister,
		Store: decisionStore, Metrics: metrics, Logger: logger, Config: cfg.Pipeline,
	})

	auth := api.NewAuth(api.AuthConfig{Secret: cfg.JWTSecret, Issuer: cfg.JWTIssuer})
	if !auth.Enabled() {
		logger.Warn("JWT_SECRET is empty; authentication is DISABLED (development only)")
	}
	checks := []api.Check{{Name: "redis", Pinger: store.RedisPinger{Client: rdb}}}
	if pool != nil {
		checks = append(checks, api.Check{Name: "postgres", Pinger: pool})
	}
	router := api.NewRouter(api.Deps{
		Logger: logger, Metrics: metrics, Pipeline: p, Rules: manager,
		Auth: auth, Limiter: api.NewRateLimiter(cfg.RateLimits),
		Auditor: &api.PostgresAuditor{Pool: pool}, Checks: checks,
		Version: version, CORSOrigin: cfg.CORSOrigin,
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("decision-api listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case <-root.Done():
		logger.Info("shutdown requested")
	case err := <-serverErr:
		logger.Error("http server stopped", "error", err)
		cancel()
		return err
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown error", "error", err)
	}
	logger.Info("decision-api stopped", "version", version)
	return nil
}
