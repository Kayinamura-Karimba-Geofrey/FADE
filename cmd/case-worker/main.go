// Command case-worker opens and triages review cases from the decision stream.
//
// It polls the decisions table rather than the txn.decisions topic because the
// topic carries the public decision contract, which deliberately omits the
// account and amount needed to build a case and enforce its SLA.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/cases"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/outbox"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/store"
)

// version is the build version; overridden at link time via -ldflags.
var version = "0.7.0"

type config struct {
	DSN        string
	Brokers    []string
	CaseConfig string
	LogLevel   string
	PollEvery  time.Duration
	SLAEvery   time.Duration
	Batch      int
	QAOverride *float64
	RelayEvery time.Duration
}

func main() {
	if err := run(); err != nil {
		slog.Error("case-worker failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := loadConfig()
	logger := obs.NewLogger(cfg.LogLevel)
	logger.Info("starting case-worker", "version", version, "poll", cfg.PollEvery,
		"sla_sweep", cfg.SLAEvery, "brokers", len(cfg.Brokers))

	root, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(root)
	defer cancel()

	pool, err := store.NewPostgresPool(ctx, cfg.DSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	caseCfg, err := cases.LoadConfig(cfg.CaseConfig)
	if err != nil {
		return err
	}
	if cfg.QAOverride != nil {
		caseCfg.QASampleRate = *cfg.QAOverride
	}
	if err := caseCfg.Validate(); err != nil {
		return err
	}
	casesStore := cases.NewStore(pool, caseCfg)

	if len(cfg.Brokers) > 0 {
		pub, err := outbox.NewKafkaPublisher(cfg.Brokers)
		if err != nil {
			logger.Warn("outbox relay disabled", "error", err)
		} else {
			defer pub.Close()
			relay := outbox.NewRelay(pool, pub, logger)
			go func() {
				if err := relay.Run(ctx, cfg.RelayEvery); err != nil {
					logger.Error("outbox relay stopped", "error", err)
				}
			}()
		}
	}

	worker := &triage{
		pool: pool, cases: casesStore, log: logger, batch: cfg.Batch,
		qaRate: caseCfg.QASampleRate,
	}
	if err := worker.run(ctx, cfg.PollEvery, cfg.SLAEvery); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	logger.Info("case-worker stopped", "version", version)
	return nil
}

func loadConfig() config {
	cfg := config{
		DSN:        env("DATABASE_URL", defaultDSN()),
		Brokers:    split(env("KAFKA_BROKERS", "")),
		CaseConfig: env("CASES_CONFIG", "config/cases.yaml"),
		LogLevel:   env("LOG_LEVEL", "info"),
		PollEvery:  envDuration("CASE_POLL_INTERVAL", 2*time.Second),
		SLAEvery:   envDuration("CASE_SLA_SWEEP_INTERVAL", 30*time.Second),
		Batch:      envInt("CASE_BATCH", 500),
		RelayEvery: envDuration("OUTBOX_RELAY_INTERVAL", time.Second),
	}
	if v := strings.TrimSpace(os.Getenv("QA_SAMPLE_RATE")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			cfg.QAOverride = &f
		}
	}
	return cfg
}

func defaultDSN() string {
	return "postgres://" + env("POSTGRES_USER", "fade") + ":" + env("POSTGRES_PASSWORD", "change-me") +
		"@" + env("POSTGRES_HOST", "postgres") + ":" + env("POSTGRES_PORT", "5432") +
		"/" + env("POSTGRES_DB", "fade") + "?sslmode=disable"
}

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key))); err == nil && v > 0 {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v, err := time.ParseDuration(strings.TrimSpace(os.Getenv(key))); err == nil && v > 0 {
		return v
	}
	return fallback
}

func split(raw string) []string {
	out := make([]string, 0, 4)
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// poolPinger adapts the pool to the readiness interface used by tests.
type poolPinger struct{ pool *pgxpool.Pool }

func (p poolPinger) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }
