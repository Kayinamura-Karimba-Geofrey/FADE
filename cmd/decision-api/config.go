package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/api"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
)

// config is the decision-api process configuration.
type appConfig struct {
	HTTPAddr      string
	LogLevel      string
	PostgresDSN   string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RulesDir      string
	SpoolDir      string
	JWTSecret     string
	JWTIssuer     string
	CORSOrigin    string
	KafkaBrokers  []string
	DecisionTopic string
	FeaturePath   string
	MLScorerURL   string
	OTLPEndpoint  string
	Version       string
	Features      config.Features
	Pipeline      pipeline.Config
	RateLimits    map[string]api.RateLimit
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v, err := strconv.Atoi(env(key, ""))
	if err != nil {
		return fallback
	}
	return v
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v, err := time.ParseDuration(env(key, ""))
	if err != nil {
		return fallback
	}
	return v
}

func envFloat(key string, fallback float64) float64 {
	v, err := strconv.ParseFloat(env(key, ""), 64)
	if err != nil {
		return fallback
	}
	return v
}

func loadConfig() (appConfig, error) {
	pgHost := env("POSTGRES_HOST", "postgres")
	pgPort := env("POSTGRES_PORT", "5432")
	pgUser := env("POSTGRES_USER", "fade")
	pgPass := env("POSTGRES_PASSWORD", "change-me")
	pgDB := env("POSTGRES_DB", "fade")
	dsn := env("DATABASE_URL",
		fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", pgUser, pgPass, pgHost, pgPort, pgDB))

	featurePath := env("FEATURE_CONFIG", config.DefaultFeaturePath)
	featuresCfg, err := config.LoadFeatures(featurePath)
	if err != nil {
		return appConfig{}, err
	}

	cfg := appConfig{
		HTTPAddr:      env("HTTP_ADDR", ":8080"),
		LogLevel:      env("LOG_LEVEL", "info"),
		PostgresDSN:   dsn,
		RedisAddr:     env("REDIS_ADDR", "redis:6379"),
		RedisPassword: env("REDIS_PASSWORD", ""),
		RedisDB:       envInt("REDIS_DB", 0),
		RulesDir:      env("RULES_DIR", "rules"),
		SpoolDir:      env("SPOOL_DIR", "./spool"),
		JWTSecret:     env("JWT_SECRET", ""),
		JWTIssuer:     env("JWT_ISSUER", ""),
		CORSOrigin:    env("CORS_ORIGIN", "http://localhost:3000"),
		KafkaBrokers:  split(env("KAFKA_BROKERS", "")),
		DecisionTopic: env("DECISION_TOPIC", "txn.decisions"),
		FeaturePath:   featurePath,
		MLScorerURL:   env("ML_SCORER_URL", ""),
		OTLPEndpoint:  env("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		Version:       env("FADE_VERSION", "0.4.0"),
		Features:      featuresCfg,
	}
	pcfg := pipeline.DefaultConfig()
	pcfg.RequestBudget = envDuration("REQUEST_BUDGET", pcfg.RequestBudget)
	pcfg.FeatureTimeout = envDuration("FEATURE_TIMEOUT", pcfg.FeatureTimeout)
	pcfg.MaxConcurrency = envInt("MAX_CONCURRENCY", pcfg.MaxConcurrency)
	pcfg.IdempotencyTTL = envDuration("IDEMPOTENCY_TTL", pcfg.IdempotencyTTL)
	pcfg.Version = cfg.Version
	if len(featuresCfg.KYCLimits) > 0 {
		pcfg.TierLimits = featuresCfg.KYCLimits
	}
	cfg.Pipeline = pcfg
	cfg.RateLimits = map[string]api.RateLimit{
		"service": {PerSecond: envFloat("SERVICE_RPS", 5000), Burst: envFloat("SERVICE_BURST", 10000)},
		"analyst": {PerSecond: envFloat("ANALYST_RPS", 50), Burst: envFloat("ANALYST_BURST", 100)},
		"admin":   {PerSecond: envFloat("ADMIN_RPS", 20), Burst: envFloat("ADMIN_BURST", 40)},
	}
	return cfg, nil
}

func split(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
