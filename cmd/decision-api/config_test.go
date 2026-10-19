package main

import (
	"strings"
	"testing"
)

// configEnvKeys lists every variable loadConfig reads, so tests can clear
// inherited values deterministically.
var configEnvKeys = []string{
	"HTTP_ADDR", "LOG_LEVEL",
	"POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD",
	"REDIS_ADDR", "KAFKA_BROKERS", "ML_SCORER_URL", "JWT_SECRET",
}

// clearEnv blanks every config key for the duration of the test.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range configEnvKeys {
		t.Setenv(k, "")
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	clearEnv(t)

	cfg := loadConfig()

	want := map[string]string{
		"HTTP_ADDR":  cfg.HTTPAddr,
		"LOG_LEVEL":  cfg.LogLevel,
		"PG_HOST":    cfg.PostgresHost,
		"PG_PORT":    cfg.PostgresPort,
		"PG_DB":      cfg.PostgresDB,
		"PG_USER":    cfg.PostgresUser,
		"PG_PASS":    cfg.PostgresPassword,
		"REDIS":      cfg.RedisAddr,
		"KAFKA":      cfg.KafkaBrokers,
		"ML_SCORER":  cfg.MLScorerURL,
		"JWT_SECRET": cfg.JWTSecret,
	}
	expect := map[string]string{
		"HTTP_ADDR":  ":8080",
		"LOG_LEVEL":  "info",
		"PG_HOST":    "postgres",
		"PG_PORT":    "5432",
		"PG_DB":      "fade",
		"PG_USER":    "fade",
		"PG_PASS":    "change-me",
		"REDIS":      "redis:6379",
		"KAFKA":      "redpanda:9092",
		"ML_SCORER":  "http://ml-scorer:8000",
		"JWT_SECRET": "change-me",
	}
	for k, v := range want {
		if v != expect[k] {
			t.Errorf("%s = %q, want %q", k, v, expect[k])
		}
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("POSTGRES_HOST", "db.internal")
	t.Setenv("POSTGRES_PASSWORD", "s3cret")

	cfg := loadConfig()

	if cfg.HTTPAddr != ":9090" {
		t.Errorf("HTTPAddr = %q, want :9090", cfg.HTTPAddr)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want debug", cfg.LogLevel)
	}
	if cfg.PostgresHost != "db.internal" {
		t.Errorf("PostgresHost = %q, want db.internal", cfg.PostgresHost)
	}
	if cfg.PostgresPassword != "s3cret" {
		t.Errorf("PostgresPassword = %q, want s3cret", cfg.PostgresPassword)
	}
	// Untouched keys keep defaults.
	if cfg.RedisAddr != "redis:6379" {
		t.Errorf("RedisAddr = %q, want redis:6379", cfg.RedisAddr)
	}
}

func TestPostgresDSN(t *testing.T) {
	clearEnv(t)
	t.Setenv("POSTGRES_USER", "fade")
	t.Setenv("POSTGRES_PASSWORD", "pw")
	t.Setenv("POSTGRES_HOST", "postgres")
	t.Setenv("POSTGRES_PORT", "5432")
	t.Setenv("POSTGRES_DB", "fade")

	dsn := loadConfig().PostgresDSN()
	want := "postgres://fade:pw@postgres:5432/fade?sslmode=disable"
	if dsn != want {
		t.Errorf("PostgresDSN = %q, want %q", dsn, want)
	}
	if !strings.HasPrefix(dsn, "postgres://") {
		t.Errorf("DSN must use postgres:// scheme, got %q", dsn)
	}
}
