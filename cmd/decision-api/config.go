package main

import (
	"fmt"
	"os"
	"strings"
)

// config holds everything decision-api reads from the environment.
// See .env.example for the canonical list of variables.
type config struct {
	HTTPAddr         string
	LogLevel         string
	PostgresHost     string
	PostgresPort     string
	PostgresDB       string
	PostgresUser     string
	PostgresPassword string
	RedisAddr        string
	KafkaBrokers     string
	MLScorerURL      string
	JWTSecret        string
}

// env returns the trimmed value of key or fallback when unset or blank.
func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return fallback
}

// loadConfig reads configuration from the process environment, applying the
// defaults documented in .env.example.
func loadConfig() config {
	return config{
		HTTPAddr:         env("HTTP_ADDR", ":8080"),
		LogLevel:         env("LOG_LEVEL", "info"),
		PostgresHost:     env("POSTGRES_HOST", "postgres"),
		PostgresPort:     env("POSTGRES_PORT", "5432"),
		PostgresDB:       env("POSTGRES_DB", "fade"),
		PostgresUser:     env("POSTGRES_USER", "fade"),
		PostgresPassword: env("POSTGRES_PASSWORD", "change-me"),
		RedisAddr:        env("REDIS_ADDR", "redis:6379"),
		KafkaBrokers:     env("KAFKA_BROKERS", "redpanda:9092"),
		MLScorerURL:      env("ML_SCORER_URL", "http://ml-scorer:8000"),
		JWTSecret:        env("JWT_SECRET", "change-me"),
	}
}

// PostgresDSN builds the pgx connection string.
func (c config) PostgresDSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.PostgresUser, c.PostgresPassword, c.PostgresHost, c.PostgresPort, c.PostgresDB,
	)
}
