// Package obs provides observability primitives: structured logging,
// Prometheus metrics and HTTP middleware shared by all FADE services.
package obs

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger builds a JSON slog logger writing to stdout at the given level.
// Unknown levels fall back to info.
func NewLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}
