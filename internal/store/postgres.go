// Package store provides state-store constructors (Postgres, Redis) used by
// FADE services. All constructors enforce a context timeout so a dead backend
// fails fast instead of hanging startup.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ConnectTimeout bounds pool creation and the initial ping.
const ConnectTimeout = 5 * time.Second

// NewPostgresPool parses the DSN, opens a pgx pool and verifies connectivity
// within ConnectTimeout. On ping failure the pool is closed before returning.
func NewPostgresPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(ctx, ConnectTimeout)
	defer cancel()

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}
