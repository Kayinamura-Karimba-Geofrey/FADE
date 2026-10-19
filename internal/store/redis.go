package store

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// NewRedisClient creates a go-redis client for addr and verifies
// connectivity within ConnectTimeout.
func NewRedisClient(ctx context.Context, addr string) (*redis.Client, error) {
	ctx, cancel := context.WithTimeout(ctx, ConnectTimeout)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis at %s: %w", addr, err)
	}
	return client, nil
}

// RedisPinger adapts *redis.Client to the readiness-check interface used by
// the decision-api without leaking Redis types into the API package.
type RedisPinger struct {
	Client *redis.Client
}

// Ping checks Redis connectivity with the caller's context deadline.
func (p RedisPinger) Ping(ctx context.Context) error {
	return p.Client.Ping(ctx).Err()
}
