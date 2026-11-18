// Package outbox implements the transactional outbox for case and label
// events. Rows are written in the same transaction as the case change, then a
// relay publishes them at-least-once and marks them published. Consumers must
// be idempotent; every payload carries a stable key and the case version.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Publisher sends one record to a topic.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
}

// Relay drains the outbox table.
type Relay struct {
	pool      *pgxpool.Pool
	publisher Publisher
	log       *slog.Logger
	batch     int
	maxTries  int
}

// NewRelay builds a relay. batch defaults to 200 and maxTries to 20.
func NewRelay(pool *pgxpool.Pool, publisher Publisher, log *slog.Logger) *Relay {
	if log == nil {
		log = slog.Default()
	}
	return &Relay{pool: pool, publisher: publisher, log: log, batch: 200, maxTries: 20}
}

// Run drains the outbox until ctx is cancelled.
func (r *Relay) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := r.Drain(ctx); err != nil && ctx.Err() == nil {
			r.log.Error("outbox drain failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

type entry struct {
	ID       int64
	Topic    string
	Key      string
	Payload  []byte
	Attempts int
}

// Drain publishes one batch and marks the rows. A publish failure leaves the
// row unpublished and increments its attempt counter; rows that exhaust their
// attempts are logged and skipped so one poison record cannot block the queue.
func (r *Relay) Drain(ctx context.Context) error {
	rows, err := r.pool.Query(ctx, `
		SELECT id, topic, key, payload, attempts FROM outbox
		WHERE published_at IS NULL AND attempts < $1
		ORDER BY id LIMIT $2`, r.maxTries, r.batch)
	if err != nil {
		return err
	}
	var batch []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.ID, &e.Topic, &e.Key, &e.Payload, &e.Attempts); err != nil {
			rows.Close()
			return err
		}
		batch = append(batch, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, e := range batch {
		if err := r.publisher.Publish(ctx, e.Topic, e.Key, e.Payload); err != nil {
			r.log.Warn("outbox publish failed", "id", e.ID, "topic", e.Topic, "error", err)
			if _, uerr := r.pool.Exec(ctx,
				`UPDATE outbox SET attempts=attempts+1 WHERE id=$1`, e.ID); uerr != nil {
				return uerr
			}
			continue
		}
		if _, err := r.pool.Exec(ctx,
			`UPDATE outbox SET published_at=now(), attempts=attempts+1 WHERE id=$1`, e.ID); err != nil {
			return err
		}
	}
	return nil
}

// Unpublished counts rows still waiting, used by readiness and dashboards.
func (r *Relay) Unpublished(ctx context.Context) (int, error) {
	var n int
	err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

// Decode reads a payload into v; the relay keeps payloads opaque but tests and
// consumers share this helper.
func Decode(raw []byte, v any) error {
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("decode outbox payload: %w", err)
	}
	return nil
}
