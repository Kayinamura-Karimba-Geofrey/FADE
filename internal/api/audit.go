package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// Auditor records administrative actions.
type Auditor interface {
	Record(ctx context.Context, actor, action, objectType, objectID string, before, after any) error
}

// NoopAuditor logs instead of writing (development and tests).
type NoopAuditor struct{ Logger *slog.Logger }

// Record implements Auditor.
func (n NoopAuditor) Record(_ context.Context, actor, action, objectType, objectID string, before, after any) error {
	if n.Logger != nil {
		n.Logger.Info("audit", "actor", actor, "action", action,
			"object_type", objectType, "object_id", objectID, "before", before, "after", after)
	}
	return nil
}

// PostgresAuditor appends to the hash-chained audit_log table in one
// transaction.
type PostgresAuditor struct{ Pool *pgxpool.Pool }

// Record implements Auditor.
func (a *PostgresAuditor) Record(ctx context.Context, actor, action, objectType, objectID string, before, after any) error {
	if a == nil || a.Pool == nil {
		return nil
	}
	beforeRaw, err := marshalOrNil(before)
	if err != nil {
		return err
	}
	afterRaw, err := marshalOrNil(after)
	if err != nil {
		return err
	}
	tx, err := a.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var prev string
	err = tx.QueryRow(ctx, `SELECT COALESCE(hash,'') FROM audit_log ORDER BY id DESC LIMIT 1`).Scan(&prev)
	if err != nil && err != pgx.ErrNoRows {
		return err
	}
	row := rules.NewAuditRow(actor, action, objectType, objectID, beforeRaw, afterRaw, time.Now())
	hash, err := rules.ChainHash(prev, row)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_log
		(actor, action, object_type, object_id, before, after, prev_hash, hash, at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		row.Actor, row.Action, row.ObjectType, row.ObjectID, row.Before, row.After,
		prev, hash, row.At); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func marshalOrNil(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}
