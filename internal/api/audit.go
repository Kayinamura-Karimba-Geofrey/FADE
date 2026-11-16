package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/audit"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/redaction"
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

// PostgresAuditor appends to the hash-chained audit_log table through the shared
// audit store, so rule activation and administrative actions share one chain.
type PostgresAuditor struct{ Pool *pgxpool.Pool }

// Store returns the audit store, or nil without a pool.
func (a *PostgresAuditor) Store() audit.Store {
	if a == nil || a.Pool == nil {
		return nil
	}
	return audit.NewPostgresStore(a.Pool)
}

// Record implements Auditor.
func (a *PostgresAuditor) Record(ctx context.Context, actor, action, objectType, objectID string, before, after any) error {
	store := a.Store()
	if store == nil {
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
	row := audit.NewRow(actor, action, objectType, objectID, beforeRaw, afterRaw, time.Now())
	_, err = store.Append(ctx, row)
	return err
}

func marshalOrNil(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	// An audit payload must never carry a raw personal identifier.
	if err := redaction.CheckJSON(json.RawMessage(raw)); err != nil {
		return nil, err
	}
	return raw, nil
}
