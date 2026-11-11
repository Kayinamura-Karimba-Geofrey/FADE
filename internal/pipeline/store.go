package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// PostgresStore implements decision history, list administration and rule
// metrics. It is never called on the response path except for duplicate event
// lookups, which happen after the feature store has already flagged a
// duplicate.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore wraps a pool.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// EnsureSchema applies the Phase 3 additive changes, so an existing Phase 0
// database keeps working without a migration run.
func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	const ddl = `
ALTER TABLE list_entries ADD COLUMN IF NOT EXISTS created_by TEXT;
ALTER TABLE list_entries ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS shadow_hits JSONB NOT NULL DEFAULT '[]'::jsonb;
CREATE INDEX IF NOT EXISTS decisions_account_decided_idx ON decisions (account_id, decided_at DESC);
CREATE INDEX IF NOT EXISTS decisions_decision_decided_idx ON decisions (decision, decided_at DESC);
CREATE INDEX IF NOT EXISTS decisions_event_idx ON decisions (event_id);`
	_, err := s.pool.Exec(ctx, ddl)
	return err
}

// WriteDecisions inserts one batch. Conflicts are ignored so spool replay is
// idempotent.
func (s *PostgresStore) WriteDecisions(ctx context.Context, batch []*StoredDecision) error {
	if len(batch) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	pgBatch := &pgx.Batch{}
	const q = `INSERT INTO decisions
	  (id, event_id, account_id, recipient_id, amount, txn_type, decision,
	   risk_score, rule_score, ml_score, ml_status, rule_set_version, model_version,
	   reasons, features, shadow_hits, latency_us, event_time, decided_at)
	  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
	  ON CONFLICT DO NOTHING`
	for _, rec := range batch {
		d := rec.Decision
		reasons, err := json.Marshal(d.Reasons)
		if err != nil {
			return err
		}
		feat, err := json.Marshal(d.Features)
		if err != nil {
			return err
		}
		shadow, err := json.Marshal(d.ShadowHits)
		if err != nil {
			return err
		}
		pgBatch.Queue(q,
			d.DecisionID, d.EventID, rec.AccountID, nullString(rec.RecipientID),
			rec.Amount, rec.TxnType, string(d.Decision), d.RiskScore, d.RuleScore,
			d.MLScore, d.MLStatus, d.RuleSetVersion, d.ModelVersion,
			reasons, feat, shadow, rec.LatencyUS, rec.EventTime, d.DecidedAt)
	}
	br := tx.SendBatch(ctx, pgBatch)
	for range batch {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return err
		}
	}
	if err := br.Close(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

const decisionColumns = `id, event_id, account_id, recipient_id, amount, txn_type,
 decision, risk_score, rule_score, ml_score, ml_status, rule_set_version, model_version,
 reasons, features, shadow_hits, latency_us, event_time, decided_at`

func scanDecision(row pgx.Row) (*StoredDecision, error) {
	var rec StoredDecision
	var d Decision
	var reasons, feats, shadow []byte
	var recipient, modelVersion *string
	var mlScore *int
	var decisionText string
	err := row.Scan(&d.DecisionID, &d.EventID, &rec.AccountID, &recipient, &rec.Amount,
		&rec.TxnType, &decisionText, &d.RiskScore, &d.RuleScore, &mlScore, &d.MLStatus,
		&d.RuleSetVersion, &modelVersion, &reasons, &feats, &shadow, &rec.LatencyUS, &rec.EventTime, &d.DecidedAt)
	if err != nil {
		return nil, err
	}
	d.Decision = rules.Decision(decisionText)
	d.MLScore = mlScore
	d.ModelVersion = modelVersion
	if recipient != nil {
		rec.RecipientID = *recipient
	}
	if err := json.Unmarshal(reasons, &d.Reasons); err != nil {
		return nil, err
	}
	if len(feats) > 0 && string(feats) != "null" {
		if err := json.Unmarshal(feats, &d.Features); err != nil {
			return nil, err
		}
	}
	if len(shadow) > 0 && string(shadow) != "null" {
		if err := json.Unmarshal(shadow, &d.ShadowHits); err != nil {
			return nil, err
		}
	}
	d.AccountID = rec.AccountID
	d.RecipientID = rec.RecipientID
	d.Amount = rec.Amount
	d.TxnType = rec.TxnType
	d.EventTime = rec.EventTime
	d.LatencyUS = rec.LatencyUS
	rec.Decision = &d
	return &rec, nil
}

// GetDecision returns one decision by id.
func (s *PostgresStore) GetDecision(ctx context.Context, id string) (*StoredDecision, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+decisionColumns+` FROM decisions WHERE id=$1 ORDER BY decided_at DESC LIMIT 1`, id)
	return scanDecision(row)
}

// DecisionByEventID returns the latest decision for an event.
func (s *PostgresStore) DecisionByEventID(ctx context.Context, eventID string) (*StoredDecision, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+decisionColumns+` FROM decisions WHERE event_id=$1 ORDER BY decided_at DESC LIMIT 1`, eventID)
	return scanDecision(row)
}

func encodeCursor(t time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano) + "|" + id))
}

func decodeCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	sep := -1
	for i := len(raw) - 1; i >= 0; i-- {
		if raw[i] == '|' {
			sep = i
			break
		}
	}
	if sep < 0 {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	t, err := time.Parse(time.RFC3339Nano, string(raw[:sep]))
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	return t, string(raw[sep+1:]), nil
}

// ListDecisions applies filters with keyset pagination on (decided_at,id).
func (s *PostgresStore) ListDecisions(ctx context.Context, f DecisionFilter) ([]*StoredDecision, string, error) {
	if f.Limit <= 0 || f.Limit > 200 {
		f.Limit = 50
	}
	args := []any{}
	where := "WHERE 1=1"
	add := func(clause string, v any) {
		args = append(args, v)
		where += fmt.Sprintf(" AND "+clause, len(args))
	}
	if f.AccountID != "" {
		add("account_id=$%d", f.AccountID)
	}
	if f.Decision != "" {
		add("decision=$%d", f.Decision)
	}
	if f.RuleID != "" {
		add("EXISTS (SELECT 1 FROM jsonb_array_elements(reasons) r WHERE r->>'id'=$%d)", f.RuleID)
	}
	if !f.From.IsZero() {
		add("decided_at >= $%d", f.From.UTC())
	}
	if !f.To.IsZero() {
		add("decided_at <= $%d", f.To.UTC())
	}
	if f.Cursor != "" {
		t, id, err := decodeCursor(f.Cursor)
		if err != nil {
			return nil, "", err
		}
		args = append(args, t, id)
		where += fmt.Sprintf(" AND (decided_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, f.Limit+1)
	q := `SELECT ` + decisionColumns + ` FROM decisions ` + where +
		fmt.Sprintf(" ORDER BY decided_at DESC, id DESC LIMIT $%d", len(args))
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out := make([]*StoredDecision, 0, f.Limit)
	for rows.Next() {
		rec, err := scanDecision(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > f.Limit {
		out = out[:f.Limit]
		last := out[len(out)-1]
		next = encodeCursor(last.Decision.DecidedAt, last.Decision.DecisionID)
	}
	return out, next, nil
}

// RuleMetrics aggregates rule fires and shadow hits over a window.
func (s *PostgresStore) RuleMetrics(ctx context.Context, window time.Duration) (*RuleMetricResult, error) {
	seconds := int64(window.Seconds())
	res := &RuleMetricResult{WindowSeconds: seconds, Rules: []RuleMetric{}}
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM decisions WHERE decided_at >= now() - make_interval(secs => $1)`,
		seconds).Scan(&res.Decisions); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r->>'id' AS rule_id, COALESCE(r->>'mode','enforce') AS mode, count(*)
		FROM decisions d, jsonb_array_elements(d.reasons) r
		WHERE d.decided_at >= now() - make_interval(secs => $1)
		  AND r->>'source' = 'rule'
		GROUP BY 1,2 ORDER BY 3 DESC`, seconds)
	if err != nil {
		return nil, err
	}
	index := map[string]int{}
	for rows.Next() {
		var m RuleMetric
		if err := rows.Scan(&m.RuleID, &m.Mode, &m.Fires); err != nil {
			rows.Close()
			return nil, err
		}
		index[m.RuleID] = len(res.Rules)
		res.Rules = append(res.Rules, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	shadow, err := s.pool.Query(ctx, `
		SELECT h, count(*) FROM decisions d, jsonb_array_elements_text(d.shadow_hits) h
		WHERE d.decided_at >= now() - make_interval(secs => $1)
		GROUP BY 1 ORDER BY 2 DESC`, seconds)
	if err != nil {
		return nil, err
	}
	defer shadow.Close()
	for shadow.Next() {
		var id string
		var n int64
		if err := shadow.Scan(&id, &n); err != nil {
			return nil, err
		}
		if i, ok := index[id]; ok {
			res.Rules[i].ShadowHits = n
		} else {
			res.Rules = append(res.Rules, RuleMetric{RuleID: id, Mode: "shadow", ShadowHits: n})
		}
	}
	if res.Decisions > 0 {
		for i := range res.Rules {
			res.Rules[i].Share = float64(res.Rules[i].Fires) / float64(res.Decisions)
		}
	}
	return res, shadow.Err()
}

// LoadLists implements ListSource.
func (s *PostgresStore) LoadLists(ctx context.Context) ([]ListEntry, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, list_type, subject, value,
		COALESCE(reason,''), COALESCE(created_by,''), created_at, expires_at
		FROM list_entries ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ListEntry
	for rows.Next() {
		var e ListEntry
		if err := rows.Scan(&e.ID, &e.ListType, &e.Subject, &e.Value, &e.Reason,
			&e.CreatedBy, &e.CreatedAt, &e.ExpiresAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// AddList implements ListSource.
func (s *PostgresStore) AddList(ctx context.Context, e ListEntry) (ListEntry, error) {
	err := s.pool.QueryRow(ctx, `INSERT INTO list_entries
		(list_type, subject, value, reason, created_by, expires_at)
		VALUES ($1,$2,$3,NULLIF($4,''),NULLIF($5,''),$6)
		ON CONFLICT (list_type, subject, value) DO UPDATE SET reason=EXCLUDED.reason,
		  created_by=EXCLUDED.created_by, expires_at=EXCLUDED.expires_at
		RETURNING id, created_at`,
		e.ListType, e.Subject, e.Value, e.Reason, e.CreatedBy, e.ExpiresAt).
		Scan(&e.ID, &e.CreatedAt)
	return e, err
}

// DeleteList implements ListSource.
func (s *PostgresStore) DeleteList(ctx context.Context, id int64) (bool, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM list_entries WHERE id=$1`, id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// ListEntries implements ListSource.
func (s *PostgresStore) ListEntries(ctx context.Context, listType, subject, value string, limit int) ([]ListEntry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT id, list_type, subject, value,
		COALESCE(reason,''), COALESCE(created_by,''), created_at, expires_at
		FROM list_entries
		WHERE ($1='' OR list_type=$1) AND ($2='' OR subject=$2) AND ($3='' OR value=$3)
		ORDER BY id LIMIT $4`, listType, subject, value, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ListEntry
	for rows.Next() {
		var e ListEntry
		if err := rows.Scan(&e.ID, &e.ListType, &e.Subject, &e.Value, &e.Reason,
			&e.CreatedBy, &e.CreatedAt, &e.ExpiresAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ErrNotFound is returned when a decision does not exist.
var ErrNotFound = errors.New("not found")
