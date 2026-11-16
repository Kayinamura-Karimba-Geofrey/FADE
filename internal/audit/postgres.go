package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ChainLockKey serialises appends. Every writer takes this transaction-scoped
// advisory lock, reads the head and inserts the next link, so concurrent
// writers cannot fork the chain.
const ChainLockKey int64 = 0x4641444500000001 // "FADE" + 1

// PostgresStore implements Store over the append-only audit_log table.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore wraps a pool.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore { return &PostgresStore{pool: pool} }

// Pool exposes the underlying pool.
func (s *PostgresStore) Pool() *pgxpool.Pool { return s.pool }

// EnsureSchema creates the Phase 5 audit objects defensively. The numbered
// migration is authoritative; this keeps an un-migrated development database
// usable and is idempotent.
func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS audit_checkpoints (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    schema_version INT NOT NULL,
    chain TEXT NOT NULL,
    seq BIGINT NOT NULL,
    hash TEXT NOT NULL,
    row_count BIGINT NOT NULL,
    digest TEXT NOT NULL,
    path TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS audit_checkpoints_seq_idx ON audit_checkpoints (seq DESC);
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS request_id TEXT;
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS source_ip_hash TEXT;
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS seq BIGINT;
CREATE UNIQUE INDEX IF NOT EXISTS audit_log_seq_uidx ON audit_log (seq);
CREATE UNIQUE INDEX IF NOT EXISTS audit_log_hash_uidx ON audit_log (hash);
`
	if _, err := s.pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("audit schema: %w", err)
	}
	return nil
}

// Append implements Store: one transaction, one advisory lock, one link.
func (s *PostgresStore) Append(ctx context.Context, row Row) (Entry, error) {
	if row.At == "" {
		row.At = NormalizeAt(time.Now())
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Entry{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ChainLockKey); err != nil {
		return Entry{}, fmt.Errorf("audit: acquire chain lock: %w", err)
	}
	prev, seq := GenesisHash, int64(1)
	var headSeq int64
	var headHash string
	err = tx.QueryRow(ctx, `SELECT seq, hash FROM audit_log WHERE seq IS NOT NULL ORDER BY seq DESC LIMIT 1`).Scan(&headSeq, &headHash)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return Entry{}, fmt.Errorf("audit: read head: %w", err)
	default:
		prev, seq = headHash, headSeq+1
	}
	hash, err := ChainHash(prev, row)
	if err != nil {
		return Entry{}, err
	}
	var at time.Time
	if err := tx.QueryRow(ctx, `
		INSERT INTO audit_log
			(seq, actor, action, object_type, object_id, before, after, prev_hash, hash, at, request_id, source_ip_hash)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		RETURNING at`,
		seq, row.Actor, row.Action, row.ObjectType, row.ObjectID,
		rawArg(row.Before), rawArg(row.After), prev, hash, row.At,
		row.RequestID, row.SourceIPHash).Scan(&at); err != nil {
		return Entry{}, fmt.Errorf("audit: insert row: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Entry{}, err
	}
	row.At = NormalizeAt(at)
	return Entry{Seq: seq, PrevHash: prev, Hash: hash, Row: row}, nil
}

func rawArg(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return string(raw)
}

const entryColumns = `seq, actor, action, object_type, object_id,
 COALESCE(before::text,''), COALESCE(after::text,''), COALESCE(prev_hash,''),
 hash, at, COALESCE(request_id,''), COALESCE(source_ip_hash,'')`

func scanEntry(row pgx.Row) (Entry, error) {
	var e Entry
	var before, after string
	var at time.Time
	if err := row.Scan(&e.Seq, &e.Row.Actor, &e.Row.Action, &e.Row.ObjectType,
		&e.Row.ObjectID, &before, &after, &e.PrevHash, &e.Hash, &at,
		&e.Row.RequestID, &e.Row.SourceIPHash); err != nil {
		return Entry{}, err
	}
	if before != "" {
		e.Row.Before = json.RawMessage(before)
	}
	if after != "" {
		e.Row.After = json.RawMessage(after)
	}
	e.Row.At = NormalizeAt(at)
	return e, nil
}

// Head implements Store.
func (s *PostgresStore) Head(ctx context.Context) (Entry, error) {
	e, err := scanEntry(s.pool.QueryRow(ctx,
		`SELECT `+entryColumns+` FROM audit_log ORDER BY seq DESC NULLS LAST LIMIT 1`))
	if errors.Is(err, pgx.ErrNoRows) {
		return GenesisEntry(), nil
	}
	return e, err
}

// Latest implements Store.
func (s *PostgresStore) Latest(ctx context.Context) (Entry, error) {
	e, err := scanEntry(s.pool.QueryRow(ctx,
		`SELECT `+entryColumns+` FROM audit_log ORDER BY seq DESC NULLS LAST LIMIT 1`))
	if errors.Is(err, pgx.ErrNoRows) {
		return Entry{}, ErrChainEmpty
	}
	return e, err
}

// AtSeq implements Store.
func (s *PostgresStore) AtSeq(ctx context.Context, seq int64) (Entry, error) {
	if seq == 0 {
		return GenesisEntry(), nil
	}
	e, err := scanEntry(s.pool.QueryRow(ctx,
		`SELECT `+entryColumns+` FROM audit_log WHERE seq=$1`, seq))
	if errors.Is(err, pgx.ErrNoRows) {
		return Entry{}, ErrSeqNotFound
	}
	return e, err
}

// CountTo implements Store.
func (s *PostgresStore) CountTo(ctx context.Context, seq int64) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM audit_log WHERE seq <= $1`, seq).Scan(&n)
	return n, err
}

// Count implements Store.
func (s *PostgresStore) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM audit_log`).Scan(&n)
	return n, err
}

// Range implements Store with a streaming cursor: memory stays constant no
// matter how many rows are verified.
func (s *PostgresStore) Range(ctx context.Context, from, to int64, fn func(Entry) error) error {
	if err := checkSeqRange(from, to); err != nil {
		return err
	}
	q := `SELECT ` + entryColumns + ` FROM audit_log WHERE seq >= $1`
	args := []any{from}
	if to != 0 {
		q += ` AND seq <= $2`
		args = append(args, to)
	}
	q += ` ORDER BY seq ASC`
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return err
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	return rows.Err()
}

// List implements Store with keyset pagination on seq.
func (s *PostgresStore) List(ctx context.Context, f Filter) ([]Entry, int64, error) {
	validateFilter(&f)
	var args []any
	var where []string
	add := func(clause string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(clause, len(args)))
	}
	if f.AfterSeq > 0 {
		add("seq > $%d", f.AfterSeq)
	}
	if f.Actor != "" {
		add("actor = $%d", f.Actor)
	}
	if f.Action != "" {
		add("action = $%d", f.Action)
	}
	if f.ObjectType != "" {
		add("object_type = $%d", f.ObjectType)
	}
	if f.ObjectID != "" {
		add("object_id = $%d", f.ObjectID)
	}
	if !f.From.IsZero() {
		add("at >= $%d", f.From.UTC())
	}
	if !f.To.IsZero() {
		add("at <= $%d", f.To.UTC())
	}
	q := `SELECT ` + entryColumns + ` FROM audit_log`
	if len(where) > 0 {
		q += ` WHERE ` + strings.Join(where, ` AND `)
	}
	args = append(args, f.Limit+1)
	q += fmt.Sprintf(` ORDER BY seq ASC LIMIT $%d`, len(args))
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := make([]Entry, 0, f.Limit)
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	var next int64
	if len(out) > f.Limit {
		out = out[:f.Limit]
		next = out[len(out)-1].Seq
	}
	return out, next, nil
}

// InsertCheckpoint implements Store.
func (s *PostgresStore) InsertCheckpoint(ctx context.Context, cp Checkpoint, path string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO audit_checkpoints
			(schema_version, chain, seq, hash, row_count, digest, path, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		cp.SchemaVersion, cp.Chain, cp.Seq, cp.Hash, cp.RowCount, cp.Digest, path,
		mustParseAt(cp.CreatedAt))
	return err
}

const checkpointColumns = `schema_version, chain, seq, hash, row_count, digest, created_at`

func scanCheckpoint(row pgx.Row) (Checkpoint, error) {
	var cp Checkpoint
	var created time.Time
	if err := row.Scan(&cp.SchemaVersion, &cp.Chain, &cp.Seq, &cp.Hash,
		&cp.RowCount, &cp.Digest, &created); err != nil {
		return Checkpoint{}, err
	}
	cp.CreatedAt = created.UTC().Format(time.RFC3339Nano)
	return cp, nil
}

// LatestCheckpoint implements Store.
func (s *PostgresStore) LatestCheckpoint(ctx context.Context) (*Checkpoint, error) {
	cp, err := scanCheckpoint(s.pool.QueryRow(ctx,
		`SELECT `+checkpointColumns+` FROM audit_checkpoints ORDER BY seq DESC, id DESC LIMIT 1`))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &cp, nil
}

// Checkpoints implements Store.
func (s *PostgresStore) Checkpoints(ctx context.Context, limit int) ([]Checkpoint, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+checkpointColumns+` FROM audit_checkpoints ORDER BY seq DESC, id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Checkpoint
	for rows.Next() {
		cp, err := scanCheckpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, cp)
	}
	return out, rows.Err()
}

// Writable reports whether the connected role holds UPDATE on audit_log. A
// production deployment must return false (INSERT/SELECT only plus triggers).
func (s *PostgresStore) Writable(ctx context.Context) (bool, error) {
	var writable bool
	err := s.pool.QueryRow(ctx, `
		SELECT has_table_privilege(current_user, 'audit_log', 'UPDATE')
		    OR has_table_privilege(current_user, 'audit_log', 'DELETE')
		    OR has_table_privilege(current_user, 'audit_log', 'TRUNCATE')`).Scan(&writable)
	return writable, err
}
