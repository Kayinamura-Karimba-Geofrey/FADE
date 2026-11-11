package rules

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PersistHooks expose persistence health to metrics without importing obs.
type PersistHooks struct {
	OnResult     func(result string)
	OnQueueDepth func(depth int)
	OnSpoolBytes func(bytes int64)
}

type activation struct {
	Actor   string          `json:"actor"`
	Content json.RawMessage `json:"content"`
	Version int             `json:"version"`
	Name    string          `json:"name"`
}

// PostgresPersister writes rule versions and audit rows asynchronously. The
// queue is bounded; overflow and database failures go to a disk spool and are
// replayed in order on recovery.
type PostgresPersister struct {
	pool     *pgxpool.Pool
	logger   *slog.Logger
	hooks    PersistHooks
	queue    chan activation
	spoolDir string
	spoolMu  sync.Mutex
	stop     chan struct{}
	done     sync.WaitGroup
}

// NewPostgresPersister connects, ensures the rule tables exist, and starts the
// writer. A nil pool is returned as an error; callers fall back to files only.
func NewPostgresPersister(ctx context.Context, dsn, spoolDir string, logger *slog.Logger, hooks PersistHooks) (*PostgresPersister, error) {
	if logger == nil {
		logger = slog.Default()
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	p := &PostgresPersister{
		pool: pool, logger: logger, hooks: hooks,
		queue: make(chan activation, 1024), spoolDir: spoolDir, stop: make(chan struct{}),
	}
	if err := p.ensureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	p.done.Add(1)
	go p.run()
	return p, nil
}

func (p *PostgresPersister) ensureSchema(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS rule_sets (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    version INT NOT NULL,
    content JSONB NOT NULL,
    content_hash TEXT NOT NULL,
    config JSONB NOT NULL,
    author TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (name, version)
);
CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    before JSONB,
    after JSONB,
    prev_hash TEXT,
    hash TEXT NOT NULL,
    at TIMESTAMPTZ NOT NULL DEFAULT now()
);`
	_, err := p.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("ensure rule tables: %w", err)
	}
	return nil
}

// SaveActivation enqueues an immutable version. It never blocks on Postgres.
func (p *PostgresPersister) SaveActivation(_ context.Context, rs *RuleSet, actor string) error {
	content, err := json.Marshal(rs.File())
	if err != nil {
		return fmt.Errorf("marshal rule set: %w", err)
	}
	op := activation{Actor: actor, Content: content, Version: rs.Version, Name: rs.RuleSet}
	select {
	case p.queue <- op:
		p.reportDepth(len(p.queue))
		return nil
	default:
		if err := p.spool(op); err != nil {
			return err
		}
		return fmt.Errorf("persist queue full; activation spooled")
	}
}

func (p *PostgresPersister) run() {
	defer p.done.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case op := <-p.queue:
			p.reportDepth(len(p.queue))
			if err := p.write(op); err != nil {
				p.report("error")
				p.logger.Error("rule activation persist failed; spooling", "error", err)
				_ = p.spool(op)
				continue
			}
			p.report("ok")
			_ = p.drainSpool()
		case <-ticker.C:
			_ = p.drainSpool()
		}
	}
}

func (p *PostgresPersister) write(op activation) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var content RuleSetFile
	if err := json.Unmarshal(op.Content, &content); err != nil {
		return fmt.Errorf("decode activation: %w", err)
	}
	config, _ := json.Marshal(content.Config)
	canon, err := CanonicalJSON(op.Content)
	if err != nil {
		return err
	}
	hash, err := HashContent(op.Content)
	if err != nil {
		return err
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var before json.RawMessage
	err = tx.QueryRow(ctx,
		`SELECT content FROM rule_sets WHERE name=$1 AND active=true AND version<>$2 LIMIT 1`,
		op.Name, op.Version).Scan(&before)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE rule_sets SET active=false WHERE name=$1 AND active=true AND version<>$2`,
		op.Name, op.Version); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO rule_sets (name, version, content, content_hash, config, author, active)
		 VALUES ($1,$2,$3,$4,$5,$6,true)
		 ON CONFLICT (name, version) DO UPDATE
		 SET content=EXCLUDED.content, content_hash=EXCLUDED.content_hash,
		     config=EXCLUDED.config, author=EXCLUDED.author, active=true`,
		op.Name, op.Version, canon, hash, config, op.Actor); err != nil {
		return err
	}

	var prevHash string
	err = tx.QueryRow(ctx, `SELECT COALESCE(hash,'') FROM audit_log ORDER BY id DESC LIMIT 1`).Scan(&prevHash)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	row := NewAuditRow(op.Actor, "rules.activate", "rule_set", fmt.Sprintf("%s/%d", op.Name, op.Version),
		before, canon, time.Now())
	chained, err := ChainHash(prevHash, row)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO audit_log (actor, action, object_type, object_id, before, after, prev_hash, hash, at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		row.Actor, row.Action, row.ObjectType, row.ObjectID, row.Before, row.After,
		prevHash, chained, row.At); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	p.logger.Info("rule set persisted", "rule_set", op.Name, "version", op.Version, "hash", hash)
	return nil
}

// Active returns the active version or nil when none exists.
func (p *PostgresPersister) Active(ctx context.Context) (*RuleSet, error) {
	var content []byte
	err := p.pool.QueryRow(ctx,
		`SELECT content FROM rule_sets WHERE active=true ORDER BY version DESC LIMIT 1`).Scan(&content)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return Load(content)
}

// Versions lists stored versions newest first.
func (p *PostgresPersister) Versions(ctx context.Context) ([]Snapshot, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT name, version, content_hash, author, active, created_at
		 FROM rule_sets ORDER BY created_at DESC, version DESC LIMIT 200`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Snapshot
	for rows.Next() {
		var s Snapshot
		var created time.Time
		if err := rows.Scan(&s.RuleSet, &s.Version, &s.Hash, &s.Author, &s.Active, &created); err != nil {
			return nil, err
		}
		s.CompiledAt = created.UTC().Format(time.RFC3339Nano)
		s.Source = "postgres"
		out = append(out, s)
	}
	return out, rows.Err()
}

func (p *PostgresPersister) spoolPath() string {
	return filepath.Join(p.spoolDir, "rules-spool.ndjson")
}

func (p *PostgresPersister) spool(op activation) error {
	if p.spoolDir == "" {
		return errors.New("no spool directory")
	}
	if err := os.MkdirAll(p.spoolDir, 0o755); err != nil {
		return err
	}
	p.spoolMu.Lock()
	defer p.spoolMu.Unlock()
	f, err := os.OpenFile(p.spoolPath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	defer f.Close()
	raw, err := json.Marshal(op)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return err
	}
	p.reportSpool()
	return nil
}

func (p *PostgresPersister) drainSpool() error {
	p.spoolMu.Lock()
	defer p.spoolMu.Unlock()
	path := p.spoolPath()
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var ops []activation
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	for sc.Scan() {
		var op activation
		if err := json.Unmarshal(sc.Bytes(), &op); err != nil {
			continue
		}
		ops = append(ops, op)
	}
	_ = f.Close()
	if len(ops) == 0 {
		_ = os.Remove(path)
		return nil
	}
	for i, op := range ops {
		if err := p.write(op); err != nil {
			// Rewrite the remaining suffix to preserve order.
			remaining := ops[i:]
			tmp := path + ".tmp"
			out, ferr := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
			if ferr == nil {
				for _, rem := range remaining {
					raw, _ := json.Marshal(rem)
					_, _ = out.Write(append(raw, '\n'))
				}
				_ = out.Close()
				_ = os.Rename(tmp, path)
			}
			p.reportSpool()
			return err
		}
	}
	_ = os.Remove(path)
	p.report("ok")
	p.reportSpool()
	return nil
}

func (p *PostgresPersister) report(result string) {
	if p.hooks.OnResult != nil {
		p.hooks.OnResult(result)
	}
}

func (p *PostgresPersister) reportDepth(d int) {
	if p.hooks.OnQueueDepth != nil {
		p.hooks.OnQueueDepth(d)
	}
}

func (p *PostgresPersister) reportSpool() {
	if p.hooks.OnSpoolBytes == nil {
		return
	}
	st, err := os.Stat(p.spoolPath())
	if err != nil {
		p.hooks.OnSpoolBytes(0)
		return
	}
	p.hooks.OnSpoolBytes(st.Size())
}

// Close stops the writer and closes the pool.
func (p *PostgresPersister) Close() {
	close(p.stop)
	p.done.Wait()
	p.pool.Close()
}

// Pool exposes the underlying pool for the decision pipeline and admin queries.
func (p *PostgresPersister) Pool() *pgxpool.Pool { return p.pool }
