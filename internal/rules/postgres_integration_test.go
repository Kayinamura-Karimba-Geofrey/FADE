package rules

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func rulesPostgresDSN(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("FADE_TEST_POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := postgrescontainer.Run(ctx, "postgres:16.4-alpine3.20",
		postgrescontainer.WithDatabase("fade"), postgrescontainer.WithUsername("fade"),
		postgrescontainer.WithPassword("fade"), postgrescontainer.BasicWaitStrategies())
	if err != nil {
		t.Skipf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	dsn, err := container.ConnectionString(context.Background(), "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	return dsn
}

func waitForVersion(t *testing.T, p *PostgresPersister, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rs, err := p.Active(context.Background())
		if err == nil && rs != nil && rs.Version == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("active version never became %d", want)
}

func TestPostgresPersisterIntegration(t *testing.T) {
	dsn := rulesPostgresDSN(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if _, err := NewPostgresPersister(ctx, "postgres://nobody:nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=1", "", logger, PersistHooks{}); err == nil {
		t.Fatal("unreachable postgres must fail")
	}

	var results, depths, spoolBytes atomic.Int64
	p, err := NewPostgresPersister(ctx, dsn, t.TempDir(), logger, PersistHooks{
		OnResult:     func(string) { results.Add(1) },
		OnQueueDepth: func(int) { depths.Add(1) },
		OnSpoolBytes: func(int64) { spoolBytes.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if rs, err := p.Active(ctx); err != nil || rs != nil {
		t.Fatalf("empty active=%v err=%v", rs, err)
	}

	rs1 := mustLoad(t, oneRule(`{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"amount","op":">","v":100},"reason":"x"}`))
	if err := p.SaveActivation(ctx, rs1, "tester"); err != nil {
		t.Fatal(err)
	}
	waitForVersion(t, p, 1)

	rs2 := mustLoad(t, strings.Replace(oneRule(`{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":20,"action":"HOLD","when":{"f":"amount","op":">","v":100},"reason":"y"}`), `"version":1`, `"version":2`, 1))
	if err := p.SaveActivation(ctx, rs2, "tester"); err != nil {
		t.Fatal(err)
	}
	waitForVersion(t, p, 2)
	versions, err := p.Versions(ctx)
	if err != nil || len(versions) < 2 || versions[0].Version != 2 || !versions[0].Active {
		t.Fatalf("versions=%+v err=%v", versions, err)
	}
	if versions[0].Source != "postgres" || versions[0].CompiledAt == "" {
		t.Fatalf("snapshot=%+v", versions[0])
	}
	if p.Pool() == nil {
		t.Fatal("pool accessor")
	}
	if err := p.write(activation{Name: "bad", Version: 99, Content: json.RawMessage("{")}); err == nil {
		t.Fatal("invalid content must fail")
	}

	// An unstarted writer with a full queue spools instead of blocking.
	dir := t.TempDir()
	direct := &PostgresPersister{
		pool: p.Pool(), logger: logger, hooks: PersistHooks{OnSpoolBytes: func(int64) { spoolBytes.Add(1) }},
		queue: make(chan activation, 1), spoolDir: dir, stop: make(chan struct{}),
	}
	rs3 := mustLoad(t, strings.Replace(oneRule(`{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":30,"action":"BLOCK","when":{"f":"amount","op":">","v":100},"reason":"z"}`), `"version":1`, `"version":3`, 1))
	if err := direct.SaveActivation(ctx, rs3, "direct"); err != nil {
		t.Fatal(err)
	}
	if err := direct.SaveActivation(ctx, rs3, "direct"); err == nil {
		t.Fatal("full queue must report the spooled activation")
	}
	if st, err := os.Stat(filepath.Join(dir, "rules-spool.ndjson")); err != nil || st.Size() == 0 {
		t.Fatalf("spool stat=%v err=%v", st, err)
	}
	if err := direct.drainSpool(); err != nil {
		t.Fatal(err)
	}
	waitForVersion(t, p, 3)
	if _, err := os.Stat(filepath.Join(dir, "rules-spool.ndjson")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("drained spool must be removed")
	}
	// A spool with no directory is an error, not a silent drop.
	if err := (&PostgresPersister{spoolDir: ""}).spool(activation{}); err == nil {
		t.Fatal("empty spool dir must fail")
	}
	// Hooks are optional and must not panic.
	(&PostgresPersister{}).report("ok")
	(&PostgresPersister{}).reportDepth(1)
	(&PostgresPersister{}).reportSpool()

	p.Close()
	if results.Load() == 0 || depths.Load() == 0 || spoolBytes.Load() == 0 {
		t.Fatalf("hooks results=%d depths=%d spool=%d", results.Load(), depths.Load(), spoolBytes.Load())
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var audits int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_log`).Scan(&audits); err != nil || audits < 3 {
		t.Fatalf("audit rows=%d err=%v", audits, err)
	}
}
