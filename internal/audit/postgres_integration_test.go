package audit

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestPostgresChainIntegration exercises the real append protocol, trigger and
// checkpoint path. It is opt-in because it needs a migrated database:
//
//	FADE_TEST_DSN=postgres://postgres:fade@127.0.0.1:55432/fade?sslmode=disable \
//	  go test ./internal/audit -run Postgres
func TestPostgresChainIntegration(t *testing.T) {
	dsn := os.Getenv("FADE_TEST_DSN")
	if dsn == "" {
		t.Skip("FADE_TEST_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	store := NewPostgresStore(pool)
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	// Concurrent writers must not fork the chain.
	const writers, each = 20, 25
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				_, err := store.Append(ctx, Row{
					Actor: "integration", Action: string(ActionConfigChange),
					ObjectType: "config", ObjectID: "k",
					After: json.RawMessage(`{"key":"k","value":1}`),
					At:    NormalizeAt(time.Now()),
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	res, err := VerifyChain(ctx, store, VerifyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK {
		t.Fatalf("chain invalid: %+v", res.FirstBreak)
	}
	if res.Checked < int64(writers*each) {
		t.Fatalf("checked %d rows", res.Checked)
	}
	t.Logf("verified %d rows at %.0f rows/s", res.Checked, res.RowsPerSec)

	// The append-only trigger must reject mutation even for the test role.
	head, err := store.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE audit_log SET actor='evil' WHERE seq=$1`, head.Seq); err == nil {
		t.Fatal("trigger allowed UPDATE")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM audit_log WHERE seq=$1`, head.Seq); err == nil {
		t.Fatal("trigger allowed DELETE")
	}

	// Checkpoint signs, stores and exports.
	dir := t.TempDir()
	key := []byte("integration-key")
	cp, err := NewCheckpointService(store, key, dir, Hooks{}).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cp.Digest == "" {
		t.Fatal("checkpoint not signed")
	}
	if _, err := LoadCheckpointFiles(dir); err != nil {
		t.Fatal(err)
	}
	after, err := VerifyChain(ctx, store, VerifyOptions{CheckpointDir: dir, HMACKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if !after.OK {
		t.Fatalf("chain invalid after checkpoint: %+v", after.FirstBreak)
	}

	// The test role is a superuser, so it still holds UPDATE at the privilege
	// level even though the trigger blocks it.
	if writable, err := store.Writable(ctx); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("Writable=%v (superuser test role)", writable)
	}
}
