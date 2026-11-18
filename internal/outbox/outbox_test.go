package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

type fakePublisher struct {
	failFor map[string]bool
	sent    []string
}

func (p *fakePublisher) Publish(_ context.Context, topic, key string, payload []byte) error {
	if p.failFor[key] {
		return errors.New("broker unavailable")
	}
	p.sent = append(p.sent, topic+"/"+key+":"+string(payload))
	return nil
}

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("FADE_TEST_POSTGRES_DSN")
	cleanup := func() {}
	if dsn == "" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		container, err := postgrescontainer.Run(ctx, "postgres:16.4-alpine3.20",
			postgrescontainer.WithDatabase("fade"), postgrescontainer.WithUsername("fade"),
			postgrescontainer.WithPassword("fade"), postgrescontainer.BasicWaitStrategies())
		cancel()
		if err != nil {
			t.Skipf("start postgres: %v", err)
		}
		cleanup = func() { _ = testcontainers.TerminateContainer(container) }
		ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		dsn, err = container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			cleanup()
			t.Fatal(err)
		}
	}
	t.Cleanup(cleanup)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for _, name := range []string{"0001_init.up.sql", "0002_phase3.up.sql",
		"0003_explain_audit.up.sql", "0004_replay.up.sql", "0005_cases_users.up.sql"} {
		sql, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	return pool
}

func TestRelayPublishesOnceAndRetriesFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	pool := testPool(t)
	ctx := context.Background()
	for key, payload := range map[string]string{
		"dec-1": `{"label":"FRAUD"}`,
		"dec-2": `{"label":"LEGIT"}`,
	} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO outbox (topic, key, payload) VALUES ('labels.feedback',$1,$2)`, key, payload); err != nil {
			t.Fatal(err)
		}
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	pub := &fakePublisher{failFor: map[string]bool{"dec-2": true}}
	relay := NewRelay(pool, pub, log)

	if err := relay.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if len(pub.sent) != 1 || !strings.HasPrefix(pub.sent[0], "labels.feedback/dec-1:") {
		t.Fatalf("sent = %v", pub.sent)
	}
	var decoded struct {
		Label string `json:"label"`
	}
	if err := Decode([]byte(pub.sent[0][strings.Index(pub.sent[0], ":")+1:]), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Label != "FRAUD" {
		t.Fatalf("payload = %+v", decoded)
	}
	pending, err := relay.Unpublished(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("unpublished = %d, want the failed row only", pending)
	}

	// The failed row is retried once the broker recovers.
	pub.failFor = map[string]bool{}
	if err := relay.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if len(pub.sent) != 2 {
		t.Fatalf("sent = %v", pub.sent)
	}
	if pending, _ := relay.Unpublished(ctx); pending != 0 {
		t.Fatalf("unpublished = %d after a successful retry", pending)
	}
}

func TestRelaySkipsPoisonRowsAfterMaxAttempts(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	pool := testPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox (topic, key, payload, attempts) VALUES ('labels.feedback','dec-dead','{}',20)`); err != nil {
		t.Fatal(err)
	}
	pub := &fakePublisher{}
	relay := NewRelay(pool, pub, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := relay.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if len(pub.sent) != 0 {
		t.Fatalf("a row at the attempt limit must not be retried, sent %v", pub.sent)
	}
}
