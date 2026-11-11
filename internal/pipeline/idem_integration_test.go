package pipeline

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("FADE_TEST_REDIS_ADDR")
	cleanup := func() {}
	if addr == "" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image: "redis:7.4-alpine", ExposedPorts: []string{"6379/tcp"},
				WaitingFor: wait.ForListeningPort("6379/tcp").WithStartupTimeout(time.Minute),
			},
			Started: true,
		})
		cancel()
		if err != nil {
			t.Skipf("start redis: %v", err)
		}
		cleanup = func() { _ = testcontainers.TerminateContainer(container) }
		ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
		host, err := container.Host(ctx)
		if err != nil {
			cleanup()
			t.Fatal(err)
		}
		port, err := container.MappedPort(ctx, "6379/tcp")
		cancel()
		if err != nil {
			cleanup()
			t.Fatal(err)
		}
		addr = host + ":" + port.Port()
	}
	t.Cleanup(cleanup)
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("ping redis: %v", err)
	}
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return client
}

func TestRedisIdempotencyIntegration(t *testing.T) {
	ctx := context.Background()
	idem := NewRedisIdempotency(testRedis(t))

	got, owner, err := idem.Begin(ctx, "evt_1", "hash_a")
	if err != nil || !owner || got != nil {
		t.Fatalf("begin owner=%v got=%s err=%v", owner, got, err)
	}
	if _, ok, err := idem.Get(ctx, "evt_1"); err != nil || ok {
		t.Fatalf("pending get ok=%v err=%v", ok, err)
	}
	if _, _, err := idem.Begin(ctx, "evt_1", "hash_other"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflict err=%v", err)
	}

	// A duplicate waits for the owner and then returns the stored decision.
	waiter := make(chan []byte, 1)
	go func() {
		d, owner, err := idem.Begin(ctx, "evt_1", "hash_a")
		if err == nil && !owner {
			waiter <- d
		}
	}()
	time.Sleep(10 * time.Millisecond)
	if err := idem.Complete(ctx, "evt_1", "hash_a", []byte(`{"decision":"BLOCK"}`), time.Hour); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-waiter:
		if string(d) != `{"decision":"BLOCK"}` {
			t.Fatalf("waiter got %s", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not receive decision")
	}

	if d, ok, err := idem.Get(ctx, "evt_1"); err != nil || !ok || string(d) != `{"decision":"BLOCK"}` {
		t.Fatalf("get=%s ok=%v err=%v", d, ok, err)
	}
	if d, owner, err := idem.Begin(ctx, "evt_1", "hash_a"); err != nil || owner || string(d) != `{"decision":"BLOCK"}` {
		t.Fatalf("replay owner=%v d=%s err=%v", owner, d, err)
	}
	if err := idem.Complete(ctx, "evt_1", "mismatch", []byte(`{}`), time.Hour); err != nil {
		// Complete overwrites by design; only Begin enforces the hash.
		t.Fatal(err)
	}
	if _, ok, err := idem.Get(ctx, "missing"); err != nil || ok {
		t.Fatalf("missing ok=%v err=%v", ok, err)
	}

	// A pending reservation that nobody completes exhausts the poll window.
	if _, owner, err := idem.Begin(ctx, "evt_stuck", "h"); err != nil || !owner {
		t.Fatalf("stuck owner=%v err=%v", owner, err)
	}
	if _, _, err := idem.Begin(ctx, "evt_stuck", "h"); !errors.Is(err, ErrIdempotencyInFlight) {
		t.Fatalf("in-flight err=%v", err)
	}
}
