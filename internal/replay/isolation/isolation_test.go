package isolation

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestPrefixAndGuard(t *testing.T) {
	if got := Prefix(42); got != "rp:{42}:" {
		t.Fatalf("prefix = %q", got)
	}
	if err := ValidatePrefix("rp:{42}:"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "rp:{}:", "rp:{x}:", "prod:", "rp:{42}"} {
		if err := ValidatePrefix(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
	g, err := NewGuard("rp:{42}:")
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Check("rp:{42}:w:acc:a"); err != nil {
		t.Fatal(err)
	}
	if err := g.Check("rp:{43}:w:acc:a"); !errors.Is(err, ErrProductionKey) {
		t.Fatalf("cross-namespace write not refused: %v", err)
	}
	if err := g.Check("s:acc:a"); !errors.Is(err, ErrProductionKey) {
		t.Fatalf("production write not refused: %v", err)
	}
}

func redisForTest(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("FADE_TEST_REDIS")
	if addr == "" {
		t.Skip("set FADE_TEST_REDIS to run Redis isolation tests")
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis: %v", err)
	}
	return c
}

func TestReserveReleaseAndFingerprint(t *testing.T) {
	c := redisForTest(t)
	ctx := context.Background()
	prodKey := "s:acc:guard-test"
	if err := c.Set(ctx, prodKey, "unchanged", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	defer c.Del(ctx, prodKey)

	before, _, err := Fingerprint(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	prefix := Prefix(9001)
	defer Release(ctx, c, prefix, false)
	if err := Reserve(ctx, c, prefix, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := Reserve(ctx, c, prefix, time.Minute); !errors.Is(err, ErrNamespaceCollision) {
		t.Fatalf("collision not refused: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := c.Set(ctx, prefix+"w:acc:test:"+string(rune('a'+i)), i, time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
	}
	after, _, err := Fingerprint(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("production fingerprint changed during namespaced writes")
	}
	n, err := Release(ctx, c, prefix, false)
	if err != nil {
		t.Fatal(err)
	}
	if n < 20 {
		t.Fatalf("released %d keys, want >= 20", n)
	}
	final, _, err := Fingerprint(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if final != before {
		t.Fatal("production fingerprint changed after release")
	}
}

func TestKeepDoesNotDelete(t *testing.T) {
	c := redisForTest(t)
	ctx := context.Background()
	prefix := Prefix(9002)
	defer Release(ctx, c, prefix, false)
	_ = Reserve(ctx, c, prefix, time.Minute)
	_ = c.Set(ctx, prefix+"x", "1", time.Minute).Err()
	if _, err := Release(ctx, c, prefix, true); err != nil {
		t.Fatal(err)
	}
	if n, _ := c.Exists(ctx, prefix+"x").Result(); n != 1 {
		t.Fatal("keep=true deleted namespace keys")
	}
}
