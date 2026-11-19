package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
)

// TestFeaturesBackfillIsIdempotent is the C1 regression: replaying the same
// raw window twice must leave every feature window and counter byte-identical,
// because the dedupe key is written inside the same Lua aggregate.
func TestFeaturesBackfillIsIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("backfill integration test")
	}
	addr := os.Getenv("FADE_TEST_REDIS_ADDR")
	cleanup := func() {}
	if addr == "" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		container, err := rediscontainer.Run(ctx, "redis:7.4.1-alpine3.20")
		cancel()
		if err != nil {
			t.Skipf("no Docker for Redis testcontainer: %v", err)
		}
		cleanup = func() { _ = testcontainers.TerminateContainer(container) }
		endpoint, err := container.Endpoint(context.Background(), "")
		if err != nil {
			t.Fatal(err)
		}
		addr = endpoint
	}
	defer cleanup()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	eventsPath := filepath.Join(dir, "events.jsonl")
	var lines []string
	base := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	alphabet := "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	ulid := func(ms int64, salt int) string {
		var ts [10]byte
		v := uint64(ms)
		for i := 9; i >= 0; i-- {
			ts[i] = alphabet[v&31]
			v >>= 5
		}
		rest := make([]byte, 16)
		seed := uint64(salt+1) * 2654435761
		for i := range rest {
			seed = seed*6364136223846793005 + 1442695040888963407
			rest[i] = alphabet[(seed>>33)&31]
		}
		return string(ts[:]) + string(rest)
	}
	for i := 0; i < 40; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		amount := 1000 + i
		ev := fmt.Sprintf(`{"event_id":%q,"event_time":%q,"txn_type":"P2P_SEND","amount":%d,`+
			`"currency":"RWF","fee":%d,`+
			`"sender":{"account_id":"acct_000001","msisdn_hash":"h_0123456789abcdef0123456789abcdef",`+
			`"device_id":"dev_000001_1","sim_serial_hash":"sim_0123456789abcdef0123456789abcdef",`+
			`"geo":{"lat":-1.95,"lon":30.06,"cell_id":"KGL-01"},"account_age_days":120,"kyc_tier":2,`+
			`"balance_before":500000},`+
			`"recipient":{"account_id":"acct_000002","msisdn_hash":"h_fedcba9876543210fedcba9876543210",`+
			`"is_agent":false,"first_seen_by_sender":false},"channel":"APP","session_id":"p2p-ABCD1234"}`,
			ulid(ts.UnixMilli(), i), ts.Format("2006-01-02T15:04:05.000Z"), amount, events.FeeForAmount(int64(amount)))
		if _, verr := events.Parse([]byte(ev), ts); verr != nil {
			t.Fatalf("fixture %d invalid: %v (detail %s)", i, verr, verr.Detail)
		}
		lines = append(lines, ev)
	}
	if err := os.WriteFile(eventsPath, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	args := []string{
		"-from", base.Add(-time.Minute).Format(time.RFC3339),
		"-to", base.Add(time.Hour).Format(time.RFC3339),
		"-events", eventsPath,
		"-redis", addr,
		"-config", "../../config/features.yaml",
	}
	if err := featuresBackfill(args); err != nil {
		t.Fatalf("first backfill: %v", err)
	}
	first := snapshot(t, ctx, rdb)
	if len(first) == 0 {
		t.Fatal("backfill wrote no feature keys")
	}
	if err := featuresBackfill(args); err != nil {
		t.Fatalf("second backfill: %v", err)
	}
	second := snapshot(t, ctx, rdb)
	if len(first) != len(second) {
		t.Fatalf("key count changed: %d -> %d", len(first), len(second))
	}
	for key, value := range first {
		if second[key] != value {
			t.Fatalf("key %s changed after idempotent replay:\n%s\n%s", key, value, second[key])
		}
	}
}

func snapshot(t *testing.T, ctx context.Context, rdb *redis.Client) map[string]string {
	t.Helper()
	out := map[string]string{}
	iter := rdb.Scan(ctx, 0, "*", 1000).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		if strings.HasPrefix(key, "dedupe:") {
			continue // dedupe TTLs legitimately differ between two runs
		}
		kind, err := rdb.Type(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		var value string
		switch kind {
		case "zset":
			members, err := rdb.ZRangeWithScores(ctx, key, 0, -1).Result()
			if err != nil {
				t.Fatal(err)
			}
			parts := make([]string, 0, len(members))
			for _, m := range members {
				parts = append(parts, fmt.Sprintf("%v=%v", m.Member, m.Score))
			}
			value = strings.Join(parts, ";")
		case "hash":
			m, err := rdb.HGetAll(ctx, key).Result()
			if err != nil {
				t.Fatal(err)
			}
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			parts := make([]string, 0, len(keys))
			for _, k := range keys {
				parts = append(parts, k+"="+m[k])
			}
			value = strings.Join(parts, ";")
		case "string":
			value, err = rdb.Get(ctx, key).Result()
			if err != nil {
				t.Fatal(err)
			}
		default:
			value = kind
		}
		out[key] = value
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
