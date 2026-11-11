package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

func testPostgres(t *testing.T) (*PostgresStore, *pgxpool.Pool) {
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
	for _, name := range []string{"0001_init.up.sql", "0002_phase3.up.sql"} {
		sql, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	store := NewPostgresStore(pool)
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	return store, pool
}

func TestPostgresDecisionRoundTrip(t *testing.T) {
	store, _ := testPostgres(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	score := 73
	decided := func(id, event, account string, at time.Time) *StoredDecision {
		d := &Decision{
			DecisionID: id, EventID: event, Decision: rules.Hold, RiskScore: 72, RuleScore: 60,
			MLScore: &score, MLStatus: rules.MLNotConfigured, RuleSetVersion: 1,
			Reasons:    []Reason{{Source: "rule", ID: "R-VEL-001", Text: "velocity", Weight: 60, Mode: rules.Enforce}},
			ShadowHits: []string{"R-SHD-001"},
			Features:   &features.FeatureVector{EventID: event, FeatureSchemaVersion: "1"},
			LatencyMS:  1.25, DecidedAt: at,
			AccountID: account, RecipientID: "acct_000456", Amount: 70000,
			TxnType: "P2P_SEND", EventTime: at, LatencyUS: 1250,
		}
		return d.ToStored()
	}
	first := decided("dec_00000000000000000000000001", "01JA1Z6Q2M8N4P7R9T2V5X8B3C", "acct_000123", now)
	second := decided("dec_00000000000000000000000002", "01JA1Z6Q2M8N4P7R9T2V5X8B3D", "acct_000123", now.Add(time.Second))
	if err := store.WriteDecisions(ctx, []*StoredDecision{first, second}); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteDecisions(ctx, []*StoredDecision{first}); err != nil {
		t.Fatalf("duplicate batch must be ignored: %v", err)
	}

	got, err := store.GetDecision(ctx, first.Decision.DecisionID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Decision.ShadowHits[0] != "R-SHD-001" || got.Decision.RiskScore != 72 ||
		got.Decision.MLScore == nil || *got.Decision.MLScore != 73 ||
		got.Decision.Reasons[0].ID != "R-VEL-001" || got.AccountID != "acct_000123" {
		t.Fatalf("round trip=%+v", got.Decision)
	}

	if _, err := store.DecisionByEventID(ctx, second.Decision.EventID); err != nil {
		t.Fatal(err)
	}

	page, cursor, err := store.ListDecisions(ctx, DecisionFilter{AccountID: "acct_000123", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || cursor == "" {
		t.Fatalf("first page=%d cursor=%q", len(page), cursor)
	}
	page2, _, err := store.ListDecisions(ctx, DecisionFilter{AccountID: "acct_000123", Limit: 1, Cursor: cursor})
	if err != nil || len(page2) != 1 || page2[0].Decision.DecisionID == page[0].Decision.DecisionID {
		t.Fatalf("second page=%v err=%v", page2, err)
	}
	byRule, _, err := store.ListDecisions(ctx, DecisionFilter{RuleID: "R-VEL-001", Limit: 10})
	if err != nil || len(byRule) != 2 {
		t.Fatalf("rule filter=%d err=%v", len(byRule), err)
	}
	injection, _, err := store.ListDecisions(ctx, DecisionFilter{AccountID: "' OR 1=1 --", Limit: 10})
	if err != nil || len(injection) != 0 {
		t.Fatalf("injection filter=%d err=%v", len(injection), err)
	}

	metrics, err := store.RuleMetrics(ctx, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Decisions != 2 || len(metrics.Rules) == 0 {
		t.Fatalf("metrics=%+v", metrics)
	}
	var fires, shadows int64
	for _, m := range metrics.Rules {
		if m.RuleID == "R-VEL-001" {
			fires = m.Fires
		}
		if m.RuleID == "R-SHD-001" {
			shadows = m.ShadowHits
		}
	}
	if fires != 2 || shadows != 2 {
		t.Fatalf("rule metrics=%+v", metrics.Rules)
	}
}

func TestPostgresListCRUD(t *testing.T) {
	store, _ := testPostgres(t)
	ctx := context.Background()
	expires := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)
	entry, err := store.AddList(ctx, ListEntry{
		ListType: "BLOCK", Subject: "ACCOUNT", Value: "acct_000999",
		Reason: "confirmed fraud", CreatedBy: "analyst@fade", ExpiresAt: &expires,
	})
	if err != nil {
		t.Fatal(err)
	}
	if entry.ID == 0 || entry.CreatedAt.IsZero() {
		t.Fatalf("entry=%+v", entry)
	}
	updated, err := store.AddList(ctx, ListEntry{
		ListType: "BLOCK", Subject: "ACCOUNT", Value: "acct_000999",
		Reason: "updated", CreatedBy: "admin@fade",
	})
	if err != nil || updated.ID != entry.ID || updated.Reason != "updated" {
		t.Fatalf("upsert=%+v err=%v", updated, err)
	}
	all, err := store.LoadLists(ctx)
	if err != nil || len(all) != 1 || all[0].CreatedBy != "admin@fade" {
		t.Fatalf("load=%+v err=%v", all, err)
	}
	filtered, err := store.ListEntries(ctx, "BLOCK", "ACCOUNT", "acct_000999", 10)
	if err != nil || len(filtered) != 1 {
		t.Fatalf("list=%+v err=%v", filtered, err)
	}
	none, err := store.ListEntries(ctx, "ALLOW", "", "", 10)
	if err != nil || len(none) != 0 {
		t.Fatalf("allow list=%+v err=%v", none, err)
	}
	ok, err := store.DeleteList(ctx, entry.ID)
	if err != nil || !ok {
		t.Fatalf("delete=%v err=%v", ok, err)
	}
	ok, err = store.DeleteList(ctx, entry.ID)
	if err != nil || ok {
		t.Fatalf("second delete=%v err=%v", ok, err)
	}
}
