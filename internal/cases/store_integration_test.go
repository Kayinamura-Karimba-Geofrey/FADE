package cases

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// testPool starts (or connects to) Postgres and applies every migration, so the
// store is exercised against the real schema rather than a hand-written one.
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

func newTestStore(t *testing.T) *Store {
	t.Helper()
	pool := testPool(t)
	// Keep the integration test independent of the repository config file.
	return NewStore(pool, DefaultConfig())
}

func TestStoreLifecycleFourEyesAndLabels(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store := newTestStore(t)
	ctx := context.Background()
	// Postgres timestamps are microsecond precision, so compare at that scale.
	now := time.Now().UTC().Truncate(time.Microsecond)

	created, err := store.Create(ctx, CreateInput{
		Source: SourceDecision, DecisionClass: "BLOCK", PrimaryDecision: "dec-1",
		AccountID: "ACC-1", RiskScore: 80, AmountTotal: 6_000_000,
		Actor: "case-worker", ActorRole: "service", Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Status != StatusOpen {
		t.Fatalf("status=%s", created.Status)
	}
	if created.Priority != 100 {
		t.Fatalf("priority=%d want 100 (clamped)", created.Priority)
	}
	if created.SLADueAt == nil || !created.SLADueAt.Equal(now.Add(30*time.Minute)) {
		t.Fatalf("sla_due_at=%v want %v", created.SLADueAt, now.Add(30*time.Minute))
	}

	// A second decision for the same account joins this case.
	withLink, err := store.Link(ctx, created.ID, "dec-linked", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(withLink.LinkedDecisions) != 1 || withLink.Priority != 100 {
		t.Fatalf("linked case = %+v", withLink)
	}

	// Stale version is rejected.
	if _, err := store.Claim(ctx, created.ID, "analyst-1", "analyst", created.Version+5); err != ErrVersionConflict {
		t.Fatalf("want version conflict, got %v", err)
	}
	claimed, err := store.Claim(ctx, created.ID, "analyst-1", "analyst", withLink.Version)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Status != StatusInReview || claimed.Assignee == nil || *claimed.Assignee != "analyst-1" {
		t.Fatalf("claim result = %+v", claimed)
	}

	// Block overturn needs a second reviewer, so no label yet.
	pending, err := store.Resolve(ctx, created.ID, Resolution{
		Outcome: OutcomeLegit, ReasonCode: "false positive", Confidence: "HIGH",
		PropagateLinked: true,
	}, "analyst-1", "analyst", claimed.Version)
	if err != nil {
		t.Fatal(err)
	}
	if !pending.PendingApproval || pending.ResolvedAt != nil {
		t.Fatalf("resolution should be pending: %+v", pending)
	}
	if labels := countLabels(t, ctx, store, "dec-1"); labels != 0 {
		t.Fatalf("unapproved verdict must not write a label, got %d", labels)
	}

	if _, err := store.Approve(ctx, created.ID, "analyst-1", "analyst", pending.Version); err != ErrSelfApproval {
		t.Fatalf("want self approval rejection, got %v", err)
	}
	approved, err := store.Approve(ctx, created.ID, "lead-1", "lead", pending.Version)
	if err != nil {
		t.Fatal(err)
	}
	if approved.PendingApproval || approved.ResolvedAt == nil {
		t.Fatalf("approved case = %+v", approved)
	}
	if labels := countLabels(t, ctx, store, "dec-1"); labels != 1 {
		t.Fatalf("approved verdict must write one label, got %d", labels)
	}
	if labels := countLabels(t, ctx, store, "dec-linked"); labels != 1 {
		t.Fatalf("propagation must label the linked decision, got %d", labels)
	}
	var source string
	if err := store.pool.QueryRow(ctx,
		`SELECT source FROM label_history WHERE event_id=$1`, "dec-linked").Scan(&source); err != nil {
		t.Fatal(err)
	}
	if source != "PROPAGATED" {
		t.Fatalf("linked label source=%q want PROPAGATED", source)
	}
	if !approved.PropagateLinked {
		t.Fatal("the propagation choice must survive the approval")
	}

	for _, key := range []string{"dec-1", "dec-linked"} {
		var feedback int
		if err := store.pool.QueryRow(ctx,
			`SELECT count(*) FROM outbox WHERE topic='labels.feedback' AND key=$1`, key).Scan(&feedback); err != nil {
			t.Fatal(err)
		}
		if feedback != 1 {
			t.Fatalf("%s: labels.feedback rows=%d", key, feedback)
		}
	}

	// A human label on a linked decision is never overwritten by propagation.
	if _, err := store.pool.Exec(ctx,
		`INSERT INTO labels (event_id, label, source, scenario, created_at)
		 VALUES ('dec-human','LEGIT','ANALYST','HOLD',now())`); err != nil {
		t.Fatal(err)
	}
	if err := pgx.BeginFunc(ctx, store.pool, func(tx pgx.Tx) error {
		return store.writePropagatedLabel(ctx, tx, created, "dec-human", "FRAUD", time.Now().UTC())
	}); err != nil {
		t.Fatal(err)
	}
	var humanHistory int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM label_history WHERE event_id='dec-human'`).Scan(&humanHistory); err != nil {
		t.Fatal(err)
	}
	if humanHistory != 0 {
		t.Fatal("propagation must skip an already-labelled decision")
	}

	reopened, err := store.Reopen(ctx, created.ID, "lead-1", "lead", "chargeback received", approved.Version)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Status != StatusReopened || reopened.Outcome != nil {
		t.Fatalf("reopened case = %+v", reopened)
	}

	timeline, err := store.Timeline(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"opened", "linked", "claimed", "resolved", "approved", "reopened"}
	if len(timeline) != len(want) {
		t.Fatalf("timeline types = %v", eventTypes(timeline))
	}
	for i, typ := range want {
		if timeline[i].Type != typ {
			t.Fatalf("timeline[%d]=%s want %s (%v)", i, timeline[i].Type, typ, eventTypes(timeline))
		}
	}
}

func TestStoreRejectsIllegalTransitionsAndBadNotes(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.Create(ctx, CreateInput{
		Source: SourceDecision, DecisionClass: "HOLD", PrimaryDecision: "dec-2",
		AccountID: "ACC-2", RiskScore: 40, AmountTotal: 10_000,
		Actor: "case-worker", ActorRole: "service",
	})
	if err != nil {
		t.Fatal(err)
	}
	// OPEN -> RESOLVED is not allowed; the case must be claimed first.
	if _, err := store.Resolve(ctx, created.ID, Resolution{
		Outcome: OutcomeFraud, ReasonCode: "confirmed",
	}, "analyst-1", "analyst", created.Version); err == nil {
		t.Fatal("expected illegal transition from OPEN to RESOLVED")
	}
	if _, err := store.AddNote(ctx, created.ID, "analyst-1", "analyst", "call 0781234567"); err != ErrRedactedContent {
		t.Fatalf("redaction: got %v", err)
	}
	if _, err := store.AddNote(ctx, created.ID, "analyst-1", "analyst", "customer confirmed the transfer"); err != nil {
		t.Fatal(err)
	}
	// Adding a note bumps the version, so re-read before the next mutation.
	current, err := store.Get(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Inconclusive requires a follow-up date.
	claimed, err := store.Claim(ctx, created.ID, "analyst-1", "analyst", current.Version)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Resolve(ctx, created.ID, Resolution{
		Outcome: OutcomeInconclusive, ReasonCode: "need contact",
	}, "analyst-1", "analyst", claimed.Version); err != ErrFollowUpRequired {
		t.Fatalf("want follow-up requirement, got %v", err)
	}
	// Reopen only applies to resolved cases.
	if _, err := store.Reopen(ctx, created.ID, "lead-1", "lead", "reason", claimed.Version); err == nil {
		t.Fatal("expected reopen rejection for an in-review case")
	}
}

func TestStoreGroupingLinksInsteadOfDuplicating(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	first := &DecisionRef{ID: "dec-g1", AccountID: "ACC-G", Decision: "HOLD",
		RiskScore: 50, Amount: 100_000, DecidedAt: now}
	if _, created, err := store.OpenFromDecision(ctx, first, SourceDecision, now); err != nil || !created {
		t.Fatalf("first decision: created=%v err=%v", created, err)
	}
	second := &DecisionRef{ID: "dec-g2", AccountID: "ACC-G", Decision: "HOLD",
		RiskScore: 50, Amount: 200_000, DecidedAt: now.Add(time.Minute)}
	linked, created, err := store.OpenFromDecision(ctx, second, SourceDecision, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("second decision inside the grouping window must not open a case")
	}
	if len(linked.LinkedDecisions) != 1 || linked.LinkedDecisions[0] != "dec-g2" {
		t.Fatalf("linked=%v", linked.LinkedDecisions)
	}
	if linked.AmountTotal != 300_000 {
		t.Fatalf("amount_total=%d", linked.AmountTotal)
	}
	// Replaying the same decision is a no-op.
	if _, created, err := store.OpenFromDecision(ctx, second, SourceDecision, now); err != nil || created {
		t.Fatalf("duplicate decision created=%v err=%v", created, err)
	}
}

func TestStoreBreachesOverdueCases(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	c, err := store.Create(ctx, CreateInput{
		Source: SourceDecision, DecisionClass: "HOLD", PrimaryDecision: "dec-sla",
		AccountID: "ACC-SLA", RiskScore: 60, AmountTotal: 1000,
		Actor: "case-worker", ActorRole: "service", Now: now.Add(-2 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	breached, err := store.BreachDue(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(breached) != 1 || breached[0].ID != c.ID {
		t.Fatalf("breached=%v", breached)
	}
	if breached[0].Status != StatusEscalated || !breached[0].Escalated {
		t.Fatalf("breach result = %+v", breached[0])
	}
	// Breaching twice is idempotent at the store level.
	again, err := store.BreachDue(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("second sweep escalated %d cases", len(again))
	}
}

func TestStoreIdempotencyRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store := newTestStore(t)
	ctx := context.Background()
	if _, found, err := store.LookupIdempotent(ctx, "key-1"); err != nil || found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if err := store.SaveIdempotent(ctx, "key-1", 1, "claim", []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	raw, found, err := store.LookupIdempotent(ctx, "key-1")
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if !jsonEqual(t, raw, `{"ok":true}`) {
		t.Fatalf("unexpected stored response %s", raw)
	}
	// A second save with the same key keeps the first response.
	if err := store.SaveIdempotent(ctx, "key-1", 1, "claim", []byte(`{"ok":false}`)); err != nil {
		t.Fatal(err)
	}
	raw, _, _ = store.LookupIdempotent(ctx, "key-1")
	if !jsonEqual(t, raw, `{"ok":true}`) {
		t.Fatalf("first writer must win, got %s", raw)
	}
}

func jsonEqual(t *testing.T, raw []byte, want string) bool {
	t.Helper()
	var a, b any
	if err := json.Unmarshal(raw, &a); err != nil {
		t.Fatalf("decode stored response %s: %v", raw, err)
	}
	if err := json.Unmarshal([]byte(want), &b); err != nil {
		t.Fatalf("decode want %s: %v", want, err)
	}
	return reflect.DeepEqual(a, b)
}

func countLabels(t *testing.T, ctx context.Context, store *Store, eventID string) int {
	t.Helper()
	var n int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM label_history WHERE event_id=$1
		   AND superseded_by IS NULL AND superseded_at IS NULL`, eventID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func eventTypes(events []CaseEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Type
	}
	return out
}
