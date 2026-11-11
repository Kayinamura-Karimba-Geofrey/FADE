package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// ---------------------------------------------------------------- fixtures

type fakeFeatures struct {
	mu     sync.Mutex
	vec    features.FeatureVector
	status features.Status
	err    error
	calls  int
	block  chan struct{}
}

func (f *fakeFeatures) Compute(ctx context.Context, _ *events.Event) (features.FeatureVector, features.Status, error) {
	f.mu.Lock()
	f.calls++
	block := f.block
	f.mu.Unlock()
	if block != nil {
		<-block
	}
	if f.err != nil {
		return features.FeatureVector{}, "", f.err
	}
	return f.vec, f.status, nil
}

func (f *fakeFeatures) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type failingIdem struct{}

func (failingIdem) Begin(context.Context, string, string) ([]byte, bool, error) {
	return nil, false, errors.New("redis down")
}
func (failingIdem) Complete(context.Context, string, string, []byte, time.Duration) error {
	return errors.New("redis down")
}
func (failingIdem) Get(context.Context, string) ([]byte, bool, error) {
	return nil, false, errors.New("redis down")
}

type captureSink struct {
	mu    sync.Mutex
	items []*StoredDecision
}

func (c *captureSink) Enqueue(d *StoredDecision) {
	c.mu.Lock()
	c.items = append(c.items, d)
	c.mu.Unlock()
}

func (c *captureSink) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

type fakeStore struct {
	byEvent *StoredDecision
}

func (f *fakeStore) WriteDecisions(context.Context, []*StoredDecision) error { return nil }
func (f *fakeStore) GetDecision(context.Context, string) (*StoredDecision, error) {
	return nil, ErrNotFound
}
func (f *fakeStore) ListDecisions(context.Context, DecisionFilter) ([]*StoredDecision, string, error) {
	return nil, "", nil
}
func (f *fakeStore) DecisionByEventID(context.Context, string) (*StoredDecision, error) {
	if f.byEvent == nil {
		return nil, ErrNotFound
	}
	return f.byEvent, nil
}
func (f *fakeStore) RuleMetrics(context.Context, time.Duration) (*RuleMetricResult, error) {
	return nil, nil
}

func testEvent() *events.Event {
	return &events.Event{
		EventID: "01JA1Z6Q2M8N4P7R9T2V5X8B3C", EventTime: "2026-11-10T12:00:00.000Z",
		EventTimeMS: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC).UnixMilli(),
		TxnType:     events.P2PSend, Amount: 1000, Currency: "RWF", Fee: 100, Channel: events.ChannelApp,
		Sender: events.Sender{
			AccountID: "acct_000123", MSISDNHash: "h_0123456789abcdef0123456789abcdef",
			DeviceID: "dev_000123_1", SIMSerialHash: "sim_0123456789abcdef0123456789abcdef",
			AccountAgeDays: 120, KYCTier: 2, BalanceBefore: 10000,
		},
		Recipient: events.Recipient{AccountID: "acct_000456", MSISDNHash: "h_fedcba9876543210fedcba9876543210"},
		SessionID: "p2p-1234ABCD",
	}
}

const rulesTemplate = `{"rule_set":"test","version":%d,"author":"t","config":{"combiner":{"thresholds":{"step_up":40,"hold":65,"block":85}},"fail_mode":{"P2P_SEND":"%s"}},"rules":[
 {"id":"R-TST-001","name":"step","enabled":true,"mode":"enforce","weight":30,"action":"STEP_UP","when":{"f":"amount","op":">","v":100},"reason":"amount {amount}"},
 {"id":"R-TST-002","name":"block","enabled":true,"mode":"enforce","weight":50,"action":"BLOCK","when":{"f":"amount","op":">","v":100000},"reason":"block {amount}"}
]}`

func loadManager(t *testing.T, doc string, version int) *rules.Manager {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "prod.json")
	if err := os.WriteFile(path, []byte(fmt.Sprintf(doc, version, "step_up")), 0o644); err != nil {
		t.Fatal(err)
	}
	m := rules.NewManager(dir, slog.New(slog.NewTextHandler(os.Stderr,
		&slog.HandlerOptions{Level: slog.LevelError})), nil, rules.Hooks{})
	if _, err := m.Reload(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	return m
}

func newPipeline(t *testing.T, manager *rules.Manager, ff *fakeFeatures, lists *ListCache, idem Idempotency, cfg Config) (*Pipeline, *captureSink) {
	t.Helper()
	if lists == nil {
		lists = NewListCache(NewMemoryListSource(), nil)
	}
	sink := &captureSink{}
	return New(Deps{
		Rules: manager, Features: ff, Lists: lists, Idem: idem, Sink: sink,
		Config: cfg, Logger: slog.New(slog.NewTextHandler(os.Stderr,
			&slog.HandlerOptions{Level: slog.LevelError})),
	}), sink
}

func TestDecideEndToEnd(t *testing.T) {
	m := loadManager(t, rulesTemplate, 1)
	ff := &fakeFeatures{vec: features.FeatureVector{Velocity10m: 12}, status: features.StatusOK}
	p, sink := newPipeline(t, m, ff, nil, NewMemoryIdempotency(), DefaultConfig())
	dec, err := p.Decide(context.Background(), testEvent(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if dec.Decision != rules.StepUp {
		t.Fatalf("decision=%s risk=%d reasons=%+v", dec.Decision, dec.RiskScore, dec.Reasons)
	}
	if dec.RiskScore < 40 || dec.RuleScore != 30 {
		t.Fatalf("scores risk=%d rule=%d", dec.RiskScore, dec.RuleScore)
	}
	if dec.MLStatus != rules.MLNotConfigured || dec.MLScore != nil {
		t.Fatalf("ml=%v/%v", dec.MLStatus, dec.MLScore)
	}
	if len(dec.Reasons) != 1 || dec.Reasons[0].ID != "R-TST-001" {
		t.Fatalf("reasons=%+v", dec.Reasons)
	}
	if dec.Reasons[0].Text != "amount 1000" {
		t.Fatalf("reason text=%q", dec.Reasons[0].Text)
	}
	if dec.DecisionID[:4] != "dec_" || len(dec.DecisionID) != 30 {
		t.Fatalf("decision id=%q", dec.DecisionID)
	}
	if sink.Len() != 1 {
		t.Fatalf("sink=%d", sink.Len())
	}
	if dec.ShadowHits == nil || dec.Reasons == nil {
		t.Fatal("JSON arrays must not be nil")
	}
}

func TestIdempotentReplaySameDecision(t *testing.T) {
	m := loadManager(t, rulesTemplate, 1)
	ff := &fakeFeatures{vec: features.FeatureVector{}, status: features.StatusOK}
	p, sink := newPipeline(t, m, ff, nil, NewMemoryIdempotency(), DefaultConfig())
	ev := testEvent()
	first, err := p.Decide(context.Background(), ev, Options{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.Decide(context.Background(), ev, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if first.DecisionID != second.DecisionID || !second.Replay {
		t.Fatalf("first=%s second=%s replay=%v", first.DecisionID, second.DecisionID, second.Replay)
	}
	if ff.Calls() != 1 {
		t.Fatalf("feature calls=%d want 1 (duplicate must not update windows)", ff.Calls())
	}
	if sink.Len() != 1 {
		t.Fatalf("sink=%d want 1", sink.Len())
	}
}

func TestIdempotencyDifferentPayload(t *testing.T) {
	m := loadManager(t, rulesTemplate, 1)
	ff := &fakeFeatures{status: features.StatusOK}
	p, _ := newPipeline(t, m, ff, nil, NewMemoryIdempotency(), DefaultConfig())
	ev := testEvent()
	if _, err := p.Decide(context.Background(), ev, Options{IdempotencyKey: "k1"}); err != nil {
		t.Fatal(err)
	}
	other := testEvent()
	other.Amount = 777
	_, err := p.Decide(context.Background(), other, Options{IdempotencyKey: "k1"})
	var pe *Error
	if !errors.As(err, &pe) || pe.Status != 422 {
		t.Fatalf("err=%v want 422 conflict", err)
	}
}

func TestConcurrentDuplicateWaitsForOwner(t *testing.T) {
	idem := NewMemoryIdempotency()
	ctx := context.Background()
	hash := "h"
	if _, owner, err := idem.Begin(ctx, "k", hash); err != nil || !owner {
		t.Fatalf("owner=%v err=%v", owner, err)
	}
	done := make(chan []byte, 1)
	go func() {
		raw, owner, err := idem.Begin(ctx, "k", hash)
		if err != nil || owner {
			done <- nil
			return
		}
		done <- raw
	}()
	time.Sleep(10 * time.Millisecond)
	if err := idem.Complete(ctx, "k", hash, []byte(`{"decision_id":"dec_x"}`), time.Hour); err != nil {
		t.Fatal(err)
	}
	select {
	case raw := <-done:
		if string(raw) != `{"decision_id":"dec_x"}` {
			t.Fatalf("got %q", raw)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not observe completion")
	}
}

func TestFailPolicyMatrix(t *testing.T) {
	cases := []struct {
		mode string
		want rules.Decision
	}{{"open", rules.Allow}, {"closed", rules.Block}, {"step_up", rules.StepUp}}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			m := loadManager(t, rulesTemplate, 1)
			ff := &fakeFeatures{err: errors.New("redis exploded")}
			p, sink := newPipeline(t, m, ff, nil, NewMemoryIdempotency(), DefaultConfig())
			// Replace the active fail mode for this subtest.
			doc := fmt.Sprintf(`{"rule_set":"test","version":2,"author":"t","config":{"fail_mode":{"P2P_SEND":"%s"}},"rules":[
			 {"id":"R-TST-001","name":"step","enabled":true,"mode":"enforce","weight":30,"action":"STEP_UP","when":{"f":"amount","op":">","v":100},"reason":"a"}]}`, tc.mode)
			file := ruleDocFor(t, doc)
			rs, err := rules.Load(file)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.Activate(context.Background(), rs, "test"); err != nil {
				t.Fatal(err)
			}
			dec, err := p.Decide(context.Background(), testEvent(), Options{})
			if err != nil {
				t.Fatal(err)
			}
			if dec.Decision != tc.want || !dec.Degraded {
				t.Fatalf("decision=%s degraded=%v want %s", dec.Decision, dec.Degraded, tc.want)
			}
			if len(dec.Reasons) == 0 || dec.Reasons[0].Source != "fail_policy" {
				t.Fatalf("reasons=%+v", dec.Reasons)
			}
			if sink.Len() != 1 {
				t.Fatalf("degraded decisions must persist")
			}
		})
	}
}

func ruleDocFor(t *testing.T, doc string) []byte {
	t.Helper()
	if !json.Valid([]byte(doc)) {
		t.Fatalf("invalid test JSON: %s", doc)
	}
	return []byte(doc)
}

func TestRedisDownSkipsFeaturesAndAppliesLists(t *testing.T) {
	m := loadManager(t, rulesTemplate, 1)
	ff := &fakeFeatures{status: features.StatusOK}
	src := NewMemoryListSource()
	_, _ = src.AddList(context.Background(), ListEntry{
		ListType: ListBlock, Subject: SubjectAccount, Value: "acct_000123", Reason: "fraud",
	})
	lists := NewListCache(src, nil)
	_ = lists.Refresh(context.Background())
	p, _ := newPipeline(t, m, ff, lists, failingIdem{}, DefaultConfig())
	dec, err := p.Decide(context.Background(), testEvent(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if ff.Calls() != 0 {
		t.Fatalf("features were called with Redis down: %d", ff.Calls())
	}
	if !dec.Degraded || dec.Decision != rules.Block {
		t.Fatalf("decision=%s degraded=%v", dec.Decision, dec.Degraded)
	}
	if dec.Reasons[0].ID != "FAIL_POLICY_REDIS_UNAVAILABLE" {
		t.Fatalf("reasons=%+v", dec.Reasons)
	}
}

func TestPrecedenceMatrix(t *testing.T) {
	type tc struct {
		name       string
		amount     int64
		kyc        int
		blockAcct  bool
		allowAcct  bool
		want       rules.Decision
		wantReason string
	}
	cases := []tc{
		{"block list wins", 500, 2, true, false, rules.Block, "LIST_BLOCK"},
		{"allow list cannot beat rule BLOCK", 200000, 2, false, true, rules.Block, "R-TST-002"},
		{"allow list beats STEP_UP rule", 500, 2, false, true, rules.Allow, "LIST_ALLOW"},
		{"allow list beats threshold", 5000, 2, false, true, rules.Allow, "LIST_ALLOW"},
		{"rule BLOCK", 200000, 2, false, false, rules.Block, "R-TST-002"},
		{"rule STEP_UP", 500, 2, false, false, rules.StepUp, "R-TST-001"},
		{"tier limit raises to HOLD", 60000, 1, false, false, rules.Hold, "TIER_LIMIT_EXCEEDED"},
		{"thresholds only", 50, 2, false, false, rules.Allow, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := loadManager(t, rulesTemplate, 1)
			ff := &fakeFeatures{vec: features.FeatureVector{}, status: features.StatusOK}
			src := NewMemoryListSource()
			if c.blockAcct {
				_, _ = src.AddList(context.Background(), ListEntry{ListType: ListBlock, Subject: SubjectAccount, Value: "acct_000123"})
			}
			if c.allowAcct {
				_, _ = src.AddList(context.Background(), ListEntry{ListType: ListAllow, Subject: SubjectAccount, Value: "acct_000123"})
			}
			lists := NewListCache(src, nil)
			_ = lists.Refresh(context.Background())
			p, _ := newPipeline(t, m, ff, lists, NewMemoryIdempotency(), DefaultConfig())
			ev := testEvent()
			ev.Amount = c.amount
			ev.Fee = events.FeeForAmount(c.amount)
			ev.Sender.KYCTier = c.kyc
			dec, err := p.Decide(context.Background(), ev, Options{IncludeExplain: true})
			if err != nil {
				t.Fatal(err)
			}
			if dec.Decision != c.want {
				t.Fatalf("decision=%s want %s (risk=%d reasons=%+v)", dec.Decision, c.want, dec.RiskScore, dec.Reasons)
			}
			if c.wantReason != "" {
				found := false
				for _, r := range dec.Reasons {
					if r.ID == c.wantReason {
						found = true
					}
				}
				if !found {
					t.Fatalf("missing reason %s in %+v", c.wantReason, dec.Reasons)
				}
			}
		})
	}
}

func TestDuplicateFeatureStatusReturnsStoredDecision(t *testing.T) {
	m := loadManager(t, rulesTemplate, 1)
	ff := &fakeFeatures{status: features.StatusDuplicate}
	stored := &Decision{DecisionID: "dec_stored", EventID: testEvent().EventID, Decision: rules.Allow}
	store := &fakeStore{byEvent: &StoredDecision{Decision: stored}}
	p, _ := newPipeline(t, m, ff, nil, NewMemoryIdempotency(), DefaultConfig())
	p.store = store
	dec, err := p.Decide(context.Background(), testEvent(), Options{IdempotencyKey: "different-key"})
	if err != nil {
		t.Fatal(err)
	}
	if dec.DecisionID != "dec_stored" || !dec.Replay {
		t.Fatalf("dec=%+v", dec)
	}
}

func TestRulePanicDegradesAndRollsBack(t *testing.T) {
	m := loadManager(t, rulesTemplate, 1)
	// Version 2 so there is a previous set to roll back to.
	doc := fmt.Sprintf(rulesTemplate, 2, "step_up")
	if err := os.WriteFile(filepath.Join(m.Path(), "prod.json"), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Reload(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	m.SetRollbackPolicy(50*time.Millisecond, 5, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.StartRollbackMonitor(ctx)

	rules.SetEvalHook(func(rules.EvalContext) { panic("injected") })
	t.Cleanup(func() { rules.SetEvalHook(nil) })

	ff := &fakeFeatures{status: features.StatusOK}
	p, _ := newPipeline(t, m, ff, nil, NewMemoryIdempotency(), DefaultConfig())
	for i := 0; i < 6; i++ {
		ev := testEvent()
		ev.EventID = fmt.Sprintf("01JA1Z6Q2M8N4P7R9T2V5X8B%02d", i)
		dec, err := p.Decide(context.Background(), ev, Options{IdempotencyKey: fmt.Sprintf("k%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		if !dec.Degraded || dec.Reasons[0].ID != "FAIL_POLICY_RULE_PANIC" {
			t.Fatalf("dec=%+v", dec)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.Current().Version == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no rollback, active=%d", m.Current().Version)
}

func TestShedAtSemaphoreLimit(t *testing.T) {
	m := loadManager(t, rulesTemplate, 1)
	block := make(chan struct{})
	ff := &fakeFeatures{status: features.StatusOK, block: block}
	cfg := DefaultConfig()
	cfg.MaxConcurrency = 1
	p, _ := newPipeline(t, m, ff, nil, NewMemoryIdempotency(), cfg)

	started := make(chan struct{})
	go func() {
		close(started)
		_, _ = p.Decide(context.Background(), testEvent(), Options{IdempotencyKey: "a"})
	}()
	<-started
	time.Sleep(20 * time.Millisecond)
	_, err := p.Decide(context.Background(), testEvent(), Options{IdempotencyKey: "b"})
	close(block)
	var pe *Error
	if !errors.As(err, &pe) || pe.Kind != KindShed {
		t.Fatalf("err=%v want shed", err)
	}
}

func TestDecisionAllocationBudget(t *testing.T) {
	m := loadManager(t, rulesTemplate, 1)
	ff := &fakeFeatures{vec: features.FeatureVector{Velocity10m: 12}, status: features.StatusOK}
	p, _ := newPipeline(t, m, ff, nil, NewMemoryIdempotency(), DefaultConfig())
	i := 0
	allocs := testing.AllocsPerRun(200, func() {
		i++
		ev := testEvent()
		ev.EventID = fmt.Sprintf("01JA1Z6Q2M8N4P7R9T2V5X8B%02d", i%100)
		if _, err := p.Decide(context.Background(), ev, Options{IdempotencyKey: fmt.Sprintf("alloc-%d", i)}); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("allocations per decision: %.0f", allocs)
	if allocs >= 50 {
		t.Fatalf("allocations per request %.0f >= 50", allocs)
	}
}
