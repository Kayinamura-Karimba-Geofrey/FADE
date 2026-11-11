package rules

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
)

type recordingPersister struct {
	mu        sync.Mutex
	active    *RuleSet
	versions  []Snapshot
	activeErr error
	saves     int
}

func (p *recordingPersister) SaveActivation(_ context.Context, _ *RuleSet, _ string) error {
	p.mu.Lock()
	p.saves++
	p.mu.Unlock()
	return nil
}
func (p *recordingPersister) Active(context.Context) (*RuleSet, error) {
	return p.active, p.activeErr
}
func (p *recordingPersister) Versions(context.Context) ([]Snapshot, error) {
	return p.versions, nil
}

func TestCompileAccessorsAndDirectMatch(t *testing.T) {
	rs := mustLoad(t, oneRule(`{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"amount","op":">","v":100},"reason":"amount {amount}"}`))
	ctx := baseCtx(vec(features.FeatureVector{}))
	if rules := rs.Rules(); len(rules) != 1 || rules[0].ID != "R-TST-001" || !rules[0].Enabled() {
		t.Fatalf("rules=%+v", rules)
	}
	if !rs.Rules()[0].Match(&ctx) {
		t.Fatal("amount 1000 must match > 100")
	}
	ctx.Amount = 10
	if rs.Rules()[0].Match(&ctx) {
		t.Fatal("amount 10 must not match")
	}
	if got := rs.RulesFor("P2P_SEND"); len(got) != 1 {
		t.Fatalf("txn rules=%d", len(got))
	}
	// Unknown transaction types share the untagged fallback slice.
	if got := rs.RulesFor("CASH_IN"); len(got) != 1 {
		t.Fatalf("fallback rules=%d", len(got))
	}
	snap := rs.Snapshot(true, "file")
	if snap.RuleSet != "test" || snap.Version != 1 || snap.Rules != 1 || snap.Source != "file" || !snap.Active {
		t.Fatalf("snapshot=%+v", snap)
	}
	file := rs.File()
	file.Rules[0].Name = "mutated"
	if rs.File().Rules[0].Name == "mutated" {
		t.Fatal("File must return a copy")
	}
}

func TestCombineAndEvalContextFor(t *testing.T) {
	rs := mustLoad(t, oneRule(`{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"ALLOW","when":{"f":"amount","op":">","v":0},"reason":"x"}`))
	res := &Result{RuleScore: 30, RuleAction: Allow}
	if score, decision := Combine(rs, res, MLResult{Status: MLNotConfigured}); score != 30 || decision != Allow {
		t.Fatalf("score=%d decision=%s", score, decision)
	}
	ml := 50
	if score, decision := Combine(rs, res, MLResult{Score: &ml, Status: MLOK}); score != 80 || decision != Hold {
		t.Fatalf("with ml score=%d decision=%s", score, decision)
	}
	negative := -100
	if score, _ := Combine(rs, res, MLResult{Score: &negative}); score != 0 {
		t.Fatalf("negative score=%d", score)
	}
	huge := 500
	if score, decision := Combine(rs, res, MLResult{Score: &huge}); score != 100 || decision != Block {
		t.Fatalf("clamped score=%d decision=%s", score, decision)
	}

	v := vec(features.FeatureVector{NewDevice: true})
	ctx := EvalContextFor(v, 100, 5, "P2P_SEND", "APP", 2, 900)
	if ctx.Vector != v || ctx.Amount != 100 || ctx.Fee != 5 || ctx.TxnType != "P2P_SEND" ||
		ctx.Channel != "APP" || ctx.KYCTier != 2 || ctx.BalanceBefore != 900 {
		t.Fatalf("ctx=%+v", ctx)
	}
	if ml, err := (NoopScorer{}).Score(context.Background(), v); err != nil || ml.Status != MLNotConfigured || ml.Score != nil {
		t.Fatalf("noop=%+v err=%v", ml, err)
	}
}

func TestInterpretRuleDisabledAndScope(t *testing.T) {
	disabled := false
	w := 10
	ctx := baseCtx(vec(features.FeatureVector{}))
	r := &Rule{ID: "R-TST-001", Enabled: &disabled, AppliesTo: []string{"CASH_IN"}, Weight: &w,
		When: Condition{F: "amount", Op: ">", V: []byte("100")}}
	if ok, err := InterpretRule(r, &ctx); ok || err != nil {
		t.Fatalf("disabled ok=%v err=%v", ok, err)
	}
	enabled := true
	r.Enabled = &enabled
	if ok, err := InterpretRule(r, &ctx); ok || err != nil {
		t.Fatalf("out-of-scope ok=%v err=%v", ok, err)
	}
	r.AppliesTo = nil
	if ok, err := InterpretRule(r, &ctx); !ok || err != nil {
		t.Fatalf("in-scope ok=%v err=%v", ok, err)
	}
	r.When = Condition{F: "not_a_field", Op: ">", V: []byte("1")}
	if _, err := InterpretRule(r, &ctx); err == nil {
		t.Fatal("unknown identifier must fail")
	}
}

func TestManagerAccessorsPersistenceAndRollback(t *testing.T) {
	rs := mustLoad(t, oneRule(`{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"amount","op":">","v":100},"reason":"x"}`))
	rs.Version = 1
	persist := &recordingPersister{active: rs, versions: []Snapshot{{RuleSet: "persist", Version: 9}}}
	hooks := Hooks{
		OnReload:   func(string, int) {},
		OnActivate: func(int) {},
		OnVersion:  func(int) {},
	}
	m := NewManager(t.TempDir(), slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})), persist, hooks)
	ctx := context.Background()
	if err := m.LoadStartup(ctx); err != nil {
		t.Fatal(err)
	}
	if m.Source() != "postgres" || m.Degraded() {
		t.Fatalf("source=%s degraded=%v", m.Source(), m.Degraded())
	}
	if m.Path() == "" || m.LoadedAt().IsZero() || m.LastError() != nil || m.Snapshot().Version != 1 {
		t.Fatalf("accessors path=%q loadedAt=%v err=%v", m.Path(), m.LoadedAt(), m.LastError())
	}
	versions := m.Versions(ctx)
	if versions[0].Version != 1 || len(versions) < 2 || versions[1].Version != 9 {
		t.Fatalf("versions=%+v", versions)
	}
	if _, err := m.RollbackNow(ctx, "admin"); err == nil {
		t.Fatal("rollback without previous must fail")
	}

	m.SetRollbackPolicy(time.Millisecond, 1, time.Millisecond)
	m.StartRollbackMonitor(ctx)
	m.RecordEval(nil)
	m.RecordEval(errors.New("boom"))

	// Install a second set via reload so a previous version exists.
	m2 := NewManager(t.TempDir(), slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})), persist, hooks)
	dir := m2.Path()
	writeRules(t, dir, 1, "enforce")
	if _, err := m2.Reload(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	writeRules(t, dir, 2, "enforce")
	if _, err := m2.Reload(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	got, err := m2.RollbackNow(ctx, "admin")
	if err != nil || got.Version != 1 || m2.Current().Version != 1 || m2.Source() != "file" {
		t.Fatalf("rollback=%+v err=%v current=%d", got, err, m2.Current().Version)
	}
	persist.mu.Lock()
	saves := persist.saves
	persist.mu.Unlock()
	if saves == 0 {
		t.Fatal("rollback must persist activation")
	}
}

func TestSmallHelpers(t *testing.T) {
	if KindInt.String() != "int" || KindFloat.String() != "float" || KindBool.String() != "bool" || KindString.String() != "string" || FieldKind(99).String() == "" {
		t.Fatal("field kind strings")
	}
	if MaxDecision(Allow, Block) != Block || MaxDecision(Block, Hold) != Block {
		t.Fatal("MaxDecision")
	}
	rs := mustLoad(t, oneRule(`{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"BLOCK","when":{"f":"amount","op":">","v":100},"reason":"amount {amount}"}`))
	var res Result
	rs.Evaluate(vec(features.FeatureVector{}), baseCtx(vec(features.FeatureVector{})), &res)
	if !res.HasBlock() || res.Fingerprint() == "" || res.FiredIDsSorted()[0] != "R-TST-001" {
		t.Fatalf("result fingerprint=%q block=%v", res.Fingerprint(), res.HasBlock())
	}
	res.Reset()
	if res.HasBlock() || res.FiredCount != 0 || res.Fingerprint() == "" {
		t.Fatal("reset")
	}
	if text := rs.Rules()[0].Reason.Lit(); text != "amount {amount}" {
		t.Fatalf("literal=%q", text)
	}
	var nilReason *ReasonTemplate
	if nilReason.Lit() != "" {
		t.Fatal("nil reason literal")
	}
	var called bool
	SetEvalHook(func(EvalContext) { called = true })
	rs.Evaluate(vec(features.FeatureVector{}), baseCtx(vec(features.FeatureVector{})), &res)
	SetEvalHook(nil)
	if !called {
		t.Fatal("eval hook not called")
	}
}

func TestManagerPersisterErrorFallsBackToFile(t *testing.T) {
	persist := &recordingPersister{activeErr: errors.New("postgres down")}
	m := NewManager(t.TempDir(), slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})), persist, Hooks{})
	writeRules(t, m.Path(), 1, "enforce")
	if err := m.LoadStartup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.Source() != "file" || m.Current().Version != 1 {
		t.Fatalf("source=%s version=%d", m.Source(), m.Current().Version)
	}
}
