package rules

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func ruleDoc(version int, mode string) []byte {
	enabled := true
	w := 10
	f := RuleSetFile{
		RuleSet: "prod", Version: version, Author: "test", Config: Config{},
		Rules: []Rule{{
			ID: "R-TST-001", Name: "one", Enabled: &enabled, Mode: Mode(mode), Weight: &w,
			Action: StepUp, When: Condition{F: "amount", Op: ">", V: json.RawMessage("100")},
			Reason: "amount {amount}",
		}},
	}
	raw, _ := json.Marshal(f)
	return raw
}

func writeRules(t *testing.T, dir string, version int, mode string) string {
	t.Helper()
	path := filepath.Join(dir, "prod.json")
	if err := os.WriteFile(path, ruleDoc(version, mode), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func testManager(t *testing.T) (*Manager, string) {
	t.Helper()
	dir := t.TempDir()
	m := NewManager(dir, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})), nil, Hooks{})
	m.rollbackTick = 10 * time.Millisecond
	m.rollbackWindow = 50 * time.Millisecond
	m.rollbackMinSamples = 5
	return m, dir
}

func TestLoadStartupAndMonotonicReload(t *testing.T) {
	m, dir := testManager(t)
	if err := m.LoadStartup(context.Background()); err == nil || !m.Degraded() {
		t.Fatalf("empty dir should start degraded, err=%v degraded=%v", err, m.Degraded())
	}
	writeRules(t, dir, 1, "enforce")
	if _, err := m.Reload(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	if m.Current().Version != 1 || m.Degraded() {
		t.Fatalf("current=%d degraded=%v", m.Current().Version, m.Degraded())
	}
	writeRules(t, dir, 2, "enforce")
	if _, err := m.Reload(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	if m.Current().Version != 2 {
		t.Fatalf("version=%d", m.Current().Version)
	}
	// A lower version is rejected and the previous set stays active.
	writeRules(t, dir, 1, "shadow")
	if _, err := m.Reload(context.Background(), "test"); err == nil {
		t.Fatal("expected version regression rejection")
	}
	if m.Current().Version != 2 {
		t.Fatalf("previous set not preserved: %d", m.Current().Version)
	}
}

func TestInvalidRuleSetKeepsPreviousAndReportsAllErrors(t *testing.T) {
	m, dir := testManager(t)
	writeRules(t, dir, 1, "enforce")
	if _, err := m.Reload(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	bad := []byte(`{"rule_set":"prod","version":2,"author":"x","config":{},"rules":[
	  {"id":"nope","name":"a","enabled":true,"mode":"enforce","weight":500,"action":"NOPE","when":{"f":"missing","op":"??","v":1},"reason":"{missing}"}
	]}`)
	if err := os.WriteFile(filepath.Join(dir, "prod.json"), bad, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := m.Reload(context.Background(), "test")
	if err == nil {
		t.Fatal("expected rejection")
	}
	ce, ok := err.(*CompileError)
	if !ok || len(ce.Errors) < 6 {
		t.Fatalf("expected aggregated errors, got %T %v", err, err)
	}
	if m.Current().Version != 1 {
		t.Fatalf("active changed to %d", m.Current().Version)
	}
}

func TestPutRuleCreatesNewVersion(t *testing.T) {
	m, dir := testManager(t)
	writeRules(t, dir, 1, "enforce")
	if _, err := m.Reload(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	enabled := true
	weight := 25
	rule := Rule{
		ID: "R-TST-002", Name: "two", Enabled: &enabled, Mode: Enforce, Weight: &weight,
		Action: Hold, When: Condition{F: "velocity_10m", Op: ">", V: json.RawMessage("3")},
		Reason: "velocity {velocity_10m}",
	}
	rs, err := m.PutRule(context.Background(), "R-TST-002", rule, "admin")
	if err != nil {
		t.Fatal(err)
	}
	if rs.Version != 2 || len(rs.Rules()) != 2 {
		t.Fatalf("version=%d rules=%d", rs.Version, len(rs.Rules()))
	}
	// Replacing the same rule creates v3, not a duplicate.
	rule.Name = "two updated"
	rs, err = m.PutRule(context.Background(), "R-TST-002", rule, "admin")
	if err != nil {
		t.Fatal(err)
	}
	if rs.Version != 3 || len(rs.Rules()) != 2 {
		t.Fatalf("version=%d rules=%d", rs.Version, len(rs.Rules()))
	}
	if err := m.Activate(context.Background(), rs, "admin"); err != nil {
		t.Fatalf("idempotent activate: %v", err)
	}
}

func TestAutoRollbackOnEvaluationFailures(t *testing.T) {
	m, dir := testManager(t)
	writeRules(t, dir, 1, "enforce")
	if _, err := m.Reload(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	writeRules(t, dir, 2, "enforce")
	if _, err := m.Reload(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.StartRollbackMonitor(ctx)
	for i := 0; i < 8; i++ {
		m.RecordEval(fmt.Errorf("boom"))
	}
	m.RecordEval(nil)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.Current().Version == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("rollback did not happen, active=%d", m.Current().Version)
}

func TestFsnotifyDebouncedReload(t *testing.T) {
	m, dir := testManager(t)
	writeRules(t, dir, 1, "enforce")
	if _, err := m.Reload(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Watch(ctx); err != nil {
		t.Fatal(err)
	}
	writeRules(t, dir, 2, "enforce")
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if m.Current().Version == 2 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("watcher did not reload, active=%d", m.Current().Version)
}

func TestAuditChain(t *testing.T) {
	prev := ""
	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		row := NewAuditRow("admin", "rules.activate", "rule_set", fmt.Sprintf("prod/%d", i), nil, []byte(`{"a":1}`), time.Unix(int64(i), 0))
		h, err := ChainHash(prev, row)
		if err != nil {
			t.Fatal(err)
		}
		if seen[h] {
			t.Fatal("duplicate hash")
		}
		seen[h] = true
		prev = h
	}
	// The chain is sensitive to the previous hash.
	row := NewAuditRow("admin", "rules.activate", "rule_set", "prod/9", nil, nil, time.Unix(9, 0))
	a, _ := ChainHash("a", row)
	b, _ := ChainHash("b", row)
	if a == b {
		t.Fatal("prev hash not chained")
	}
}
