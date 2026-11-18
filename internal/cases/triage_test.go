package cases

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfigMatchesDocumentedPolicy(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
	if cfg.Priority.BlockBonus != 15 || cfg.Priority.RepeatAccountBonus != 10 {
		t.Fatalf("priority weights drifted: %+v", cfg.Priority)
	}
	if cfg.SLA.BlockMinutes != 30 || cfg.SLA.HoldMinutes != 15 || cfg.SLA.QASampleHours != 24 {
		t.Fatalf("sla drifted: %+v", cfg.SLA)
	}
	if cfg.LabelMature.Hours() != 168 {
		t.Fatalf("label maturity = %v, want 168h", cfg.LabelMature)
	}
	if !cfg.FourEyes.Enabled || cfg.FourEyes.AmountThreshold != 5_000_000 {
		t.Fatalf("four eyes drifted: %+v", cfg.FourEyes)
	}
}

func TestLoadConfigFromRepositoryFile(t *testing.T) {
	cfg, err := LoadConfig(filepath.Join("..", "..", "config", "cases.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.QASampleRate != 0.005 {
		t.Fatalf("qa_sample_rate=%v", cfg.QASampleRate)
	}
	if cfg.BreachPolicy != "escalate" {
		t.Fatalf("breach_policy=%q", cfg.BreachPolicy)
	}
	if cfg.Grouping.WindowMinutes != 60 {
		t.Fatalf("grouping window=%d", cfg.Grouping.WindowMinutes)
	}
}

func TestLoadConfigMissingFileUsesDefaults(t *testing.T) {
	cfg, err := LoadConfig(filepath.Join(t.TempDir(), "absent.yaml"))
	if err != nil {
		t.Fatalf("missing file must not error: %v", err)
	}
	if cfg.NoteMaxChars != 2000 {
		t.Fatalf("note_max_chars=%d", cfg.NoteMaxChars)
	}
}

func TestLoadConfigRejectsInvalidPolicy(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "cases.yaml")
	if err := os.WriteFile(bad, []byte("qa_sample_rate: 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(bad); err == nil {
		t.Fatal("qa_sample_rate above 1 must be rejected")
	}
}

func TestNeedsFourEyes(t *testing.T) {
	cfg := DefaultConfig()
	cases := []struct {
		class   string
		outcome Outcome
		amount  int64
		want    bool
	}{
		{"BLOCK", OutcomeLegit, 1000, true},        // overturning a block
		{"HOLD", OutcomeLegit, 1000, false},        // cheap hold release
		{"HOLD", OutcomeFraud, 5_000_001, true},    // above the amount threshold
		{"HOLD", OutcomeFraud, 5_000_000, false},   // at, not above, the threshold
		{"BLOCK", OutcomeInconclusive, 100, false}, // no overturn and cheap
	}
	for _, c := range cases {
		if got := cfg.NeedsFourEyes(c.class, c.outcome, c.amount); got != c.want {
			t.Errorf("%s/%s/%d: got %v want %v", c.class, c.outcome, c.amount, got, c.want)
		}
	}
}

func TestQASampleIsDeterministic(t *testing.T) {
	if QASampleSelected("", 0.5) {
		t.Fatal("empty decision id must not sample")
	}
	if QASampleSelected("dec-1", 0) {
		t.Fatal("rate 0 disables sampling")
	}
	if !QASampleSelected("dec-1", 1) {
		t.Fatal("rate 1 samples everything")
	}
	first := QASampleSelected("dec-abc", 0.005)
	for i := 0; i < 5; i++ {
		if QASampleSelected("dec-abc", 0.005) != first {
			t.Fatal("sampling must be deterministic")
		}
	}
	// At 0.5 the sample should be roughly balanced over many ids.
	var hits int
	for i := 0; i < 2000; i++ {
		if QASampleSelected("dec-"+string(rune('a'+i%26))+string(rune('0'+i/26%10))+string(rune('0'+i/260)), 0.5) {
			hits++
		}
	}
	if hits < 700 || hits > 1300 {
		t.Fatalf("sampling is biased: %d/2000", hits)
	}
}

func TestParseReasons(t *testing.T) {
	raw, _ := json.Marshal([]map[string]any{
		{"rule_id": "R001", "detail": "velocity"},
		{"code": "GEO_JUMP"},
		{"id": "legacy", "rule_id": "R002"},
	})
	got := ParseReasons(raw)
	want := []string{"R001", "GEO_JUMP", "R002"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
	if reasons := ParseReasons([]byte("not json")); reasons != nil {
		t.Fatalf("unparseable reasons must be ignored, got %v", reasons)
	}
	if reasons := ParseReasons(nil); reasons != nil {
		t.Fatalf("nil reasons must yield nil, got %v", reasons)
	}
}

func TestPseudonymIsStableAndNotReversible(t *testing.T) {
	p := NewPseudonymizer("unit-test-key")
	a := p.ID("acct", "ACC-123")
	b := p.ID("acct", "ACC-123")
	if a != b {
		t.Fatal("pseudonym must be stable for the same input")
	}
	if a == p.ID("acct", "ACC-124") {
		t.Fatal("distinct accounts must not collide")
	}
	if a == p.ID("dec", "ACC-123") {
		t.Fatal("prefix must separate namespaces")
	}
	if len(a) != len("acct_")+10 {
		t.Fatalf("unexpected pseudonym shape %q", a)
	}
	if a == "ACC-123" || p.ID("acct", "") != "" {
		t.Fatal("raw identifiers must never be returned")
	}
	other := NewPseudonymizer("another-key")
	if other.ID("acct", "ACC-123") == a {
		t.Fatal("a different deployment key must produce a different pseudonym")
	}
}
