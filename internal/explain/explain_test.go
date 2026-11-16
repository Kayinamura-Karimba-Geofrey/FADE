package explain

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

func fixture() *pipeline.Decision {
	ml := 74
	return &pipeline.Decision{
		DecisionID: "dec_01JA1Z6Q2M8N4P7R9T2V5X8B3C", EventID: "01JA1Z6Q2M8N4P7R9T2V5X8B3C",
		Decision: rules.Hold, RiskScore: 72, RuleScore: 60, MLScore: &ml, MLStatus: rules.MLOK,
		RuleSetVersion: 7,
		Reasons: []pipeline.Reason{
			{Source: "rule", ID: "R-VEL-001", Text: "12 transfers in 1 minute", Weight: 40, Mode: rules.Enforce},
			{Source: "rule", ID: "R-DEV-002", Text: "new device", Weight: 20, Mode: rules.Enforce},
			{Source: "model", ID: "velocity_1m", Feature: "velocity_1m", Value: 12, Contribution: 0.31},
			{Source: "tier", ID: "TIER_LIMIT_EXCEEDED", Text: "amount above the daily KYC tier limit"},
		},
		ShadowHits: []string{"R-SHD-001"},
		Features: &features.FeatureVector{
			EventID: "01JA1Z6Q2M8N4P7R9T2V5X8B3C", Velocity1m: 12, AmountSum10m: 70000,
			FeatureSchemaVersion: "1",
		},
		DecidedAt:  time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC),
		SnapshotID: "abc123",
		Provenance: pipeline.Provenance{
			RuleSetVersion: 7, RuleSetHash: "hash7", ModelVersion: "m1",
			CombinerConfig: map[string]any{"hold_threshold": 65, "block_threshold": 85, "step_up_threshold": 40},
			SnapshotID:     "abc123", ExplainSchemaVersion: 1,
		},
		ExplainSchemaVersion: 1,
	}
}

func TestBuildAudiences(t *testing.T) {
	b := NewBuilderWithClock(func() time.Time { return time.Unix(0, 0) })
	analyst, err := b.Build(fixture(), Options{Audience: AudienceAnalyst})
	if err != nil {
		t.Fatal(err)
	}
	if analyst.Summary == "" || len(analyst.Reasons) != 4 || len(analyst.Factors) == 0 {
		t.Fatalf("analyst view=%+v", analyst)
	}
	if analyst.Provenance == nil || analyst.Provenance.RuleSetHash != "hash7" || analyst.Provenance.SnapshotID != "abc123" {
		t.Fatalf("provenance=%+v", analyst.Provenance)
	}
	if len(analyst.Counterfactuals) != 1 || analyst.Counterfactuals[0].ResultingDecision != "STEP_UP" {
		t.Fatalf("counterfactuals=%+v", analyst.Counterfactuals)
	}
	if !analyst.GeneratedAt.Equal(time.Unix(0, 0).UTC()) {
		t.Fatalf("generated_at not from clock: %v", analyst.GeneratedAt)
	}

	customer, err := b.Build(fixture(), Options{Audience: AudienceCustomer, Language: "fr"})
	if err != nil {
		t.Fatal(err)
	}
	if customer.Language != "fr" || customer.Provenance != nil || len(customer.Factors) != 0 {
		t.Fatalf("customer view=%+v", customer)
	}
	if !strings.Contains(customer.Summary, "attente") {
		t.Fatalf("summary not localized: %q", customer.Summary)
	}
	for _, r := range customer.Reasons {
		if r.ID != "" || r.Weight != 0 {
			t.Fatalf("customer reason leaked internals: %+v", r)
		}
	}
	found := false
	for _, r := range customer.Reasons {
		if strings.Contains(r.Text, "limite") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected localized tier reason, got %+v", customer.Reasons)
	}

	regulator, err := b.Build(fixture(), Options{Audience: AudienceRegulator})
	if err != nil {
		t.Fatal(err)
	}
	if len(regulator.Features) == 0 || regulator.Provenance == nil {
		t.Fatalf("regulator view=%+v", regulator)
	}
}

func TestDecisionHashStable(t *testing.T) {
	a, err := DecisionHash(fixture())
	if err != nil {
		t.Fatal(err)
	}
	b, err := DecisionHash(fixture())
	if err != nil || a != b {
		t.Fatalf("hash not stable: %q %q err=%v", a, b, err)
	}
	changed := fixture()
	changed.Decision = rules.Block
	c, err := DecisionHash(changed)
	if err != nil || c == a {
		t.Fatalf("hash must change with decision: %q vs %q err=%v", c, a, err)
	}
}

func TestCounterfactualsPerDecision(t *testing.T) {
	cases := []struct {
		in    rules.Decision
		score int
		want  string
	}{
		{rules.Allow, 10, "STEP_UP"},
		{rules.StepUp, 45, "ALLOW"},
		{rules.Hold, 70, "STEP_UP"},
		{rules.Block, 95, "HOLD"},
	}
	for _, c := range cases {
		d := fixture()
		d.Decision, d.RiskScore = c.in, c.score
		got := Counterfactuals(d)
		if len(got) != 1 || got[0].ResultingDecision != c.want {
			t.Fatalf("%s: %+v", c.in, got)
		}
		if c.in != rules.Allow && got[0].Delta >= 0 {
			t.Fatalf("%s: delta must be negative: %+v", c.in, got[0])
		}
	}
}

func TestAudienceAndLanguageParsing(t *testing.T) {
	if a, err := ParseAudience(""); err != nil || a != AudienceAnalyst {
		t.Fatalf("default audience=%v err=%v", a, err)
	}
	if _, err := ParseAudience("public"); err == nil {
		t.Fatal("unknown audience must fail")
	}
	if NormalizeLanguage("fr-CA") != "fr" || NormalizeLanguage("zz") != "en" || NormalizeLanguage("") != "en" {
		t.Fatal("language normalization broken")
	}
}

func TestViewIsJSONSerialisable(t *testing.T) {
	v, err := NewBuilder().Build(fixture(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"explain_schema_version":1`) {
		t.Fatalf("schema version missing: %s", raw)
	}
}

// TestTemplateCoverage guarantees every production rule and every model feature
// has a sentence in every language, and that all languages take the same
// parameters. A missing sentence would silently fall back to English, which the
// spec forbids for the shipped rule set.
func TestTemplateCoverage(t *testing.T) {
	raw, err := os.ReadFile("../../rules/prod.json")
	if err != nil {
		t.Fatal(err)
	}
	var rs rules.RuleSetFile
	if err := json.Unmarshal(raw, &rs); err != nil {
		t.Fatal(err)
	}
	sets, err := LoadTemplateSets()
	if err != nil {
		t.Fatal(err)
	}
	for _, lang := range []string{"en", "fr", "rw"} {
		set, ok := sets[lang]
		if !ok {
			t.Fatalf("missing %s catalog", lang)
		}
		for _, r := range rs.Rules {
			if _, ok := set.Templates[r.ID]; !ok {
				t.Errorf("%s: rule %s has no template", lang, r.ID)
			}
		}
	}
	// Placeholder sets must agree across languages.
	for id, en := range sets["en"].Templates {
		want := placeholders(en)
		for _, lang := range []string{"fr", "rw"} {
			got := placeholders(sets[lang].Templates[id])
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("%s: %q placeholders %v != en %v", lang, id, got, want)
			}
		}
	}
	for lang, set := range sets {
		if lang != "en" && !set.NeedsReview {
			t.Errorf("%s must be marked needs_review", lang)
		}
	}
}

func placeholders(text string) []string {
	var out []string
	for i := 0; i < len(text); i++ {
		if text[i] != '{' {
			continue
		}
		end := strings.IndexByte(text[i:], '}')
		if end < 0 {
			break
		}
		out = append(out, text[i+1:i+end])
		i += end
	}
	sort.Strings(out)
	return out
}

func TestRuleReasonRendersFromStructuredParams(t *testing.T) {
	set := TemplateSetFor("fr")
	if set == nil {
		t.Fatal("no catalog")
	}
	got, ok := Render("R-VEL-001", "fr", map[string]string{
		"velocity_10m": "12", "amount_sum_10m": "70000",
	})
	if !ok {
		t.Fatal("render failed")
	}
	if !strings.Contains(got, "12") || !strings.Contains(got, "70000") || !strings.Contains(got, "envois") {
		t.Fatalf("unexpected sentence %q", got)
	}
	if _, ok := Render("R-VEL-001", "fr", map[string]string{"velocity_10m": "12"}); ok {
		t.Fatal("missing parameter must fail rather than emit a partial sentence")
	}
	if _, ok := Render("R-NOT-A-RULE", "en", nil); ok {
		t.Fatal("unknown id must fail")
	}
}

func TestKinyarwandaCatalogIsAvailableAndFlagged(t *testing.T) {
	v, err := NewBuilder().Build(fixture(), Options{Audience: AudienceCustomer, Language: "rw"})
	if err != nil {
		t.Fatal(err)
	}
	if v.Language != "rw" || !v.LanguageNeedsReview {
		t.Fatalf("rw view=%+v", v)
	}
	if v.Summary == "" {
		t.Fatal("empty rw summary")
	}
	if en, _ := NewBuilder().Build(fixture(), Options{}); en.LanguageNeedsReview {
		t.Fatal("English must not need review")
	}
}

// TestCustomerViewLeaksNothing asserts the customer-safe contract directly: no
// digits, rule ids, feature names, model words or thresholds.
func TestCustomerViewLeaksNothing(t *testing.T) {
	for _, d := range []rules.Decision{rules.Allow, rules.StepUp, rules.Hold, rules.Block} {
		dec := fixture()
		dec.Decision = d
		view, err := NewBuilder().Build(dec, Options{Audience: AudienceCustomer, Language: "en"})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(view)
		if err != nil {
			t.Fatal(err)
		}
		body := string(raw)
		for _, bad := range []string{"R-VEL", "velocity", "rule_score", "risk_score", "threshold", "model", "shap", "sim_"} {
			if strings.Contains(body, bad) {
				t.Fatalf("%s customer view leaked %q: %s", d, bad, body)
			}
		}
		for _, r := range view.Reasons {
			if strings.ContainsAny(r.Text, "0123456789") {
				t.Fatalf("customer sentence contains a number: %q", r.Text)
			}
		}
	}
}

func BenchmarkBuildAnalyst(b *testing.B) {
	builder := NewBuilderWithClock(func() time.Time { return time.Unix(0, 0) })
	dec := fixture()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := builder.Build(dec, Options{Audience: AudienceAnalyst}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuildCustomerKinyarwanda(b *testing.B) {
	builder := NewBuilderWithClock(func() time.Time { return time.Unix(0, 0) })
	dec := fixture()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := builder.Build(dec, Options{Audience: AudienceCustomer, Language: "rw"}); err != nil {
			b.Fatal(err)
		}
	}
}
