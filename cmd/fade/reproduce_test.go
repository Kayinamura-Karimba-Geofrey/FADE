package main

import (
	"os"
	"testing"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

func loadProd(t *testing.T) *rules.RuleSet {
	t.Helper()
	raw, err := os.ReadFile("../../rules/prod.json")
	if err != nil {
		t.Fatal(err)
	}
	rs, err := rules.Load(raw)
	if err != nil {
		t.Fatal(err)
	}
	return rs
}

// makeFixture builds a stored decision exactly as the pipeline would after
// evaluating the rule set over a feature vector, so reproduction has a truthful
// baseline.
func makeFixture(t *testing.T, rs *rules.RuleSet, i int) fixture {
	t.Helper()
	vector := features.FeatureVector{
		EventID:      "evt",
		Velocity1m:   int64(i % 7),
		Velocity10m:  int64(i%13 + 1),
		AmountSum10m: int64(1000 + i*37),
	}
	ctx := rules.EvalContextFor(&vector, int64(5000+i), 0, "P2P", "APP", int64(1+i%3), 100000)
	var res rules.Result
	rs.Evaluate(&vector, ctx, &res)
	risk, decision := rules.Combine(rs, &res, rules.MLResult{Status: rules.MLNotConfigured})
	d := pipeline.Decision{
		DecisionID: "dec", EventID: "evt", Decision: decision,
		RiskScore: risk, RuleScore: res.RuleScore, MLStatus: rules.MLNotConfigured,
		RuleSetVersion: rs.Version, Features: &vector,
	}
	return fixture{Decision: d, Amount: 5000 + int64(i), TxnType: "P2P", Channel: "APP",
		KYCTier: 1 + int64(i%3), BalanceBefore: 100000}
}

func TestReproduceTenThousandDecisions(t *testing.T) {
	if testing.Short() {
		t.Skip("10k reproductions")
	}
	rs := loadProd(t)
	const n = 10000
	for i := 0; i < n; i++ {
		res, err := reproduce(rs, makeFixture(t, rs, i))
		if err != nil {
			t.Fatalf("reproduce %d: %v", i, err)
		}
		if !res.OK {
			t.Fatalf("reproduce %d mismatched: %v", i, res.Mismatches)
		}
	}
}

func TestReproduceDetectsTamperedDecision(t *testing.T) {
	rs := loadProd(t)
	fx := makeFixture(t, rs, 3)
	res, err := reproduce(rs, fx)
	if err != nil || !res.OK {
		t.Fatalf("baseline: %v %v", err, res)
	}
	fx.Decision.RiskScore = (fx.Decision.RiskScore + 7) % 101
	tampered, err := reproduce(rs, fx)
	if err != nil {
		t.Fatal(err)
	}
	if tampered.OK {
		t.Fatal("tampered decision reproduced as OK")
	}
}

func TestReproduceRequiresFeatures(t *testing.T) {
	rs := loadProd(t)
	fx := makeFixture(t, rs, 1)
	fx.Decision.Features = nil
	if _, err := reproduce(rs, fx); err == nil {
		t.Fatal("missing feature snapshot accepted")
	}
}
