package rules

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
)

// build200 compiles a 200-rule set with mixed predicates, returning the set and
// a context that fires roughly half of it.
func build200(b *testing.B) (*RuleSet, *features.FeatureVector, EvalContext) {
	b.Helper()
	rng := rand.New(rand.NewSource(7))
	enabled := true
	rules := make([]Rule, 0, 200)
	for i := 0; i < 200; i++ {
		w := 1 + i%5
		op := ">"
		val := int64(rng.Intn(20))
		action := []Decision{Allow, StepUp, Hold, Block}[i%4]
		mode := Enforce
		if i%10 == 0 {
			mode = Shadow
		}
		rules = append(rules, Rule{
			ID:      fmt.Sprintf("R-BN-%03d", i+1),
			Name:    fmt.Sprintf("bench rule %d", i+1),
			Enabled: &enabled,
			Mode:    mode,
			Weight:  &w,
			Action:  action,
			When: Condition{All: []Condition{
				{F: "velocity_10m", Op: op, V: json.RawMessage(fmt.Sprintf("%d", val))},
				{F: "amount_sum_10m", Op: ">", V: json.RawMessage("1000")},
			}},
			Reason: "bench {velocity_10m} {amount_sum_10m}",
		})
	}
	doc := RuleSetFile{
		RuleSet: "bench", Version: 1, Author: "bench",
		Config: Config{}, Rules: rules,
	}
	rs, err := Compile(doc)
	if err != nil {
		b.Fatalf("compile: %v", err)
	}
	v := &features.FeatureVector{Velocity10m: 15, AmountSum10m: 500_000}
	ctx := EvalContext{Vector: v, Amount: 50_000, Fee: 100, TxnType: "P2P_SEND", Channel: "APP", KYCTier: 2, BalanceBefore: 100_000}
	return rs, v, ctx
}

func BenchmarkEvaluate200Rules(b *testing.B) {
	rs, v, ctx := build200(b)
	var res Result
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs.Evaluate(v, ctx, &res)
	}
}

func BenchmarkEvaluate200RulesParallel(b *testing.B) {
	rs, v, ctx := build200(b)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var res Result
		for pb.Next() {
			rs.Evaluate(v, ctx, &res)
		}
	})
}

func BenchmarkCompile200Rules(b *testing.B) {
	rs, _, _ := build200(b)
	doc := rs.File()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Compile(doc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEvaluateOneRule(b *testing.B) {
	enabled := true
	w := 10
	doc := RuleSetFile{
		RuleSet: "bn", Version: 1, Author: "b", Config: Config{},
		Rules: []Rule{{
			ID: "R-BN-001", Name: "one", Enabled: &enabled, Mode: Enforce, Weight: &w,
			Action: StepUp, When: Condition{F: "amount", Op: ">", V: json.RawMessage("100")}, Reason: "x",
		}},
	}
	rs, err := Compile(doc)
	if err != nil {
		b.Fatal(err)
	}
	v := &features.FeatureVector{}
	ctx := EvalContext{Vector: v, Amount: 1000, TxnType: "P2P_SEND"}
	var res Result
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rs.Evaluate(v, ctx, &res)
	}
}
