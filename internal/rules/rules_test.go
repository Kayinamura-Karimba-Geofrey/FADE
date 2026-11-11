package rules

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
)

func vec(v features.FeatureVector) *features.FeatureVector { return &v }

func baseCtx(v *features.FeatureVector) EvalContext {
	return EvalContext{
		Vector: v, Amount: 1000, Fee: 100, TxnType: "P2P_SEND",
		Channel: "APP", KYCTier: 2, BalanceBefore: 10000,
	}
}

func mustLoad(t *testing.T, doc string) *RuleSet {
	t.Helper()
	rs, err := Load([]byte(doc))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return rs
}

const testHeader = `{"rule_set":"test","version":1,"author":"t","config":{"combiner":{"thresholds":{"step_up":40,"hold":65,"block":85}}},"rules":[`

func oneRule(body string) string { return testHeader + body + "]}" }

func TestOperatorsByType(t *testing.T) {
	tests := []struct {
		name string
		rule string
		v    features.FeatureVector
		want bool
	}{
		{"int eq", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"velocity_10m","op":"==","v":5},"reason":"v {velocity_10m}"}`, features.FeatureVector{Velocity10m: 5}, true},
		{"int ne", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"velocity_10m","op":"!=","v":5},"reason":"x"}`, features.FeatureVector{Velocity10m: 5}, false},
		{"int gt", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"velocity_10m","op":">","v":4},"reason":"x"}`, features.FeatureVector{Velocity10m: 5}, true},
		{"int gte", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"velocity_10m","op":">=","v":5},"reason":"x"}`, features.FeatureVector{Velocity10m: 5}, true},
		{"int lt", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"velocity_10m","op":"<","v":6},"reason":"x"}`, features.FeatureVector{Velocity10m: 5}, true},
		{"int lte", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"velocity_10m","op":"<=","v":5},"reason":"x"}`, features.FeatureVector{Velocity10m: 5}, true},
		{"int in", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"velocity_10m","op":"in","v":[1,5,9]},"reason":"x"}`, features.FeatureVector{Velocity10m: 5}, true},
		{"int not_in", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"velocity_10m","op":"not_in","v":[1,9]},"reason":"x"}`, features.FeatureVector{Velocity10m: 5}, true},
		{"float gt", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"near_limit_ratio","op":">","v":0.9},"reason":"x"}`, features.FeatureVector{NearLimitRatio: 0.95}, true},
		{"float eq", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"near_limit_ratio","op":"==","v":0.5},"reason":"x"}`, features.FeatureVector{NearLimitRatio: 0.5}, true},
		{"float in", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"near_limit_ratio","op":"in","v":[0.1,0.95]},"reason":"x"}`, features.FeatureVector{NearLimitRatio: 0.95}, true},
		{"float not_in", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"near_limit_ratio","op":"not_in","v":[0.1,0.2]},"reason":"x"}`, features.FeatureVector{NearLimitRatio: 0.95}, true},
		{"bool eq true", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"new_device","op":"==","v":true},"reason":"x"}`, features.FeatureVector{NewDevice: true}, true},
		{"bool ne", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"new_device","op":"!=","v":false},"reason":"x"}`, features.FeatureVector{NewDevice: true}, true},
		{"bool in", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"new_device","op":"in","v":[true]},"reason":"x"}`, features.FeatureVector{NewDevice: true}, true},
		{"string eq", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"txn_type","op":"==","v":"P2P_SEND"},"reason":"x"}`, features.FeatureVector{}, true},
		{"string ne", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"channel","op":"!=","v":"USSD"},"reason":"x"}`, features.FeatureVector{}, true},
		{"string in", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"channel","op":"in","v":["APP","API"]},"reason":"x"}`, features.FeatureVector{}, true},
		{"string not_in", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"channel","op":"not_in","v":["USSD"]},"reason":"x"}`, features.FeatureVector{}, true},
		{"raw amount", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"amount","op":">","v":999},"reason":"x"}`, features.FeatureVector{}, true},
		{"balance ratio", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"balance_before_ratio","op":">","v":0.09},"reason":"x"}`, features.FeatureVector{}, true},
		{"all short circuit", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"all":[{"f":"velocity_10m","op":"==","v":99},{"f":"amount","op":">","v":1}]},"reason":"x"}`, features.FeatureVector{Velocity10m: 1}, false},
		{"any", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"any":[{"f":"velocity_10m","op":"==","v":1},{"f":"amount","op":">","v":99999}]},"reason":"x"}`, features.FeatureVector{Velocity10m: 1}, true},
		{"not", `{"id":"R-TST-001","name":"n1","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"not":{"f":"new_device","op":"==","v":true}},"reason":"x"}`, features.FeatureVector{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := mustLoad(t, oneRule(tt.rule))
			ctx := baseCtx(vec(tt.v))
			res := rs.EvaluateNew(ctx.Vector, ctx)
			if got := res.FiredCount == 1; got != tt.want {
				t.Fatalf("fired=%v want %v (result %+v)", got, tt.want, res)
			}
		})
	}
}

func TestScoringModesAndAppliesTo(t *testing.T) {
	doc := testHeader + `
{"id":"R-TST-001","name":"a","enabled":true,"mode":"enforce","weight":30,"action":"STEP_UP","when":{"f":"amount","op":">","v":1},"reason":"a","applies_to":["P2P_SEND"]},
{"id":"R-TST-002","name":"b","enabled":true,"mode":"enforce","weight":80,"action":"BLOCK","when":{"f":"amount","op":">","v":1},"reason":"b"},
{"id":"R-TST-003","name":"c","enabled":true,"mode":"shadow","weight":100,"action":"BLOCK","when":{"f":"amount","op":">","v":1},"reason":"c"},
{"id":"R-TST-004","name":"d","enabled":false,"mode":"enforce","weight":100,"action":"BLOCK","when":{"f":"amount","op":">","v":1},"reason":"d"}
]}`
	rs := mustLoad(t, doc)
	ctx := baseCtx(vec(features.FeatureVector{}))
	res := rs.EvaluateNew(ctx.Vector, ctx)
	if res.RuleScore != 100 {
		t.Fatalf("score=%d want 100", res.RuleScore)
	}
	if res.RuleAction != Block {
		t.Fatalf("action=%s want BLOCK", res.RuleAction)
	}
	if res.FiredCount != 2 {
		t.Fatalf("fired=%d want 2", res.FiredCount)
	}
	if res.ShadowCount != 1 || res.Shadow[0] != "R-TST-003" {
		t.Fatalf("shadow=%v", res.Shadow[:res.ShadowCount])
	}
	score, decision := Combine(rs, &res, NoopML())
	if score != 100 || decision != Block {
		t.Fatalf("combine=%d/%s", score, decision)
	}

	cash := baseCtx(vec(features.FeatureVector{}))
	cash.TxnType = "CASH_OUT"
	cashRes := rs.EvaluateNew(cash.Vector, cash)
	if cashRes.RuleScore != 80 {
		t.Fatalf("cash score=%d want 80", cashRes.RuleScore)
	}
}

func NoopML() MLResult { return MLResult{Status: MLNotConfigured} }

func TestValidationErrorsAllReported(t *testing.T) {
	doc := `{
	  "rule_set":"BAD NAME","version":0,"author":"","config":{"combiner":{"thresholds":{"step_up":90,"hold":40,"block":200},"floors":{"STEP_UP":90,"HOLD":10}},"fail_mode":{"NOPE":"maybe"}},
	  "rules":[
	    {"id":"bad","name":"a","enabled":true,"mode":"enforce","weight":101,"action":"MAYBE","when":{"f":"nope","op":"~","v":1},"reason":"{nope}","applies_to":["NOPE"]},
	    {"id":"bad","name":"a","enabled":true,"mode":"enforce","weight":1,"action":"ALLOW","when":{"all":[]},"reason":"x"},
	    {"id":"R-AA-001","name":"b","enabled":true,"mode":"enforce","weight":1,"action":"ALLOW","when":{"f":"amount","op":">","v":"x"},"reason":"x"},
	    {"id":"R-AA-002","name":"c","enabled":true,"mode":"nope","weight":1,"action":"ALLOW","when":{"all":[{"all":[{"all":[{"all":[{"all":[{"all":[{"f":"amount","op":"==","v":1}]}]}]}]}]}]},"reason":"{{}"}
	  ]
	}`
	_, err := Load([]byte(doc))
	var ce *CompileError
	if !errors.As(err, &ce) {
		t.Fatalf("want CompileError, got %T %v", err, err)
	}
	joined := ce.Error()
	for _, want := range []string{
		"rule_set", "version", "author", "thresholds", "floors", "fail_mode", "id", "duplicate rule id",
		"name", "weight", "action", "unknown identifier", "unknown operator", "applies_to",
		"condition needs", "expected integer", "mode", "nesting deeper", "unmatched brace",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing error %q in:\n%s", want, joined)
		}
	}
	if len(ce.Errors) < 15 {
		t.Errorf("only %d errors reported", len(ce.Errors))
	}
}

func TestValidationSpecific(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want string
	}{
		{"empty rules", strings.Replace(testHeader, `"rules":[`, `"rules":[`, 1) + "]}", "at least one rule"},
		{"duplicate name", oneRule(`{"id":"R-AA-001","name":"x","enabled":true,"mode":"enforce","weight":1,"action":"ALLOW","when":{"f":"amount","op":">","v":1},"reason":"a"},{"id":"R-AA-002","name":"x","enabled":true,"mode":"enforce","weight":1,"action":"ALLOW","when":{"f":"amount","op":">","v":1},"reason":"b"}`), "duplicate rule name"},
		{"unknown txn", oneRule(`{"id":"R-AA-001","name":"x","enabled":true,"mode":"enforce","weight":1,"action":"ALLOW","when":{"f":"amount","op":">","v":1},"reason":"a","applies_to":["PAYMENT"]}`), "unknown txn_type"},
		{"stateless feature", oneRule(`{"id":"R-AA-001","name":"x","enabled":true,"mode":"enforce","weight":1,"action":"ALLOW","stateless":true,"when":{"f":"velocity_10m","op":">","v":1},"reason":"a"}`), "stateless rules"},
		{"bool gt", oneRule(`{"id":"R-AA-001","name":"x","enabled":true,"mode":"enforce","weight":1,"action":"ALLOW","when":{"f":"new_device","op":">","v":true},"reason":"a"}`), "bool identifiers"},
		{"empty in", oneRule(`{"id":"R-AA-001","name":"x","enabled":true,"mode":"enforce","weight":1,"action":"ALLOW","when":{"f":"amount","op":"in","v":[]},"reason":"a"}`), "must not be empty"},
		{"weight missing", oneRule(`{"id":"R-AA-001","name":"x","enabled":true,"mode":"enforce","action":"ALLOW","when":{"f":"amount","op":">","v":1},"reason":"a"}`), "weight: is required"},
		{"enabled missing", oneRule(`{"id":"R-AA-001","name":"x","mode":"enforce","weight":1,"action":"ALLOW","when":{"f":"amount","op":">","v":1},"reason":"a"}`), "enabled: is required"},
		{"trailing json", oneRule(`{"id":"R-AA-001","name":"x","enabled":true,"mode":"enforce","weight":1,"action":"ALLOW","when":{"f":"amount","op":">","v":1},"reason":"a"}`) + "{}", "trailing JSON"},
		{"unknown field", strings.Replace(oneRule(`{"id":"R-AA-001","name":"x","enabled":true,"mode":"enforce","weight":1,"action":"ALLOW","when":{"f":"amount","op":">","v":1},"reason":"a"}`), `"reason":"a"`, `"reason":"a","surprise":1`, 1), "unknown field"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load([]byte(tc.doc))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want contains %q", err, tc.want)
			}
		})
	}
}

func TestCombinerThresholdsAndFloors(t *testing.T) {
	c := Combiner{StepUpThreshold: 40, HoldThreshold: 65, BlockThreshold: 85, StepUpFloor: 50, HoldFloor: 70, BlockFloor: 90}
	cases := []struct {
		score     int
		action    Decision
		wantScore int
		want      Decision
	}{
		{0, Allow, 0, Allow},
		{39, Allow, 39, Allow},
		{40, Allow, 40, StepUp},
		{64, Allow, 64, StepUp},
		{65, Allow, 65, Hold},
		{84, Allow, 84, Hold},
		{85, Allow, 85, Block},
		{0, Hold, 70, Hold},
		{90, StepUp, 90, Block},
	}
	for _, tc := range cases {
		gotScore, got := c.Apply(tc.score, tc.action)
		if gotScore != tc.wantScore || got != tc.want {
			t.Errorf("Apply(%d,%s)=(%d,%s) want (%d,%s)", tc.score, tc.action, gotScore, got, tc.wantScore, tc.want)
		}
	}
}

func TestReasonRenderingGolden(t *testing.T) {
	rs := mustLoad(t, oneRule(`{"id":"R-TST-001","name":"n","enabled":true,"mode":"enforce","weight":60,"action":"HOLD","when":{"all":[{"f":"velocity_10m","op":">","v":1},{"f":"amount_sum_10m","op":">","v":1}]},"reason":"{velocity_10m} sends in 10 min totalling {amount_sum_10m} RWF on {txn_type}/{channel} new={new_device} ratio={near_limit_ratio}"}`))
	v := features.FeatureVector{Velocity10m: 12, AmountSum10m: 750000, NewDevice: true, NearLimitRatio: 1.4, AmountVsAvg30d: 2.5}
	ctx := baseCtx(vec(v))
	res := rs.EvaluateNew(ctx.Vector, ctx)
	got := res.Fired[0].Rule.Reason.Render(&ctx)
	goldenPath := filepath.Join("testdata", "reasons.golden")
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if strings.TrimSpace(string(want)) != got {
		t.Fatalf("reason mismatch:\n got: %q\nwant: %q", got, strings.TrimSpace(string(want)))
	}
}

func TestEvaluateSafeRecoversPanic(t *testing.T) {
	rs := mustLoad(t, oneRule(`{"id":"R-TST-001","name":"n","enabled":true,"mode":"enforce","weight":10,"action":"STEP_UP","when":{"f":"amount","op":">","v":0},"reason":"x"}`))
	rs.rules[0].match = func(*EvalContext) bool { panic("boom") }
	byTxn := rs.byTxn["P2P_SEND"]
	for i := range byTxn {
		if byTxn[i].ID == "R-TST-001" {
			byTxn[i].match = rs.rules[0].match
		}
	}
	ctx := baseCtx(vec(features.FeatureVector{}))
	var res Result
	if err := rs.EvaluateSafe(ctx.Vector, ctx, &res); !errors.Is(err, ErrRulePanic) || !res.Failed {
		t.Fatalf("err=%v failed=%v", err, res.Failed)
	}
}

func TestCanonicalHashStable(t *testing.T) {
	a := []byte(`{"b":1,"a":{"z":true,"y":[1,2]},"n":2.0}`)
	b := []byte(`{"n":2,"a":{"y":[1,2],"z":true},"b":1}`)
	ha, err := HashContent(a)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := HashContent(b)
	if err != nil {
		t.Fatal(err)
	}
	if ha != hb {
		t.Fatalf("hash differs: %s %s", ha, hb)
	}
}

// TestReferenceEquivalence100k compares the compiled evaluator to the naive
// interpreter on 100,000 random rule-tree/feature-vector pairs.
func TestReferenceEquivalence100k(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	checked := 0
	for i := 0; i < 2000; i++ {
		doc := randomFile(rng)
		raw, _ := json.Marshal(doc)
		rs, err := Load(raw)
		if err != nil {
			t.Fatalf("generated rule set invalid: %v\n%s", err, raw)
		}
		for j := 0; j < 50; j++ {
			v, ctx := randomContext(rng)
			ctx.Vector = v
			var got Result
			rs.Evaluate(v, ctx, &got)
			want, err := InterpretFile(&doc, &ctx)
			if err != nil {
				t.Fatalf("interpret: %v", err)
			}
			if got.RuleScore != want.RuleScore || got.RuleAction != want.RuleAction {
				t.Fatalf("mismatch score/action got %d/%s want %d/%s\nrule set: %s",
					got.RuleScore, got.RuleAction, want.RuleScore, want.RuleAction, raw)
			}
			if strings.Join(got.FiredIDsSorted(), ",") != strings.Join(want.FiredIDsSorted(), ",") {
				t.Fatalf("mismatch fired got %v want %v", got.FiredIDsSorted(), want.FiredIDsSorted())
			}
			gotShadow := append([]string(nil), got.Shadow[:got.ShadowCount]...)
			wantShadow := append([]string(nil), want.Shadow[:want.ShadowCount]...)
			sort.Strings(gotShadow)
			sort.Strings(wantShadow)
			if strings.Join(gotShadow, ",") != strings.Join(wantShadow, ",") {
				t.Fatalf("mismatch shadow got %v want %v", gotShadow, wantShadow)
			}
			checked++
		}
	}
	if checked < 100000 {
		t.Fatalf("only %d cases", checked)
	}
}

func randomContext(rng *rand.Rand) (*features.FeatureVector, EvalContext) {
	v := &features.FeatureVector{
		Velocity1m: rng.Int63n(20), Velocity10m: rng.Int63n(30), Velocity1h: rng.Int63n(60),
		AmountSum10m: rng.Int63n(2_000_000), AmountSum24h: rng.Int63n(10_000_000),
		DistinctRecip24h: rng.Int63n(20), AmountVsAvg30d: math.Round(rng.Float64()*1000) / 100,
		AmountZScore30d: math.Round(rng.Float64()*1000) / 100, NearLimitRatio: math.Round(rng.Float64()*1000) / 100,
		SmallTxnRun1h: rng.Int63n(20), NewDevice: rng.Intn(2) == 0, NewGeoKm: math.Round(rng.Float64()*10000) / 100,
		SIMChanged72h: rng.Intn(2) == 0, TimeSinceSIMChange: rng.Int63n(10000), AccPerDevice7d: rng.Int63n(10),
		RecipNewForSender: rng.Intn(2) == 0, RecipDistinctSenders24h: rng.Int63n(30),
		RecipCashoutLagMin: rng.Int63n(200), InboundOutboundRatio1h: math.Round(rng.Float64()*1000) / 100,
		NightHour: rng.Intn(2) == 0, CycleLen2To4: rng.Int63n(5), AgentCicoRatio24h: math.Round(rng.Float64()*1000) / 100,
		AgentFloatDeltaPct: math.Round(rng.Float64()*20000) / 100, AccountAgeDays: rng.Int63n(2000), KYCTier: int64(1 + rng.Intn(3)),
	}
	ctx := EvalContext{
		Vector: v, Amount: rng.Int63n(1_000_000), Fee: rng.Int63n(1000),
		TxnType: TxnTypes[rng.Intn(len(TxnTypes))], Channel: []string{"USSD", "APP", "AGENT", "API"}[rng.Intn(4)],
		KYCTier: int64(1 + rng.Intn(3)), BalanceBefore: rng.Int63n(2_000_000),
	}
	return v, ctx
}

func randomFile(rng *rand.Rand) RuleSetFile {
	enabled := true
	n := 1 + rng.Intn(5)
	rules := make([]Rule, 0, n)
	for i := 0; i < n; i++ {
		w := rng.Intn(101)
		rules = append(rules, Rule{
			ID:      fmt.Sprintf("R-AA-%03d", i+1),
			Name:    fmt.Sprintf("rule %d", i+1),
			Enabled: &enabled,
			Mode:    []Mode{Enforce, Shadow}[rng.Intn(2)],
			Weight:  &w,
			Action:  []Decision{Allow, StepUp, Hold, Block}[rng.Intn(4)],
			When:    randomCondition(rng, 1),
			Reason:  "hit {amount}",
			Tags:    []string{"prop"},
		})
	}
	return RuleSetFile{
		RuleSet: "prop", Version: 1, Author: "rapid",
		Config: Config{}, Rules: rules,
	}
}

func randomCondition(rng *rand.Rand, depth int) Condition {
	if depth < 4 && rng.Intn(3) == 0 {
		switch rng.Intn(3) {
		case 0:
			kids := make([]Condition, 1+rng.Intn(3))
			for i := range kids {
				kids[i] = randomCondition(rng, depth+1)
			}
			return Condition{All: kids}
		case 1:
			kids := make([]Condition, 1+rng.Intn(3))
			for i := range kids {
				kids[i] = randomCondition(rng, depth+1)
			}
			return Condition{Any: kids}
		default:
			child := randomCondition(rng, depth+1)
			return Condition{Not: &child}
		}
	}
	fields := FieldNames()
	name := fields[rng.Intn(len(fields))]
	f := Fields[name]
	var value any
	switch f.Kind {
	case KindInt:
		value = rng.Int63n(100)
	case KindFloat:
		value = math.Round(rng.Float64()*1000) / 100
	case KindBool:
		value = rng.Intn(2) == 0
	case KindString:
		value = []string{"P2P_SEND", "CASH_OUT", "AIRTIME", "APP", "USSD"}[rng.Intn(5)]
	}
	op := "=="
	switch f.Kind {
	case KindInt, KindFloat:
		op = []string{"==", "!=", ">", ">=", "<", "<="}[rng.Intn(6)]
	case KindBool, KindString:
		op = []string{"==", "!="}[rng.Intn(2)]
	}
	if rng.Intn(6) == 0 {
		op = []string{"in", "not_in"}[rng.Intn(2)]
		var list []any
		if f.Kind == KindString {
			list = []any{"P2P_SEND", "APP"}
		} else if f.Kind == KindBool {
			list = []any{true, false}
		} else if f.Kind == KindFloat {
			list = []any{1.0, 2.5}
		} else {
			list = []any{1, 2, 3}
		}
		value = list
	}
	raw, _ := json.Marshal(value)
	return Condition{F: name, Op: op, V: raw}
}
