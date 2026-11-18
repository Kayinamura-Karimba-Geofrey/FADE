package report

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/metrics"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

func fixture() Input {
	m := metrics.Result{Label: "baseline", Scored: 10, Fraud: 4, Legit: 6, Days: 1}
	m.Estimates = map[string]metrics.Estimate{
		"fraud_caught_rate":     {Point: 0.5, Lo: 0.25, Hi: 0.75},
		"fraud_intervened_rate": {Point: 0.25, Lo: 0, Hi: 0.5},
		"false_positive_rate":   {Point: 0.1667, Lo: 0, Hi: 0.33},
		"customer_friction":     {Point: 0.3, Lo: 0.2, Hi: 0.4},
	}
	m.Amounts = map[string]int64{"FRAUD:INTERVENED": 500, "FRAUD:MISSED": 1500}
	m.Workload = metrics.Workload{Interventions: 2, PerDay: 2, Per1000: 200}
	m.Scenarios = []metrics.ScenarioResult{{Scenario: "VELOCITY", Fraud: 4, RecallAlerted: 0.5, RecallIntervened: 0.25, CaughtAmount: 500}}
	m.Rules = []metrics.RuleAgg{{RuleID: "R-VEL-001", Fires: 3, TP: 2, FP: 1, Precision: 0.6667, UniqueCatch: 1}}
	m.Daily = []metrics.DayResult{{Date: "2026-01-01", Scored: 10, Fraud: 4, Recall: 0.5, FPR: 0.1667, Interventions: 2}}
	m.Segments = []metrics.SegmentResult{{Segment: "urban_youth", Scored: 10, Legit: 6, FPR: 0.1667, Friction: 0.3}}
	m.Sweep = []metrics.SweepPoint{{Threshold: 40, Recall: 0.5, Precision: 0.6667, FPR: 0.1667, Alerts: 3}}
	m.TimeToDetect = metrics.TimeToDetect{Runs: 1, Detected: 1, DetectedShare: 1, P50Minutes: 3, P90Minutes: 3}
	cand := m
	cand.Label = "cand"
	cand.Amounts = map[string]int64{"FRAUD:INTERVENED": 600, "FRAUD:MISSED": 1400}
	pair := metrics.NewPairAccumulator()
	pair.Add(metrics.Event{EventID: "f1", AccountID: "a", Label: "FRAUD"}, metrics.Outcome{Decision: rules.Allow}, metrics.Outcome{Decision: rules.StepUp})
	return Input{
		JobID: 7, Mode: "full", StatePolicy: "ignore", Seed: 11, EngineVersion: "0.7.0",
		Dataset:     DatasetInfo{Name: "golden", Hash: "deadbeef", Events: 12, Scored: 10, WarmupDays: 0},
		Configs:     []ConfigInfo{{Label: "baseline", Rules: "file:rules/prod.json", RulesHash: "rh", Model: "none", ConfigHash: "ch"}},
		Results:     map[string]metrics.Result{"baseline": m, "cand": cand},
		Pairs:       map[string]*metrics.PairAccumulator{"cand": pair},
		Transitions: map[string]map[[2]rules.Decision]int64{"cand": {{rules.Allow, rules.StepUp}: 1}},
		Cost:        CostModel{Currency: "RWF", AnalystCostPerCase: 1200, AverageFee: 250, RecoveryFactor: 0.35},
	}
}

func TestMarkdownGolden(t *testing.T) {
	rep := Build(fixture())
	applyCostForTest(rep)
	got := []byte(rep.Markdown())
	path := filepath.Join("testdata", "report.md")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("missing golden (run UPDATE_GOLDEN=1): %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("markdown differs; run UPDATE_GOLDEN=1 and review the diff")
	}
}

func TestJSONIsByteIdentical(t *testing.T) {
	a, err := Build(fixture()).JSON()
	if err != nil {
		t.Fatal(err)
	}
	b, err := Build(fixture()).JSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("report JSON is not deterministic")
	}
}

func applyCostForTest(r *Report) {
	r.CostModel.NetBenefitCandidates = map[string]float64{"cand": 100}
}
