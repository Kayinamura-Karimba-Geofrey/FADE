package gate

import (
	"testing"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/metrics"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/report"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

func result(label string, recall, fpr, friction, load float64, saved int64) metrics.Result {
	m := metrics.Result{Label: label, Scored: 1000, Fraud: 100, Legit: 900, Days: 10}
	m.Estimates = map[string]metrics.Estimate{
		"fraud_caught_rate":       {Point: recall, Lo: recall - 0.02, Hi: recall + 0.02},
		"fraud_intervened_rate":   {Point: recall / 2, Lo: 0, Hi: 1},
		"false_positive_rate":     {Point: fpr, Lo: fpr, Hi: fpr},
		"legit_intervention_rate": {Point: fpr / 2},
		"customer_friction":       {Point: friction},
		"precision_alerted":       {Point: 0.5},
		"precision_intervened":    {Point: 0.7},
		"f2_alerted":              {Point: 0.5},
	}
	m.Amounts = map[string]int64{"FRAUD:INTERVENED": saved, "FRAUD:MISSED": 1000}
	m.Decisions = map[string]int64{"STEP_UP": 100, "HOLD": 50, "BLOCK": 5}
	m.Workload = metrics.Workload{Interventions: 55, PerDay: load}
	m.Scenarios = []metrics.ScenarioResult{{Scenario: "VELOCITY", Fraud: 40, RecallAlerted: recall, RecallIntervened: recall / 2}}
	m.RiskHistogram[0][50] = 100
	m.RiskHistogram[1][20] = 900
	return m
}

func baseReport() *report.Report {
	rep := &report.Report{ReplayReportVersion: "1", JobID: 1}
	rep.Configs = []report.ConfigInfo{{Label: "baseline", ConfigHash: "b", RulesHash: "rb"},
		{Label: "cand", ConfigHash: "c", RulesHash: "rc"}}
	base := result("baseline", 0.8, 0.01, 0.30, 5, 100000)
	cand := result("cand", 0.8, 0.01, 0.30, 5, 100000)
	rep.Summary = map[string]metrics.Result{"baseline": base, "cand": cand}
	rep.Candidates = []report.Candidate{{Label: "cand", Metrics: cand,
		Delta: metrics.DeltaResult{RecallDiff: metrics.Estimate{Point: 0, Lo: -0.01, Hi: 0.01},
			FPRDiff: metrics.Estimate{Point: 0, Lo: -0.001, Hi: 0.001}}}}
	rep.Scenarios = map[string][]metrics.ScenarioResult{"baseline": base.Scenarios, "cand": cand.Scenarios}
	return rep
}

func TestNeutralPasses(t *testing.T) {
	res, err := Evaluate(baseReport(), "cand", DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Passed {
		for _, c := range res.Checks {
			if !c.Passed {
				t.Fatalf("unexpected failure: %+v", c)
			}
		}
	}
}

func TestRecallRegressionFailsConservatively(t *testing.T) {
	rep := baseReport()
	cand := rep.Summary["cand"]
	cand.Estimates["fraud_caught_rate"] = metrics.Estimate{Point: 0.55, Lo: 0.50, Hi: 0.60}
	rep.Summary["cand"] = cand
	rep.Candidates[0].Delta.RecallDiff = metrics.Estimate{Point: -0.25, Lo: -0.30, Hi: -0.20}
	res, _ := Evaluate(rep, "cand", DefaultPolicy())
	if res.Passed {
		t.Fatal("recall regression passed the gate")
	}
}

func TestTinyNonSignificantDropDoesNotFail(t *testing.T) {
	rep := baseReport()
	cand := rep.Summary["cand"]
	cand.Estimates["fraud_caught_rate"] = metrics.Estimate{Point: 0.795, Lo: 0.77, Hi: 0.82}
	rep.Summary["cand"] = cand
	rep.Candidates[0].Delta.RecallDiff = metrics.Estimate{Point: -0.005, Lo: -0.02, Hi: 0.01}
	res, _ := Evaluate(rep, "cand", DefaultPolicy())
	for _, c := range res.Checks {
		if c.Name == "recall_alerted" && !c.Passed {
			t.Fatalf("conservative rule declared a regression: %+v", c)
		}
	}
}

func TestPSI(t *testing.T) {
	var a, b [2][101]int64
	a[1][10] = 100
	b[1][10] = 100
	if psi := PSI(a, b); psi != 0 {
		t.Fatalf("identical distributions psi=%v", psi)
	}
	b = [2][101]int64{}
	b[1][90] = 100
	if psi := PSI(a, b); psi <= 0.25 {
		t.Fatalf("shifted distributions psi=%v", psi)
	}
}

var _ = rules.StepUp
