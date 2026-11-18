package metrics

import (
	"math"
	"testing"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

func miniAccumulator() *Accumulator {
	a := NewAccumulator("test")
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Fraud 100 (A, STEP_UP), fraud 200 (A, ALLOW), fraud 300 (B, HOLD).
	a.Add(Event{EventID: "f1", AccountID: "A", Segment: "s1", Label: "FRAUD",
		Scenario: "VELOCITY", RunID: "r1", Amount: 100, EventTime: t0},
		Outcome{Decision: rules.StepUp, Risk: 80, Fired: []string{"R1"}}, rules.DefaultCombiner())
	a.Add(Event{EventID: "f2", AccountID: "A", Segment: "s1", Label: "FRAUD",
		Scenario: "VELOCITY", RunID: "r1", Amount: 200, EventTime: t0.Add(time.Minute)},
		Outcome{Decision: rules.Allow, Risk: 20, Fired: []string{"R2"}}, rules.DefaultCombiner())
	a.Add(Event{EventID: "f3", AccountID: "B", Segment: "s2", Label: "FRAUD",
		Scenario: "ATO_SIMSWAP", RunID: "r2", Amount: 300, EventTime: t0.Add(2 * time.Minute)},
		Outcome{Decision: rules.Hold, Risk: 70, Fired: []string{"R1", "R2"}}, rules.DefaultCombiner())
	// Legit 50 (B, STEP_UP), legit 60 (A, ALLOW).
	a.Add(Event{EventID: "l1", AccountID: "B", Segment: "s2", Label: "LEGIT",
		Amount: 50, EventTime: t0.Add(3 * time.Minute)},
		Outcome{Decision: rules.StepUp, Risk: 50, Fired: []string{"R2"}}, rules.DefaultCombiner())
	a.Add(Event{EventID: "l2", AccountID: "A", Segment: "s1", Label: "LEGIT",
		Amount: 60, EventTime: t0.Add(4 * time.Minute)},
		Outcome{Decision: rules.Allow, Risk: 10}, rules.DefaultCombiner())
	return a
}

func TestHandComputedRates(t *testing.T) {
	r := miniAccumulator().Finalize(7)
	if r.Scored != 5 || r.Fraud != 3 || r.Legit != 2 {
		t.Fatalf("counts %+v", r)
	}
	if got := r.Estimates["fraud_caught_rate"].Point; math.Abs(got-2.0/3.0) > 1e-12 {
		t.Fatalf("recall alerted = %v", got)
	}
	if got := r.Estimates["fraud_intervened_rate"].Point; math.Abs(got-1.0/3.0) > 1e-12 {
		t.Fatalf("recall intervened = %v", got)
	}
	if got := r.Estimates["false_positive_rate"].Point; math.Abs(got-0.5) > 1e-12 {
		t.Fatalf("fpr = %v", got)
	}
	if got := r.Estimates["customer_friction"].Point; math.Abs(got-3.0/5.0) > 1e-12 {
		t.Fatalf("friction = %v", got)
	}
	if r.Amounts["FRAUD:INTERVENED"] != 300 {
		t.Fatalf("saved = %d", r.Amounts["FRAUD:INTERVENED"])
	}
	if r.Amounts["FRAUD:MISSED"] != 300 {
		t.Fatalf("missed = %d", r.Amounts["FRAUD:MISSED"])
	}
	if r.Workload.Interventions != 1 || r.Workload.PerDay != 1 {
		t.Fatalf("workload = %+v", r.Workload)
	}
}

func TestSweepAndRuleStats(t *testing.T) {
	r := miniAccumulator().Finalize(7)
	var at40 SweepPoint
	for _, p := range r.Sweep {
		if p.Threshold == 40 {
			at40 = p
		}
	}
	if math.Abs(at40.Recall-2.0/3.0) > 1e-12 {
		t.Fatalf("sweep recall = %v", at40.Recall)
	}
	if math.Abs(at40.FPR-0.5) > 1e-12 {
		t.Fatalf("sweep fpr = %v", at40.FPR)
	}
	if len(r.Rules) != 2 {
		t.Fatalf("rules = %d", len(r.Rules))
	}
	var r1 RuleAgg
	for _, x := range r.Rules {
		if x.RuleID == "R1" {
			r1 = x
		}
	}
	// R1 fires on f1 and f3, both fraud -> precision 1, one unique catch (f1).
	if r1.Fires != 2 || r1.TP != 2 || r1.UniqueCatch != 1 {
		t.Fatalf("R1 stats = %+v", r1)
	}
}

func TestMcNemarExact(t *testing.T) {
	// n=3, k=1: two-sided p = 2*(C(0)+C(1))/8 = 1.
	_, p := McNemar(1, 2)
	if math.Abs(p-1) > 1e-12 {
		t.Fatalf("p = %v", p)
	}
	// n=10, k=0: p = 2/1024.
	_, p = McNemar(0, 10)
	if math.Abs(p-2.0/1024.0) > 1e-12 {
		t.Fatalf("p = %v", p)
	}
}

func TestNoFraudEdgeCases(t *testing.T) {
	a := NewAccumulator("empty")
	r := a.Finalize(1)
	if r.Scored != 0 || r.Estimates["fraud_caught_rate"].Point != 0 {
		t.Fatalf("empty result = %+v", r)
	}
	if r.TimeToDetect.Runs != 0 || r.Workload.PerDay != 0 {
		t.Fatalf("empty time = %+v", r.TimeToDetect)
	}
}
