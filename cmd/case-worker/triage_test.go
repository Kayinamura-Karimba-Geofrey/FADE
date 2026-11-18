package main

import (
	"testing"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/cases"
)

func TestTriageOutcomeOnlyOpensCasesThatNeedReview(t *testing.T) {
	now := time.Now().UTC()
	casesList := []struct {
		name     string
		decision string
		rate     float64
		want     cases.Source
		open     bool
	}{
		{"block", "BLOCK", 0, cases.SourceDecision, true},
		{"hold", "HOLD", 0, cases.SourceDecision, true},
		{"step-up waits for the challenge", "STEP_UP", 1, "", false},
		{"allow without sampling", "ALLOW", 0, "", false},
	}
	for _, c := range casesList {
		d := &cases.DecisionRef{ID: "dec-" + c.name, Decision: c.decision, DecidedAt: now}
		source, open := triageOutcome(d, c.rate)
		if open != c.open || source != c.want {
			t.Errorf("%s: source=%q open=%v, want %q/%v", c.name, source, open, c.want, c.open)
		}
	}

	// A sampled ALLOW becomes a QA case, and only for ids the sampler selects.
	d := &cases.DecisionRef{ID: "dec-allow-1", Decision: "ALLOW", DecidedAt: now}
	source, open := triageOutcome(d, 1)
	if !open || source != cases.SourceQASample {
		t.Fatalf("rate 1 must sample: source=%q open=%v", source, open)
	}
	for _, id := range []string{"dec-allow-1", "dec-allow-2", "dec-allow-3"} {
		d := &cases.DecisionRef{ID: id, Decision: "ALLOW", DecidedAt: now}
		source, open := triageOutcome(d, 0.005)
		if open != cases.QASampleSelected(id, 0.005) {
			t.Fatalf("%s: sampling decision disagrees with QASampleSelected", id)
		}
		if open && source != cases.SourceQASample {
			t.Fatalf("%s: sampled cases must be QA, got %q", id, source)
		}
	}
}

func TestTriageOutcomeIsCaseSensitive(t *testing.T) {
	d := &cases.DecisionRef{ID: "dec-lower", Decision: "hold"}
	if _, open := triageOutcome(d, 1); open {
		t.Fatal("decision classes are upper-case; a lower-case value is not a HOLD")
	}
}
