// Package gate evaluates a replay candidate against the promotion policy.
package gate

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/report"
)

// Policy mirrors config/gate.yaml.
type Policy struct {
	RulesOnly struct {
		MinRecallDelta         float64 `yaml:"min_recall_delta"`
		MaxFPRIncrease         float64 `yaml:"max_fpr_increase"`
		MaxFrictionRelative    float64 `yaml:"max_friction_relative"`
		MinScenarioRecallDelta float64 `yaml:"min_scenario_recall_delta"`
		MinScenarioFraud       int64   `yaml:"min_scenario_fraud"`
		MinAmountSavedRelative float64 `yaml:"min_amount_saved_relative"`
		MaxWorkloadRelative    float64 `yaml:"max_workload_relative"`
	} `yaml:"rules_only"`
	WithModel struct {
		MaxPSI          float64 `yaml:"max_psi"`
		MaxECE          float64 `yaml:"max_ece"`
		MaxDegradedRate float64 `yaml:"max_degraded_rate"`
	} `yaml:"with_model"`
	Improvement struct {
		Required bool `yaml:"required"`
	} `yaml:"improvement"`
}

// Check is one gate line.
type Check struct {
	Name      string  `json:"name"`
	Passed    bool    `json:"passed"`
	Measured  float64 `json:"measured"`
	Threshold float64 `json:"threshold"`
	Detail    string  `json:"detail,omitempty"`
}

// Result is the gate decision.
type Result struct {
	JobID      int64   `json:"job_id"`
	Candidate  string  `json:"candidate"`
	Passed     bool    `json:"passed"`
	Checks     []Check `json:"checks"`
	ConfigHash string  `json:"config_hash"`
	RulesHash  string  `json:"rules_hash"`
	Model      string  `json:"model_version"`
}

// LoadPolicy reads a YAML policy. Missing keys keep zero values, which would
// make a guardrail vacuous; callers should use DefaultPolicy in that case.
func LoadPolicy(path string) (Policy, error) {
	var p Policy
	raw, err := os.ReadFile(path)
	if err != nil {
		return p, err
	}
	if err := yaml.Unmarshal(raw, &p); err != nil {
		return p, err
	}
	return p, nil
}

// DefaultPolicy returns the documented policy.
func DefaultPolicy() Policy {
	var p Policy
	p.RulesOnly.MinRecallDelta = -0.01
	p.RulesOnly.MaxFPRIncrease = 0.001
	p.RulesOnly.MaxFrictionRelative = 0.10
	p.RulesOnly.MinScenarioRecallDelta = -0.05
	p.RulesOnly.MinScenarioFraud = 30
	p.RulesOnly.MinAmountSavedRelative = -0.02
	p.RulesOnly.MaxWorkloadRelative = 0.20
	p.WithModel.MaxPSI = 0.25
	p.WithModel.MaxECE = 0.10
	p.WithModel.MaxDegradedRate = 0.001
	return p
}

// Evaluate compares candidate with the baseline in a parsed report.
func Evaluate(rep *report.Report, candidate string, policy Policy) (*Result, error) {
	base, ok := rep.Summary["baseline"]
	if !ok {
		return nil, fmt.Errorf("gate: report has no baseline summary")
	}
	cand, ok := rep.Summary[candidate]
	if !ok {
		return nil, fmt.Errorf("gate: report has no candidate %q", candidate)
	}
	var delta *report.Candidate
	for i := range rep.Candidates {
		if rep.Candidates[i].Label == candidate {
			delta = &rep.Candidates[i]
			break
		}
	}
	if delta == nil {
		return nil, fmt.Errorf("gate: candidate %q has no delta analysis", candidate)
	}
	res := &Result{JobID: rep.JobID, Candidate: candidate, Checks: []Check{}}
	for _, c := range rep.Configs {
		if c.Label == candidate {
			res.ConfigHash, res.RulesHash, res.Model = c.ConfigHash, c.RulesHash, c.Model
		}
	}

	// Non-inferiority: a regression is declared only when the paired CI puts
	// the whole difference below the allowed slack.
	recallCheck := Check{Name: "recall_alerted", Measured: cand.Estimates["fraud_caught_rate"].Point,
		Threshold: policy.RulesOnly.MinRecallDelta,
		Detail: fmt.Sprintf("baseline %.4f, candidate %.4f, paired CI [%.4f, %.4f]",
			base.Estimates["fraud_caught_rate"].Point, cand.Estimates["fraud_caught_rate"].Point,
			delta.Delta.RecallDiff.Lo, delta.Delta.RecallDiff.Hi)}
	recallCheck.Passed = delta.Delta.RecallDiff.Hi >= policy.RulesOnly.MinRecallDelta
	res.Checks = append(res.Checks, recallCheck)

	fprCheck := Check{Name: "false_positive_rate", Measured: cand.Estimates["false_positive_rate"].Point,
		Threshold: policy.RulesOnly.MaxFPRIncrease,
		Detail: fmt.Sprintf("baseline %.4f, candidate %.4f, paired diff CI [%.4f, %.4f]",
			base.Estimates["false_positive_rate"].Point, cand.Estimates["false_positive_rate"].Point,
			delta.Delta.FPRDiff.Lo, delta.Delta.FPRDiff.Hi)}
	fprCheck.Passed = delta.Delta.FPRDiff.Lo <= policy.RulesOnly.MaxFPRIncrease
	res.Checks = append(res.Checks, fprCheck)

	baseFric := base.Estimates["customer_friction"].Point
	candFric := cand.Estimates["customer_friction"].Point
	fric := Check{Name: "customer_friction_relative", Measured: candFric,
		Threshold: baseFric * (1 + policy.RulesOnly.MaxFrictionRelative),
		Detail:    fmt.Sprintf("baseline %.4f, allowed +%.0f%%", baseFric, policy.RulesOnly.MaxFrictionRelative*100)}
	fric.Passed = baseFric == 0 || candFric <= baseFric*(1+policy.RulesOnly.MaxFrictionRelative)
	res.Checks = append(res.Checks, fric)

	for _, s := range cand.Scenarios {
		if s.Fraud < policy.RulesOnly.MinScenarioFraud {
			continue
		}
		var b float64
		found := false
		for _, bs := range base.Scenarios {
			if bs.Scenario == s.Scenario {
				b, found = bs.RecallAlerted, true
				break
			}
		}
		if !found {
			continue
		}
		c := Check{Name: "scenario_recall:" + s.Scenario, Measured: s.RecallAlerted,
			Threshold: b + policy.RulesOnly.MinScenarioRecallDelta,
			Detail:    fmt.Sprintf("baseline %.4f, fraud events %d", b, s.Fraud)}
		c.Passed = s.RecallAlerted >= b+policy.RulesOnly.MinScenarioRecallDelta
		res.Checks = append(res.Checks, c)
	}

	baseSaved := float64(base.Amounts["FRAUD:INTERVENED"])
	candSaved := float64(cand.Amounts["FRAUD:INTERVENED"])
	saved := Check{Name: "amount_saved", Measured: candSaved,
		Threshold: baseSaved * (1 + policy.RulesOnly.MinAmountSavedRelative),
		Detail:    fmt.Sprintf("baseline %.0f, allowed %.0f%%", baseSaved, policy.RulesOnly.MinAmountSavedRelative*100)}
	saved.Passed = baseSaved == 0 || candSaved >= baseSaved*(1+policy.RulesOnly.MinAmountSavedRelative)
	res.Checks = append(res.Checks, saved)

	baseLoad := base.Workload.PerDay
	candLoad := cand.Workload.PerDay
	load := Check{Name: "analyst_workload", Measured: candLoad,
		Threshold: baseLoad * (1 + policy.RulesOnly.MaxWorkloadRelative),
		Detail:    fmt.Sprintf("baseline %.2f/day, allowed +%.0f%%", baseLoad, policy.RulesOnly.MaxWorkloadRelative*100)}
	load.Passed = baseLoad == 0 || candLoad <= baseLoad*(1+policy.RulesOnly.MaxWorkloadRelative)
	res.Checks = append(res.Checks, load)

	if res.Model != "" && res.Model != "none" {
		psi := PSI(base.RiskHistogram, cand.RiskHistogram)
		psiCheck := Check{Name: "score_psi", Measured: psi, Threshold: policy.WithModel.MaxPSI}
		psiCheck.Passed = psi < policy.WithModel.MaxPSI
		res.Checks = append(res.Checks, psiCheck)

		degraded := 0.0
		if cand.MLScored > 0 {
			degraded = float64(cand.MLDegraded) / float64(cand.MLScored)
		}
		deg := Check{Name: "ml_degraded_rate", Measured: degraded, Threshold: policy.WithModel.MaxDegradedRate}
		deg.Passed = degraded < policy.WithModel.MaxDegradedRate
		res.Checks = append(res.Checks, deg)

		if ece, ok := modelECE(res.Model); ok {
			ec := Check{Name: "model_ece", Measured: ece, Threshold: policy.WithModel.MaxECE}
			ec.Passed = ece <= policy.WithModel.MaxECE
			res.Checks = append(res.Checks, ec)
		}
	}

	if policy.Improvement.Required {
		improved := cand.Estimates["fraud_caught_rate"].Point > base.Estimates["fraud_caught_rate"].Point ||
			cand.Amounts["FRAUD:INTERVENED"] > base.Amounts["FRAUD:INTERVENED"] ||
			cand.TimeToDetect.P50Minutes < base.TimeToDetect.P50Minutes
		imp := Check{Name: "strict_improvement", Measured: boolFloat(improved), Threshold: 1}
		imp.Passed = improved
		res.Checks = append(res.Checks, imp)
	}

	res.Passed = true
	for _, c := range res.Checks {
		if !c.Passed {
			res.Passed = false
		}
	}
	return res, nil
}

func boolFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// PSI computes the population stability index of two risk histograms.
func PSI(base, cand [2][101]int64) float64 {
	return psi1D(hist(base), hist(cand))
}

func hist(h [2][101]int64) [101]int64 {
	var out [101]int64
	for i := 0; i <= 100; i++ {
		out[i] = h[0][i] + h[1][i]
	}
	return out
}

func psi1D(a, b [101]int64) float64 {
	var ta, tb int64
	for i := 0; i <= 100; i++ {
		ta += a[i]
		tb += b[i]
	}
	if ta == 0 || tb == 0 {
		return 0
	}
	sum := 0.0
	for i := 0; i <= 100; i++ {
		pa := float64(a[i]) / float64(ta)
		pb := float64(b[i]) / float64(tb)
		if pa == 0 {
			pa = 0.0001
		}
		if pb == 0 {
			pb = 0.0001
		}
		sum += (pb - pa) * math.Log(pb/pa)
	}
	return sum
}

func modelECE(version string) (float64, bool) {
	raw, err := os.ReadFile(filepath.Join("ml", "models", version, "metrics.json"))
	if err != nil {
		return 0, false
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return 0, false
	}
	if v, ok := findNumber(doc, "ece"); ok {
		return v, true
	}
	return 0, false
}

func findNumber(v any, key string) (float64, bool) {
	switch x := v.(type) {
	case map[string]any:
		if val, ok := x[key]; ok {
			if f, ok := val.(float64); ok {
				return f, true
			}
		}
		for _, val := range x {
			if f, ok := findNumber(val, key); ok {
				return f, true
			}
		}
	}
	return 0, false
}
