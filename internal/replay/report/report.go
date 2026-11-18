// Package report builds the deterministic replay report JSON and Markdown.
package report

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/metrics"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// Version is the report schema version.
const Version = "1"

// ConfigInfo describes one resolved config.
type ConfigInfo struct {
	Label      string         `json:"label"`
	Rules      string         `json:"rules"`
	RulesHash  string         `json:"rules_hash"`
	Model      string         `json:"model"`
	ModelHash  string         `json:"model_hash"`
	Combiner   rules.Combiner `json:"combiner"`
	ConfigHash string         `json:"config_hash"`
}

// DatasetInfo pins the input.
type DatasetInfo struct {
	Name       string `json:"name"`
	Hash       string `json:"hash"`
	From       string `json:"from,omitempty"`
	To         string `json:"to,omitempty"`
	WarmupDays int    `json:"warmup_days"`
	Events     int64  `json:"events"`
	Scored     int64  `json:"scored"`
	MetricFrom string `json:"metric_from,omitempty"`
}

// Change is one event whose decision differs between baseline and candidate.
type Change struct {
	EventID         string `json:"event_id"`
	Label           string `json:"label"`
	Scenario        string `json:"scenario,omitempty"`
	Amount          int64  `json:"amount"`
	From            string `json:"from"`
	To              string `json:"to"`
	BaselineReason  string `json:"baseline_reason"`
	CandidateReason string `json:"candidate_reason"`
}

// Candidate is the per-candidate analysis.
type Candidate struct {
	Label           string              `json:"label"`
	Metrics         metrics.Result      `json:"metrics"`
	Delta           metrics.DeltaResult `json:"delta"`
	NewlyIntervened []Change            `json:"newly_intervened"`
	NewlyAllowed    []Change            `json:"newly_allowed"`
	NewlyBlocked    []Change            `json:"newly_blocked"`
	TopFraud        []Change            `json:"top_fraud_changes"`
	TopLegit        []Change            `json:"top_legit_changes"`
}

// CostModel records the assumptions used for net benefit.
type CostModel struct {
	Currency             string             `json:"currency"`
	AnalystCostPerCase   float64            `json:"analyst_cost_per_case"`
	StepUpDropoff        float64            `json:"step_up_dropoff_probability"`
	AverageFee           float64            `json:"average_fee"`
	RecoveryFactor       float64            `json:"fraud_recovery_factor"`
	NetBenefitBaseline   float64            `json:"net_benefit_baseline"`
	NetBenefitCandidates map[string]float64 `json:"net_benefit_candidates"`
}

// Report is the versioned JSON document.
type Report struct {
	ReplayReportVersion string                              `json:"replay_report_version"`
	JobID               int64                               `json:"job_id"`
	Dataset             DatasetInfo                         `json:"dataset"`
	Mode                string                              `json:"mode"`
	StatePolicy         string                              `json:"state_policy"`
	Seed                int64                               `json:"seed"`
	EngineVersion       string                              `json:"engine_version"`
	Configs             []ConfigInfo                        `json:"configs"`
	Summary             map[string]metrics.Result           `json:"summary"`
	Candidates          []Candidate                         `json:"candidates"`
	Scenarios           map[string][]metrics.ScenarioResult `json:"scenarios"`
	Rules               map[string][]metrics.RuleAgg        `json:"rules"`
	Daily               map[string][]metrics.DayResult      `json:"daily"`
	Segments            map[string][]metrics.SegmentResult  `json:"segments"`
	Sweep               map[string][]metrics.SweepPoint     `json:"sweep"`
	TimeToDetect        map[string]metrics.TimeToDetect     `json:"time_to_detect"`
	CostModel           CostModel                           `json:"cost_model"`
	Caveats             []string                            `json:"caveats"`
}

// Input is everything the report needs; the engine fills it.
type Input struct {
	JobID         int64
	Dataset       DatasetInfo
	Mode          string
	StatePolicy   string
	Seed          int64
	EngineVersion string
	Configs       []ConfigInfo
	Results       map[string]metrics.Result
	Pairs         map[string]*metrics.PairAccumulator
	Transitions   map[string]map[[2]rules.Decision]int64
	Changes       map[string][]Change
	Cost          CostModel
	ExtraCaveats  []string
}

// Build produces a deterministic report. Map keys are emitted sorted by
// encoding/json, and every slice is explicitly sorted.
func Build(in Input) *Report {
	r := &Report{
		ReplayReportVersion: Version, JobID: in.JobID, Dataset: in.Dataset,
		Mode: in.Mode, StatePolicy: in.StatePolicy, Seed: in.Seed,
		EngineVersion: in.EngineVersion, Configs: in.Configs,
		Summary: map[string]metrics.Result{}, Scenarios: map[string][]metrics.ScenarioResult{},
		Rules: map[string][]metrics.RuleAgg{}, Daily: map[string][]metrics.DayResult{},
		Segments: map[string][]metrics.SegmentResult{}, Sweep: map[string][]metrics.SweepPoint{},
		TimeToDetect: map[string]metrics.TimeToDetect{}, CostModel: in.Cost,
	}
	labels := make([]string, 0, len(in.Results))
	for label := range in.Results {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	for _, label := range labels {
		m := in.Results[label]
		r.Summary[label] = m
		r.Scenarios[label] = m.Scenarios
		r.Rules[label] = m.Rules
		r.Daily[label] = m.Daily
		r.Segments[label] = m.Segments
		r.Sweep[label] = m.Sweep
		r.TimeToDetect[label] = m.TimeToDetect
		if label == "baseline" {
			continue
		}
		pair := in.Pairs[label]
		var delta metrics.DeltaResult
		if pair != nil {
			delta = pair.PairedDelta(in.Seed, in.Transitions[label])
		}
		c := Candidate{Label: label, Metrics: m, Delta: delta}
		for _, ch := range in.Changes[label] {
			switch {
			case ch.From != string(rules.Block) && ch.To == string(rules.Block):
				c.NewlyBlocked = append(c.NewlyBlocked, ch)
			case severity(ch.From) < severity(string(rules.Hold)) && severity(ch.To) >= severity(string(rules.Hold)):
				c.NewlyIntervened = append(c.NewlyIntervened, ch)
			case severity(ch.From) >= severity(string(rules.Hold)) && severity(ch.To) < severity(string(rules.Hold)):
				c.NewlyAllowed = append(c.NewlyAllowed, ch)
			}
			if ch.Label == "FRAUD" {
				c.TopFraud = append(c.TopFraud, ch)
			} else if ch.Label == "LEGIT" {
				c.TopLegit = append(c.TopLegit, ch)
			}
		}
		sortByAmount(c.TopFraud)
		sortByAmount(c.TopLegit)
		trim(&c.TopFraud, 50)
		trim(&c.TopLegit, 50)
		trim(&c.NewlyIntervened, 100)
		trim(&c.NewlyAllowed, 100)
		trim(&c.NewlyBlocked, 100)
		r.Candidates = append(r.Candidates, c)
	}
	r.Caveats = caveats(in)
	return r
}

func severity(s string) int { return rules.Decision(s).Severity() }

func sortByAmount(c []Change) {
	sort.Slice(c, func(i, j int) bool {
		if c[i].Amount != c[j].Amount {
			return c[i].Amount > c[j].Amount
		}
		return c[i].EventID < c[j].EventID
	})
}

func trim[T any](s *[]T, n int) {
	if len(*s) > n {
		*s = (*s)[:n]
	}
}

func caveats(in Input) []string {
	out := []string{
		"Labels are the dataset's ground truth; label noise propagates to every rate.",
		"Confidence intervals resample accounts, not events, to respect within-account correlation.",
	}
	if in.Mode == "snapshot" {
		out = append(out, "Snapshot mode reuses stored features and cannot evaluate feature-code changes.")
	}
	if in.StatePolicy == "apply-block" {
		out = append(out, "apply-block is a counterfactual: blocked events would have had retries and feedback effects this replay cannot model.")
	}
	if in.Dataset.WarmupDays > 0 {
		out = append(out, fmt.Sprintf("The first %d day(s) are warm-up and excluded from metrics to avoid cold-start bias.", in.Dataset.WarmupDays))
	}
	if len(in.Configs) > 4 {
		out = append(out, "More than three candidates: p-values are not adjusted for multiple comparisons.")
	}
	out = append(out, in.ExtraCaveats...)
	return out
}

// JSON renders the canonical report bytes (no wall-clock fields).
func (r *Report) JSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// Markdown renders a human report. The output is deterministic.
func (r *Report) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Replay %d — %s\n\n", r.JobID, r.Dataset.Name)
	fmt.Fprintf(&b, "Mode `%s`, state policy `%s`, seed `%d`, engine `%s`.\n\n",
		r.Mode, r.StatePolicy, r.Seed, r.EngineVersion)
	fmt.Fprintf(&b, "Dataset `%s` (%d events, %d scored).\n\n", r.Dataset.Hash, r.Dataset.Events, r.Dataset.Scored)

	b.WriteString("## Summary\n\n")
	b.WriteString("| Config | Recall (alerted) | Recall (≥HOLD) | FPR | Friction | Amount saved | Amount missed | Workload/day |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	labels := sortedKeys(r.Summary)
	for _, label := range labels {
		m := r.Summary[label]
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %d | %d | %.1f |\n", label,
			pct(m.Estimates["fraud_caught_rate"]), pct(m.Estimates["fraud_intervened_rate"]),
			pct(m.Estimates["false_positive_rate"]), pct(m.Estimates["customer_friction"]),
			m.Amounts["FRAUD:INTERVENED"], m.Amounts["FRAUD:MISSED"], m.Workload.PerDay)
	}
	b.WriteString("\nIntervals are 95% bootstrap CIs over accounts; the table prints the point estimate (CI in JSON).\n")

	b.WriteString("\n## Per-scenario recall\n\n| Config | Scenario | Fraud | Alerted | ≥HOLD | Caught amount |\n|---|---|---|---|---|---|\n")
	for _, label := range labels {
		for _, s := range r.Scenarios[label] {
			fmt.Fprintf(&b, "| %s | %s | %d | %.1f%% | %.1f%% | %d |\n", label, s.Scenario, s.Fraud,
				s.RecallAlerted*100, s.RecallIntervened*100, s.CaughtAmount)
		}
	}

	b.WriteString("\n## Rule movers (top 10 by fires)\n\n| Config | Rule | Fires | TP | FP | Precision | Unique catches |\n|---|---|---|---|---|---|---|\n")
	for _, label := range labels {
		rs := r.Rules[label]
		if len(rs) > 10 {
			rs = rs[:10]
		}
		for _, x := range rs {
			fmt.Fprintf(&b, "| %s | %s | %d | %d | %d | %.3f | %d |\n", label, x.RuleID, x.Fires, x.TP, x.FP, x.Precision, x.UniqueCatch)
		}
	}

	b.WriteString("\n## Deltas\n\n")
	for _, c := range r.Candidates {
		fmt.Fprintf(&b, "### %s\n\n", c.Label)
		fmt.Fprintf(&b, "- McNemar (fraud): baseline-only %d, candidate-only %d, p=%.4g\n",
			c.Delta.McNemarFraud.BaselineOnly, c.Delta.McNemarFraud.CandidateOnly, c.Delta.McNemarFraud.PValue)
		fmt.Fprintf(&b, "- McNemar (legit): baseline-only %d, candidate-only %d, p=%.4g\n",
			c.Delta.McNemarLegit.BaselineOnly, c.Delta.McNemarLegit.CandidateOnly, c.Delta.McNemarLegit.PValue)
		fmt.Fprintf(&b, "- Recall difference %+.4f (95%% CI %+.4f, %+.4f)\n",
			c.Delta.RecallDiff.Point, c.Delta.RecallDiff.Lo, c.Delta.RecallDiff.Hi)
		fmt.Fprintf(&b, "- FPR difference %+.4f (95%% CI %+.4f, %+.4f)\n",
			c.Delta.FPRDiff.Point, c.Delta.FPRDiff.Lo, c.Delta.FPRDiff.Hi)
		b.WriteString("\nTransition matrix:\n\n| From | To | Count |\n|---|---|---|\n")
		for _, tr := range c.Delta.Transition {
			fmt.Fprintf(&b, "| %s | %s | %d |\n", tr.From, tr.To, tr.Count)
		}
		b.WriteString("\n")
	}

	b.WriteString("## Time to detect\n\n")
	for _, label := range labels {
		t := r.TimeToDetect[label]
		fmt.Fprintf(&b, "- %s: %d/%d runs detected, p50 %.1f min, p90 %.1f min, loss before detection %d\n",
			label, t.Detected, t.Runs, t.P50Minutes, t.P90Minutes, t.LossBefore)
	}

	b.WriteString("\n## Cost model (modelling aid, not accounting)\n\n")
	fmt.Fprintf(&b, "Currency %s; analyst %0.f/case; STEP_UP drop-off %.0f%% × fee %0.f; recovery %.0f%%.\n",
		r.CostModel.Currency, r.CostModel.AnalystCostPerCase, r.CostModel.StepUpDropoff*100,
		r.CostModel.AverageFee, r.CostModel.RecoveryFactor*100)
	for label, v := range r.CostModel.NetBenefitCandidates {
		fmt.Fprintf(&b, "- %s net benefit: %.0f\n", label, v)
	}

	b.WriteString("\n## Caveats\n\n")
	for _, c := range r.Caveats {
		fmt.Fprintf(&b, "- %s\n", c)
	}
	return b.String()
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func pct(e metrics.Estimate) string {
	return fmt.Sprintf("%.2f%% (%.2f–%.2f)", e.Point*100, e.Lo*100, e.Hi*100)
}
