package metrics

import (
	"math"
	"math/rand"
	"sort"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// Result is the deterministic metrics document stored in replay_results.
type Result struct {
	Label             string              `json:"label"`
	Scored            int64               `json:"scored"`
	Fraud             int64               `json:"fraud"`
	Legit             int64               `json:"legit"`
	Unlabelled        int64               `json:"unlabelled"`
	MLScored          int64               `json:"ml_scored"`
	MLDegraded        int64               `json:"ml_degraded"`
	SkippedLate       int64               `json:"skipped_late"`
	SkippedDup        int64               `json:"skipped_duplicate"`
	SkippedInvalid    int64               `json:"skipped_invalid"`
	SkippedNoSnapshot int64               `json:"skipped_no_snapshot"`
	Estimates         map[string]Estimate `json:"estimates"`
	Decisions         map[string]int64    `json:"decisions"`
	Amounts           map[string]int64    `json:"amounts"`
	Workload          Workload            `json:"workload"`
	TimeToDetect      TimeToDetect        `json:"time_to_detect"`
	Scenarios         []ScenarioResult    `json:"scenarios"`
	Segments          []SegmentResult     `json:"segments"`
	Daily             []DayResult         `json:"daily"`
	Rules             []RuleAgg           `json:"rules"`
	Shadow            []RuleAgg           `json:"shadow"`
	Overlap           []Overlap           `json:"overlap"`
	Sweep             []SweepPoint        `json:"sweep"`
	RiskHistogram     [2][101]int64       `json:"risk_histogram"`
	Days              int64               `json:"days"`
}

// Workload is analyst load.
type Workload struct {
	Interventions int64   `json:"interventions"`
	PerDay        float64 `json:"per_day"`
	Per1000       float64 `json:"per_1000_events"`
}

// TimeToDetect summarises scenario runs.
type TimeToDetect struct {
	Runs             int64   `json:"runs"`
	Detected         int64   `json:"detected"`
	DetectedShare    float64 `json:"detected_share"`
	P50Minutes       float64 `json:"p50_minutes"`
	P90Minutes       float64 `json:"p90_minutes"`
	LossBefore       int64   `json:"loss_before_detection"`
	TotalFraudAmount int64   `json:"total_fraud_amount"`
}

// ScenarioResult is per-scenario recall.
type ScenarioResult struct {
	Scenario         string  `json:"scenario"`
	Fraud            int64   `json:"fraud"`
	RecallAlerted    float64 `json:"recall_alerted"`
	RecallIntervened float64 `json:"recall_intervened"`
	CaughtAmount     int64   `json:"caught_amount"`
	Runs             int64   `json:"runs"`
	Detected         int64   `json:"detected"`
	LowPower         bool    `json:"low_power"`
}

// SegmentResult is customer impact by segment.
type SegmentResult struct {
	Segment  string  `json:"segment"`
	Scored   int64   `json:"scored"`
	Legit    int64   `json:"legit"`
	FPR      float64 `json:"fpr"`
	Friction float64 `json:"friction"`
}

// DayResult is one day of the drift series.
type DayResult struct {
	Date            string  `json:"date"`
	Scored          int64   `json:"scored"`
	Fraud           int64   `json:"fraud"`
	Recall          float64 `json:"recall"`
	FPR             float64 `json:"fpr"`
	Interventions   int64   `json:"interventions"`
	WorkloadPer1000 float64 `json:"workload_per_1000"`
}

// Overlap is a rule-pair co-fire count.
type Overlap struct {
	Pair  string `json:"pair"`
	Count int64  `json:"count"`
}

// SweepPoint is one risk threshold in the precision/recall curve.
type SweepPoint struct {
	Threshold int     `json:"threshold"`
	Recall    float64 `json:"recall"`
	Precision float64 `json:"precision"`
	FPR       float64 `json:"fpr"`
	Alerts    int64   `json:"alerts"`
}

// Finalize computes the deterministic result document.
func (a *Accumulator) Finalize(seed int64) Result {
	r := Result{
		Label: a.Label, Scored: a.Scored, Fraud: a.Fraud, Legit: a.Legit,
		Unlabelled: a.Unlabelled, MLScored: a.MLScored, MLDegraded: a.MLDegraded,
		SkippedLate: a.SkippedLate, RiskHistogram: a.Risk,
		SkippedDup: a.SkippedDup, SkippedInvalid: a.SkippedInvalid,
		SkippedNoSnapshot: a.SkippedNoSnapshot,
		Estimates:         a.Bootstrap(seed, 1000, 4),
		Decisions:         a.Decisions, Amounts: a.Amounts,
		Days:  int64(len(a.Days)),
		Rules: sortedRules(a.Rules), Shadow: sortedRules(a.Shadow),
	}
	inter := a.Decisions[string(rules.Hold)] + a.Decisions[string(rules.Block)]
	days := float64(len(a.Days))
	if days == 0 {
		days = 1
	}
	r.Workload = Workload{Interventions: inter,
		PerDay:  float64(inter) / days,
		Per1000: 1000 * Ratio(inter, a.Scored)}

	daysList := make([]string, 0, len(a.Days))
	for d := range a.Days {
		daysList = append(daysList, d)
	}
	sort.Strings(daysList)
	for _, d := range daysList {
		x := a.Days[d]
		r.Daily = append(r.Daily, DayResult{
			Date: x.Date, Scored: x.Scored, Fraud: x.Fraud,
			Recall: Ratio(x.FraudAlert, x.Fraud),
			FPR:    Ratio(x.LegitAlert, x.Legit), Interventions: x.Inter,
			WorkloadPer1000: 1000 * Ratio(x.Inter, x.Scored),
		})
	}

	scen := make([]string, 0, len(a.Scenarios))
	for s := range a.Scenarios {
		scen = append(scen, s)
	}
	sort.Strings(scen)
	for _, s := range scen {
		x := a.Scenarios[s]
		det := int64(0)
		for _, run := range a.Runs {
			if run.Scenario == s && run.FirstInterventionMS > 0 {
				det++
			}
		}
		r.Scenarios = append(r.Scenarios, ScenarioResult{
			Scenario: s, Fraud: x.Fraud,
			RecallAlerted:    Ratio(x.Alerted, x.Fraud),
			RecallIntervened: Ratio(x.Intervened, x.Fraud),
			CaughtAmount:     x.CaughtAmount, Runs: x.Runs, Detected: det,
			LowPower: x.Fraud < 30,
		})
	}

	segs := make([]string, 0, len(a.Segments))
	for s := range a.Segments {
		segs = append(segs, s)
	}
	sort.Strings(segs)
	for _, s := range segs {
		x := a.Segments[s]
		r.Segments = append(r.Segments, SegmentResult{
			Segment: s, Scored: x.Scored, Legit: x.Legit,
			FPR: Ratio(x.LegitAlert, x.Legit), Friction: Ratio(x.Friction, x.Scored),
		})
	}

	r.TimeToDetect = timeToDetect(a.Runs)
	r.Sweep = sweep(a.Risk)
	ov := make([]Overlap, 0, len(a.Overlap))
	for p, n := range a.Overlap {
		ov = append(ov, Overlap{Pair: p, Count: n})
	}
	sort.Slice(ov, func(i, j int) bool {
		if ov[i].Count != ov[j].Count {
			return ov[i].Count > ov[j].Count
		}
		return ov[i].Pair < ov[j].Pair
	})
	if len(ov) > 10 {
		ov = ov[:10]
	}
	r.Overlap = ov
	return r
}

func sortedRules(m map[string]*RuleAgg) []RuleAgg {
	out := make([]RuleAgg, 0, len(m))
	for _, r := range m {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Fires != out[j].Fires {
			return out[i].Fires > out[j].Fires
		}
		return out[i].RuleID < out[j].RuleID
	})
	return out
}

func timeToDetect(runs map[string]*RunAgg) TimeToDetect {
	out := TimeToDetect{Runs: int64(len(runs))}
	var mins []float64
	for _, r := range runs {
		out.TotalFraudAmount += r.TotalFraudAmount
		if r.FirstInterventionMS > 0 {
			out.Detected++
			d := float64(r.FirstInterventionMS-r.FirstFraudMS) / 60000
			if d < 0 {
				d = 0
			}
			mins = append(mins, d)
		} else {
			out.LossBefore += r.TotalFraudAmount
		}
	}
	out.DetectedShare = Ratio(out.Detected, out.Runs)
	sort.Float64s(mins)
	out.P50Minutes = quantile(mins, 0.5)
	out.P90Minutes = quantile(mins, 0.9)
	return out
}

func sweep(h [2][101]int64) []SweepPoint {
	var fraud, legit int64
	for i := 0; i <= 100; i++ {
		fraud += h[0][i]
		legit += h[1][i]
	}
	out := make([]SweepPoint, 0, 18)
	for t := 10; t <= 95; t += 5 {
		var f, l int64
		for i := t; i <= 100; i++ {
			f += h[0][i]
			l += h[1][i]
		}
		out = append(out, SweepPoint{
			Threshold: t, Recall: Ratio(f, fraud), Precision: Ratio(f, f+l),
			FPR: Ratio(l, legit), Alerts: f + l,
		})
	}
	return out
}

// McNemar returns the exact two-sided binomial p-value for paired counts.
func McNemar(b, c int64) (float64, float64) {
	n := b + c
	if n == 0 {
		return 0, 1
	}
	k := b
	if c < b {
		k = c
	}
	// Two-sided exact binomial with p=0.5.
	sum := 0.0
	logN := logFactorial(n)
	for i := int64(0); i <= k; i++ {
		sum += math.Exp(logN - logFactorial(i) - logFactorial(n-i) - float64(n)*math.Ln2)
	}
	p := 2 * sum
	if p > 1 {
		p = 1
	}
	stat := 0.0
	if n > 0 {
		stat = float64(b-c) * float64(b-c) / float64(n)
	}
	return stat, p
}

func logFactorial(n int64) float64 {
	if n < 2 {
		return 0
	}
	v, _ := math.Lgamma(float64(n + 1))
	return v
}

// DeltaResult is baseline -> candidate paired analysis.
type DeltaResult struct {
	Transition   []Transition  `json:"transition"`
	McNemarFraud McNemarResult `json:"mcnemar_fraud"`
	McNemarLegit McNemarResult `json:"mcnemar_legit"`
	RecallDiff   Estimate      `json:"recall_diff"`
	FPRDiff      Estimate      `json:"fpr_diff"`
}

// McNemarResult carries the test outputs.
type McNemarResult struct {
	BaselineOnly  int64   `json:"baseline_only"`
	CandidateOnly int64   `json:"candidate_only"`
	Statistic     float64 `json:"statistic"`
	PValue        float64 `json:"p_value"`
}

// Transition is one cell of the 4x4 decision matrix.
type Transition struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Count int64  `json:"count"`
}

// PairedDelta computes transition counts and paired bootstrap differences.
func (p *PairAccumulator) PairedDelta(seed int64, transitions map[[2]rules.Decision]int64) DeltaResult {
	var fraudBase, fraudCand, legitBase, legitCand int64
	pairs := make([]*PairAgg, 0, len(p.Accounts))
	ids := make([]string, 0, len(p.Accounts))
	for id := range p.Accounts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		pairs = append(pairs, p.Accounts[id])
	}
	for _, x := range pairs {
		fraudBase += x.FraudBothAlert + x.FraudBaseOnly
		fraudCand += x.FraudBothAlert + x.FraudCandOnly
		legitBase += x.LegitBothAlert + x.LegitBaseOnly
		legitCand += x.LegitBothAlert + x.LegitCandOnly
	}
	out := DeltaResult{}
	// Discordant cells, computed from the per-account sufficient statistics.
	var fb, fc, lb, lc int64
	for _, x := range pairs {
		fb += x.FraudBaseOnly
		fc += x.FraudCandOnly
		lb += x.LegitBaseOnly
		lc += x.LegitCandOnly
	}
	stat, pv := McNemar(fb, fc)
	out.McNemarFraud = McNemarResult{BaselineOnly: fb, CandidateOnly: fc, Statistic: stat, PValue: pv}
	stat, pv = McNemar(lb, lc)
	out.McNemarLegit = McNemarResult{BaselineOnly: lb, CandidateOnly: lc, Statistic: stat, PValue: pv}

	for from, count := range transitions {
		out.Transition = append(out.Transition, Transition{From: string(from[0]), To: string(from[1]), Count: count})
	}
	sort.Slice(out.Transition, func(i, j int) bool {
		if out.Transition[i].From != out.Transition[j].From {
			return out.Transition[i].From < out.Transition[j].From
		}
		return out.Transition[i].To < out.Transition[j].To
	})

	if len(pairs) == 0 {
		return out
	}
	const resamples = 1000
	recall := make([]float64, 0, resamples)
	fpr := make([]float64, 0, resamples)
	rng := rand.New(rand.NewSource(seed))
	for k := 0; k < resamples; k++ {
		var acc PairAgg
		for i := 0; i < len(pairs); i++ {
			pairs[rng.Intn(len(pairs))].add(&acc)
		}
		fraudTotal := acc.FraudBothAlert + acc.FraudBaseOnly + acc.FraudCandOnly + acc.FraudNeither
		legitTotal := acc.LegitBothAlert + acc.LegitBaseOnly + acc.LegitCandOnly + acc.LegitNeither
		recall = append(recall, Ratio(acc.FraudCandOnly+acc.FraudBothAlert, fraudTotal)-Ratio(acc.FraudBaseOnly+acc.FraudBothAlert, fraudTotal))
		fpr = append(fpr, Ratio(acc.LegitCandOnly+acc.LegitBothAlert, legitTotal)-Ratio(acc.LegitBaseOnly+acc.LegitBothAlert, legitTotal))
	}
	sort.Float64s(recall)
	sort.Float64s(fpr)
	var allFraud, allLegit int64
	for _, x := range pairs {
		allFraud += x.FraudBothAlert + x.FraudBaseOnly + x.FraudCandOnly + x.FraudNeither
		allLegit += x.LegitBothAlert + x.LegitBaseOnly + x.LegitCandOnly + x.LegitNeither
	}
	out.RecallDiff = Estimate{
		Point: Ratio(fraudCand, allFraud) - Ratio(fraudBase, allFraud),
		Lo:    quantile(recall, 0.025), Hi: quantile(recall, 0.975), N: allFraud,
	}
	out.FPRDiff = Estimate{
		Point: Ratio(legitCand, allLegit) - Ratio(legitBase, allLegit),
		Lo:    quantile(fpr, 0.025), Hi: quantile(fpr, 0.975), N: allLegit,
	}
	return out
}
