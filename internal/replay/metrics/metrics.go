// Package metrics holds exact replay accumulators and the seeded statistics
// used by reports and the promotion gate. Counts are exact; only confidence
// intervals resample, and they resample accounts (not events).
package metrics

import (
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// Estimate is a point estimate with a 95% bootstrap interval.
type Estimate struct {
	Point float64 `json:"point"`
	Lo    float64 `json:"lo"`
	Hi    float64 `json:"hi"`
	N     int64   `json:"n"`
}

// Event is the label and identity context for one scored event.
type Event struct {
	EventID   string
	AccountID string
	Segment   string
	Label     string // FRAUD or LEGIT
	Scenario  string
	RunID     string
	Amount    int64
	EventTime time.Time
}

// Outcome is one config's decision for an event.
type Outcome struct {
	Decision  rules.Decision
	Risk      int
	RuleScore int
	MLScore   *int
	MLStatus  string
	Fired     []string
	Shadow    []string
	Reasons   []string
}

// AccountAgg is the sufficient statistic for one account. Bootstrap resamples
// these rows.
type AccountAgg struct {
	Scored         int64 `json:"scored"`
	Fraud          int64 `json:"fraud"`
	Legit          int64 `json:"legit"`
	FraudAlert     int64 `json:"fraud_alert"`
	FraudInter     int64 `json:"fraud_inter"`
	FraudBlock     int64 `json:"fraud_block"`
	LegitAlert     int64 `json:"legit_alert"`
	LegitInter     int64 `json:"legit_inter"`
	Friction       int64 `json:"friction"`
	SavedAmount    int64 `json:"saved_amount"`
	MissedAmount   int64 `json:"missed_amount"`
	FrictionAmount int64 `json:"friction_amount"`
	Amount         int64 `json:"amount"`
}

func (a *AccountAgg) add(dst *AccountAgg) {
	dst.Scored += a.Scored
	dst.Fraud += a.Fraud
	dst.Legit += a.Legit
	dst.FraudAlert += a.FraudAlert
	dst.FraudInter += a.FraudInter
	dst.FraudBlock += a.FraudBlock
	dst.LegitAlert += a.LegitAlert
	dst.LegitInter += a.LegitInter
	dst.Friction += a.Friction
	dst.SavedAmount += a.SavedAmount
	dst.MissedAmount += a.MissedAmount
	dst.FrictionAmount += a.FrictionAmount
	dst.Amount += a.Amount
}

// DayAgg is one UTC day.
type DayAgg struct {
	Date       string `json:"date"`
	Scored     int64  `json:"scored"`
	Fraud      int64  `json:"fraud"`
	Legit      int64  `json:"legit"`
	FraudAlert int64  `json:"fraud_alert"`
	LegitAlert int64  `json:"legit_alert"`
	Inter      int64  `json:"interventions"`
}

// ScenarioAgg is one fraud scenario.
type ScenarioAgg struct {
	Scenario     string  `json:"scenario"`
	Fraud        int64   `json:"fraud"`
	Alerted      int64   `json:"alerted"`
	Intervened   int64   `json:"intervened"`
	CaughtAmount int64   `json:"caught_amount"`
	Runs         int64   `json:"runs"`
	Detected     int64   `json:"detected"`
	DetectMin    []int64 `json:"-"`
}

// SegmentAgg catches uneven customer impact.
type SegmentAgg struct {
	Segment    string `json:"segment"`
	Scored     int64  `json:"scored"`
	Legit      int64  `json:"legit"`
	LegitAlert int64  `json:"legit_alert"`
	Friction   int64  `json:"friction"`
}

// RuleAgg is per-rule and shadow-rule performance.
type RuleAgg struct {
	RuleID      string  `json:"rule_id"`
	Shadow      bool    `json:"shadow"`
	Fires       int64   `json:"fires"`
	TP          int64   `json:"tp"`
	FP          int64   `json:"fp"`
	Precision   float64 `json:"precision"`
	UniqueCatch int64   `json:"unique_catch"`
	WeightSum   int64   `json:"weight_sum"`
	Share       float64 `json:"share"`
}

// RunAgg tracks time-to-detect for one scenario run.
type RunAgg struct {
	RunID               string `json:"run_id"`
	Scenario            string `json:"scenario"`
	FirstFraudMS        int64  `json:"first_fraud_ms"`
	FirstInterventionMS int64  `json:"first_intervention_ms"`
	TotalFraudAmount    int64  `json:"total_fraud_amount"`
	LossBeforeDetect    int64  `json:"loss_before_detection"`
	FraudEvents         int64  `json:"fraud_events"`
}

// PairAgg is baseline/candidate joint statistics for one account.
type PairAgg struct {
	FraudBothAlert int64 `json:"fraud_both_alert"`
	FraudBaseOnly  int64 `json:"fraud_baseline_only"`
	FraudCandOnly  int64 `json:"fraud_candidate_only"`
	FraudNeither   int64 `json:"fraud_neither"`
	LegitBothAlert int64 `json:"legit_both_alert"`
	LegitBaseOnly  int64 `json:"legit_baseline_only"`
	LegitCandOnly  int64 `json:"legit_candidate_only"`
	LegitNeither   int64 `json:"legit_neither"`
}

func (p *PairAgg) add(dst *PairAgg) {
	dst.FraudBothAlert += p.FraudBothAlert
	dst.FraudBaseOnly += p.FraudBaseOnly
	dst.FraudCandOnly += p.FraudCandOnly
	dst.FraudNeither += p.FraudNeither
	dst.LegitBothAlert += p.LegitBothAlert
	dst.LegitBaseOnly += p.LegitBaseOnly
	dst.LegitCandOnly += p.LegitCandOnly
	dst.LegitNeither += p.LegitNeither
}

// Accumulator is one config over one dataset. All fields are exported so a
// checkpoint can be gob-encoded and resumed.
type Accumulator struct {
	Label             string `json:"label"`
	Scored            int64  `json:"scored"`
	Fraud             int64  `json:"fraud"`
	Legit             int64  `json:"legit"`
	Unlabelled        int64  `json:"unlabelled"`
	MLScored          int64  `json:"ml_scored"`
	MLDegraded        int64  `json:"ml_degraded"`
	SkippedLate       int64  `json:"skipped_late"`
	SkippedDup        int64  `json:"skipped_duplicate"`
	SkippedInvalid    int64  `json:"skipped_invalid"`
	SkippedNoSnapshot int64  `json:"skipped_no_snapshot"`

	Decisions map[string]int64        `json:"decisions"`
	Amounts   map[string]int64        `json:"amounts"` // label:decision
	Days      map[string]*DayAgg      `json:"days"`
	Scenarios map[string]*ScenarioAgg `json:"scenarios"`
	Segments  map[string]*SegmentAgg  `json:"segments"`
	Rules     map[string]*RuleAgg     `json:"rules"`
	Shadow    map[string]*RuleAgg     `json:"shadow"`
	Runs      map[string]*RunAgg      `json:"runs"`
	Accounts  map[string]*AccountAgg  `json:"accounts"`
	Risk      [2][101]int64           `json:"risk_histogram"`
	Overlap   map[string]int64        `json:"overlap"`

	FirstEventMS int64 `json:"first_event_ms"`
	LastEventMS  int64 `json:"last_event_ms"`
}

// NewAccumulator builds an empty accumulator for a config label.
func NewAccumulator(label string) *Accumulator {
	return &Accumulator{
		Label:     label,
		Decisions: map[string]int64{},
		Amounts:   map[string]int64{},
		Days:      map[string]*DayAgg{}, Scenarios: map[string]*ScenarioAgg{},
		Segments: map[string]*SegmentAgg{}, Rules: map[string]*RuleAgg{},
		Shadow: map[string]*RuleAgg{}, Runs: map[string]*RunAgg{},
		Accounts: map[string]*AccountAgg{}, Overlap: map[string]int64{},
	}
}

// Add folds one event/outcome into the accumulator.
func (a *Accumulator) Add(ev Event, o Outcome, combiner rules.Combiner) {
	a.Scored++
	if a.FirstEventMS == 0 || ev.EventTime.UnixMilli() < a.FirstEventMS {
		a.FirstEventMS = ev.EventTime.UnixMilli()
	}
	if ms := ev.EventTime.UnixMilli(); ms > a.LastEventMS {
		a.LastEventMS = ms
	}
	day := ev.EventTime.UTC().Format("2006-01-02")
	d := a.Days[day]
	if d == nil {
		d = &DayAgg{Date: day}
		a.Days[day] = d
	}
	d.Scored++
	a.Decisions[string(o.Decision)]++

	if o.MLStatus != "" && o.MLStatus != "NOT_CONFIGURED" {
		a.MLScored++
		if o.MLStatus != "OK" {
			a.MLDegraded++
		}
	}
	isFraud := ev.Label == "FRAUD"
	isLegit := ev.Label == "LEGIT"
	if isFraud {
		a.Fraud++
	} else if isLegit {
		a.Legit++
	} else {
		a.Unlabelled++
	}

	account := a.Accounts[ev.AccountID]
	if account == nil {
		account = &AccountAgg{}
		a.Accounts[ev.AccountID] = account
	}
	account.Scored++
	account.Amount += ev.Amount

	alerted := o.Decision.Severity() >= rules.StepUp.Severity()
	intervened := o.Decision.Severity() >= rules.Hold.Severity()
	blocked := o.Decision == rules.Block

	if isFraud {
		account.Fraud++
		if alerted {
			account.FraudAlert++
		}
		if intervened {
			account.FraudInter++
			account.SavedAmount += ev.Amount
			a.Amounts["FRAUD:INTERVENED"] += ev.Amount
		} else {
			account.MissedAmount += ev.Amount
			a.Amounts["FRAUD:MISSED"] += ev.Amount
		}
		if blocked {
			account.FraudBlock++
		}
	} else if isLegit {
		account.Legit++
		if alerted {
			account.LegitAlert++
			a.Amounts["LEGIT:ALERTED"] += ev.Amount
		}
		if intervened {
			account.LegitInter++
			account.FrictionAmount += ev.Amount
			a.Amounts["LEGIT:INTERVENED"] += ev.Amount
		}
	}
	if alerted {
		d.Inter++
		if isFraud {
			d.FraudAlert++
		} else if isLegit {
			d.LegitAlert++
		}
	}
	if isFraud {
		d.Fraud++
	} else if isLegit {
		d.Legit++
	}
	if alerted {
		account.Friction++
	}

	seg := a.Segments[ev.Segment]
	if seg == nil {
		seg = &SegmentAgg{Segment: ev.Segment}
		a.Segments[ev.Segment] = seg
	}
	seg.Scored++
	if isLegit {
		seg.Legit++
		if alerted {
			seg.LegitAlert++
		}
	}
	if intervened {
		seg.Friction++
	}

	bin := 0
	if o.Risk >= 0 && o.Risk <= 100 {
		bin = o.Risk
	}
	switch {
	case isFraud:
		a.Risk[0][bin]++
	case isLegit:
		a.Risk[1][bin]++
	}

	if isFraud {
		sc := a.Scenarios[ev.Scenario]
		if sc == nil {
			sc = &ScenarioAgg{Scenario: ev.Scenario}
			a.Scenarios[ev.Scenario] = sc
		}
		sc.Fraud++
		if alerted {
			sc.Alerted++
		}
		if intervened {
			sc.Intervened++
			sc.CaughtAmount += ev.Amount
		}
		if ev.RunID != "" {
			run := a.Runs[ev.RunID]
			if run == nil {
				run = &RunAgg{RunID: ev.RunID, Scenario: ev.Scenario}
				a.Runs[ev.RunID] = run
			}
			ms := ev.EventTime.UnixMilli()
			if run.FirstFraudMS == 0 || ms < run.FirstFraudMS {
				run.FirstFraudMS = ms
			}
			run.FraudEvents++
			run.TotalFraudAmount += ev.Amount
			if intervened {
				if run.FirstInterventionMS == 0 || ms < run.FirstInterventionMS {
					run.FirstInterventionMS = ms
				}
			} else if run.FirstInterventionMS == 0 || ms < run.FirstInterventionMS {
				run.LossBeforeDetect += ev.Amount
			}
		}
	}

	// Per-rule statistics. Unique catch means this was the only fired rule on a
	// fraud event; it is a conservative lower bound on rule contribution.
	for _, id := range o.Fired {
		r := a.Rules[id]
		if r == nil {
			r = &RuleAgg{RuleID: id}
			a.Rules[id] = r
		}
		r.Fires++
		if isFraud {
			r.TP++
			if len(o.Fired) == 1 {
				r.UniqueCatch++
			}
		} else if isLegit {
			r.FP++
		}
	}
	for i, id := range o.Fired {
		for j := i + 1; j < len(o.Fired); j++ {
			key := id + "|" + o.Fired[j]
			if id > o.Fired[j] {
				key = o.Fired[j] + "|" + id
			}
			a.Overlap[key]++
		}
	}
	for _, id := range o.Shadow {
		r := a.Shadow[id]
		if r == nil {
			r = &RuleAgg{RuleID: id, Shadow: true}
			a.Shadow[id] = r
		}
		r.Fires++
		if isFraud {
			r.TP++
		} else if isLegit {
			r.FP++
		}
	}
	for _, r := range a.Rules {
		if r.Fires > 0 {
			r.Precision = float64(r.TP) / float64(r.Fires)
			r.Share = float64(r.Fires) / float64(a.Scored)
		}
	}
	for _, r := range a.Shadow {
		if r.Fires > 0 {
			r.Precision = float64(r.TP) / float64(r.Fires)
			r.Share = float64(r.Fires) / float64(a.Scored)
		}
	}
	_ = combiner
}

// PairAccumulator holds baseline/candidate joint stats per account.
type PairAccumulator struct {
	Accounts map[string]*PairAgg `json:"accounts"`
}

// NewPairAccumulator builds an empty pair accumulator.
func NewPairAccumulator() *PairAccumulator {
	return &PairAccumulator{Accounts: map[string]*PairAgg{}}
}

// Add folds one event's two outcomes.
func (p *PairAccumulator) Add(ev Event, baseline, candidate Outcome) {
	pa := p.Accounts[ev.AccountID]
	if pa == nil {
		pa = &PairAgg{}
		p.Accounts[ev.AccountID] = pa
	}
	ba := baseline.Decision.Severity() >= rules.StepUp.Severity()
	ca := candidate.Decision.Severity() >= rules.StepUp.Severity()
	switch ev.Label {
	case "FRAUD":
		switch {
		case ba && ca:
			pa.FraudBothAlert++
		case ba:
			pa.FraudBaseOnly++
		case ca:
			pa.FraudCandOnly++
		default:
			pa.FraudNeither++
		}
	case "LEGIT":
		switch {
		case ba && ca:
			pa.LegitBothAlert++
		case ba:
			pa.LegitBaseOnly++
		case ca:
			pa.LegitCandOnly++
		default:
			pa.LegitNeither++
		}
	}
}

// Ratio returns numerator/denominator, 0 for empty denominators.
func Ratio(num, den int64) float64 {
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den)
}

// Bootstrap computes 95% CIs for the headline metrics by resampling accounts.
func (a *Accumulator) Bootstrap(seed int64, resamples, workers int) map[string]Estimate {
	if resamples <= 0 {
		resamples = 1000
	}
	if workers <= 0 {
		workers = 4
	}
	accounts := make([]*AccountAgg, 0, len(a.Accounts))
	ids := make([]string, 0, len(a.Accounts))
	for id := range a.Accounts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		accounts = append(accounts, a.Accounts[id])
	}
	names := []string{"fraud_caught_rate", "fraud_intervened_rate", "fraud_blocked_rate",
		"false_positive_rate", "legit_intervention_rate", "customer_friction",
		"precision_alerted", "precision_intervened", "f2_alerted"}
	points := a.metricMap(accounts)
	out := make(map[string]Estimate, len(names))
	for _, n := range names {
		out[n] = Estimate{Point: points[n], N: a.Scored}
	}
	if len(accounts) == 0 {
		return out
	}
	type sample struct {
		vals [9]float64
	}
	ch := make(chan sample, resamples)
	var wg sync.WaitGroup
	per := resamples / workers
	if per == 0 {
		per = 1
	}
	for w := 0; w < workers; w++ {
		start := w * per
		end := start + per
		if w == workers-1 || end > resamples {
			end = resamples
		}
		if start >= end {
			continue
		}
		rng := rand.New(rand.NewSource(seed + int64(w)*7919))
		wg.Add(1)
		go func(rng *rand.Rand, n int) {
			defer wg.Done()
			var acc AccountAgg
			for k := 0; k < n; k++ {
				acc = AccountAgg{}
				for i := 0; i < len(accounts); i++ {
					accounts[rng.Intn(len(accounts))].add(&acc)
				}
				m := metricMapFromAgg(&acc)
				var vals [9]float64
				for i, name := range names {
					vals[i] = m[name]
				}
				ch <- sample{vals: vals}
			}
		}(rng, end-start)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	all := make([][9]float64, 0, resamples)
	for s := range ch {
		all = append(all, s.vals)
	}
	for i, name := range names {
		col := make([]float64, len(all))
		for j := range all {
			col[j] = all[j][i]
		}
		sort.Float64s(col)
		e := out[name]
		e.Lo = quantile(col, 0.025)
		e.Hi = quantile(col, 0.975)
		out[name] = e
	}
	return out
}

func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	pos := q * float64(len(sorted)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	return sorted[lo] + (sorted[hi]-sorted[lo])*(pos-float64(lo))
}

func (a *Accumulator) metricMap(accounts []*AccountAgg) map[string]float64 {
	var acc AccountAgg
	for _, x := range accounts {
		x.add(&acc)
	}
	return metricMapFromAgg(&acc)
}

func metricMapFromAgg(a *AccountAgg) map[string]float64 {
	alertedFraud := a.FraudAlert
	precisionDen := a.FraudAlert + a.LegitAlert
	p := Ratio(a.FraudAlert, precisionDen)
	interDen := a.FraudInter + a.LegitInter
	pi := Ratio(a.FraudInter, interDen)
	f2 := 0.0
	recall := Ratio(a.FraudAlert, a.Fraud)
	if p+recall > 0 {
		f2 = 5 * p * recall / (4*p + recall)
	}
	return map[string]float64{
		"fraud_caught_rate":       Ratio(a.FraudAlert, a.Fraud),
		"fraud_intervened_rate":   Ratio(a.FraudInter, a.Fraud),
		"fraud_blocked_rate":      Ratio(a.FraudBlock, a.Fraud),
		"false_positive_rate":     Ratio(a.LegitAlert, a.Legit),
		"legit_intervention_rate": Ratio(a.LegitInter, a.Legit),
		"customer_friction":       Ratio(a.Friction, a.Scored),
		"precision_alerted":       p,
		"precision_intervened":    pi,
		"f2_alerted":              f2,
		"alerted_fraud":           float64(alertedFraud),
	}
}
