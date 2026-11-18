// Package engine runs one replay job: it computes features once per event in an
// isolated namespace, evaluates every config with the production pipeline, and
// writes deterministic metrics and per-decision artefacts.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parquet-go/parquet-go"
	"github.com/redis/go-redis/v9"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
	replayconfig "github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/dataset"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/isolation"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/metrics"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/report"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/scoring"
)

// Mode is the replay input mode.
type Mode string

const (
	ModeFull     Mode = "full"
	ModeSnapshot Mode = "snapshot"
	ModeStoredML Mode = "stored-ml"
)

// Options is one job.
type Options struct {
	JobID           int64
	Mode            Mode
	DatasetDir      string
	StatePolicy     string
	Seed            int64
	Workers         int
	WarmupDays      int
	From            time.Time
	To              time.Time
	Scenario        string
	SamplePct       float64
	AllowMLDegraded bool
	WithExplain     bool
	Ablate          bool
	CacheFeatures   bool
	KeepNamespace   bool
	ArtifactDir     string
	EngineVersion   string

	MetricFrom time.Time
	Baseline   *replayconfig.Resolved
	Candidates []*replayconfig.Resolved
}

// Progress is reported periodically.
type Progress struct {
	Processed int64
	Events    int64
	Elapsed   time.Duration
	PerSecond float64
	ETA       time.Duration
}

// Result is the engine output.
type Result struct {
	Report       *report.Report
	Decisions    string
	Deltas       string
	Scored       int64
	Processed    int64
	Elapsed      time.Duration
	EventsPerSec float64
	ResumeOffset int64
	ReportHash   string
}

// Engine owns the external collaborators.
type Engine struct {
	PG         *pgxpool.Pool
	Redis      *redis.Client
	ScorerURL  string
	FeatureCfg config.Features
	LateTol    time.Duration
	DedupeTTL  time.Duration
	Logger     *slog.Logger
	Metrics    *obs.Metrics
	Progress   func(Progress)
	Now        func() time.Time
}

// decisionRow is the per-decision parquet contract.
type decisionRow struct {
	ConfigLabel string `parquet:"config_label"`
	EventID     string `parquet:"event_id"`
	AccountID   string `parquet:"account_id"`
	Label       string `parquet:"label"`
	Scenario    string `parquet:"scenario"`
	Segment     string `parquet:"segment"`
	Decision    string `parquet:"decision"`
	Risk        int64  `parquet:"risk"`
	RuleScore   int64  `parquet:"rule_score"`
	MLScore     int64  `parquet:"ml_score"`
	FiredRules  string `parquet:"fired_rules"`
	ShadowRules string `parquet:"shadow_rules"`
	Reasons     string `parquet:"reasons"`
	Amount      int64  `parquet:"amount"`
	EventTimeMS int64  `parquet:"event_time_ms"`
}

// deltaRow is the per-changed-event parquet contract.
type deltaRow struct {
	CandidateLabel  string `parquet:"candidate_label"`
	EventID         string `parquet:"event_id"`
	Label           string `parquet:"label"`
	Scenario        string `parquet:"scenario"`
	Amount          int64  `parquet:"amount"`
	From            string `parquet:"from"`
	To              string `parquet:"to"`
	BaselineReason  string `parquet:"baseline_reason"`
	CandidateReason string `parquet:"candidate_reason"`
}

type configRunner struct {
	resolved *replayconfig.Resolved
	pipe     *pipeline.Pipeline
	label    string
}

// Run executes a job. It returns only after the report and parquet artefacts
// are on disk.
func (e *Engine) Run(ctx context.Context, opts Options) (*Result, error) {
	started := e.now()
	if opts.JobID <= 0 {
		return nil, fmt.Errorf("replay: job id required")
	}
	if opts.Mode == "" {
		opts.Mode = ModeFull
	}
	if opts.StatePolicy == "" {
		opts.StatePolicy = "ignore"
	}
	if opts.StatePolicy != "ignore" {
		return nil, fmt.Errorf("replay: state policy %q is not supported; only ignore is implemented", opts.StatePolicy)
	}
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.ArtifactDir == "" {
		opts.ArtifactDir = filepath.Join("replay", "jobs", fmt.Sprint(opts.JobID))
	}
	if err := os.MkdirAll(opts.ArtifactDir, 0o750); err != nil {
		return nil, fmt.Errorf("replay: create artifact dir: %w", err)
	}
	if opts.DatasetDir == "" {
		return nil, fmt.Errorf("replay: dataset directory required")
	}
	ds, err := dataset.Load(opts.DatasetDir)
	if err != nil {
		return nil, err
	}
	return e.runDataset(ctx, ds, opts, started)
}

// RunDataset is Run with an already opened dataset (used by tests and the CLI
// when a dataset path is explicit).
func (e *Engine) RunDataset(ctx context.Context, ds *dataset.Dataset, opts Options) (*Result, error) {
	return e.runDataset(ctx, ds, opts, e.now())
}

func (e *Engine) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

func (e *Engine) runDataset(ctx context.Context, ds *dataset.Dataset, opts Options, started time.Time) (*Result, error) {
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.Mode == "" {
		opts.Mode = ModeFull
	}
	if opts.Mode == ModeSnapshot || opts.Mode == ModeStoredML {
		return e.runSnapshot(ctx, ds, opts)
	}
	prefix := isolation.Prefix(opts.JobID)
	if err := isolation.Reserve(ctx, e.Redis, prefix, 48*time.Hour); err != nil {
		return nil, err
	}
	if !opts.KeepNamespace {
		defer isolation.Release(context.WithoutCancel(ctx), e.Redis, prefix, false)
	}

	fs := features.NewStoreWithNamespace(e.Redis, e.FeatureCfg, e.LateTol, e.DedupeTTL, features.Hooks{}, prefix)
	runners, err := e.buildRunners(ctx, opts, fs)
	if err != nil {
		return nil, err
	}
	accs := map[string]*metrics.Accumulator{
		"baseline": metrics.NewAccumulator("baseline"),
	}
	pairs := map[string]*metrics.PairAccumulator{}
	transitions := map[string]map[[2]rules.Decision]int64{}
	changes := map[string][]report.Change{}
	for _, c := range opts.Candidates {
		accs[c.Label] = metrics.NewAccumulator(c.Label)
		pairs[c.Label] = metrics.NewPairAccumulator()
		transitions[c.Label] = map[[2]rules.Decision]int64{}
	}

	decPath := filepath.Join(opts.ArtifactDir, "decisions.parquet")
	deltaPath := filepath.Join(opts.ArtifactDir, "deltas.parquet")
	dw, err := newWriter[decisionRow](decPath)
	if err != nil {
		return nil, err
	}
	defer dw.Close()
	xw, err := newWriter[deltaRow](deltaPath)
	if err != nil {
		return nil, err
	}
	defer xw.Close()

	scorer := scoring.NewBatchClient(e.ScorerURL, 60*time.Second)
	modelVersions := map[string]bool{}
	for _, r := range runners {
		if r.resolved.Model != "" && r.resolved.Model != "none" {
			modelVersions[r.resolved.Model] = true
		}
	}

	// Metrics start after warm-up. The first in-scope event anchors the clock.
	var firstMS int64
	var processed, scored int64
	var buffer []*pending
	const batchSize = 200
	flush := func() error {
		if len(buffer) == 0 {
			return nil
		}
		mlByVersion := map[string][]rules.MLResult{}
		if len(modelVersions) > 0 {
			vectors := make([]*features.FeatureVector, len(buffer))
			for i, p := range buffer {
				vectors[i] = &p.vector
			}
			versions := make([]string, 0, len(modelVersions))
			for v := range modelVersions {
				versions = append(versions, v)
			}
			sort.Strings(versions)
			// Every model version scores the same vectors, so versions are
			// independent and can run concurrently without changing results.
			type scoredVersion struct {
				version string
				results []rules.MLResult
				err     error
			}
			scoredCh := make(chan scoredVersion, len(versions))
			for _, version := range versions {
				go func(version string) {
					mlStart := e.now()
					res, serr := scorer.ScoreBatch(ctx, vectors, version, opts.WithExplain)
					if e.Metrics != nil {
						result := "ok"
						if serr != nil {
							result = "error"
						}
						e.Metrics.ReplayMLBatch.WithLabelValues(result).Observe(e.now().Sub(mlStart).Seconds())
					}
					scoredCh <- scoredVersion{version: version, results: res, err: serr}
				}(version)
			}
			for range versions {
				sv := <-scoredCh
				res := sv.results
				if sv.err != nil {
					if !opts.AllowMLDegraded {
						return fmt.Errorf("replay: scoring model %s: %w", sv.version, sv.err)
					}
					res = make([]rules.MLResult, len(buffer))
					for i := range res {
						res[i] = rules.MLResult{Status: rules.MLDegraded, ModelVersion: sv.version}
					}
				}
				mlByVersion[sv.version] = res
			}
		}

		// Prepare the per-event metrics record sequentially, then evaluate each
		// config in its own worker. A config's accumulator is only ever touched
		// by that config's worker and in buffer order, so the report is
		// independent of worker count.
		mevs := make([]metrics.Event, len(buffer))
		inMetrics := make([]bool, len(buffer))
		for i, p := range buffer {
			ev := p.ev
			inMetrics[i] = opts.MetricFrom.IsZero() || ev.EventTimeMS >= opts.MetricFrom.UnixMilli()
			label := ds.Label(ev.EventID)
			mevs[i] = metrics.Event{
				EventID: ev.EventID, AccountID: ev.Sender.AccountID,
				Segment: ds.Segment(ev.Sender.AccountID), Label: label.Label,
				Scenario: label.Scenario, RunID: label.ScenarioRun,
				Amount: ev.Amount, EventTime: time.UnixMilli(ev.EventTimeMS).UTC(),
			}
		}
		outs := make([][]metrics.Outcome, len(runners))
		rows := make([][]decisionRow, len(runners))
		sem := make(chan struct{}, opts.Workers)
		var wg sync.WaitGroup
		for i, r := range runners {
			wg.Add(1)
			go func(i int, r *configRunner) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				outs[i] = make([]metrics.Outcome, len(buffer))
				rows[i] = make([]decisionRow, len(buffer))
				for j, p := range buffer {
					ml := rules.MLResult{Status: rules.MLNotConfigured}
					if r.resolved.Model != "" && r.resolved.Model != "none" {
						if results := mlByVersion[r.resolved.Model]; j < len(results) {
							ml = results[j]
						}
					}
					dec := r.pipe.DecideWithFeatures(ctx, p.ev, p.vector, pipeline.Options{
						IncludeFeatures: true, MLOverride: &ml,
					})
					out := outcomeOf(dec)
					outs[i][j] = out
					if inMetrics[j] {
						accs[r.label].Add(mevs[j], out, r.resolved.Combiner)
					}
					rows[i][j] = decisionRow{
						ConfigLabel: r.label, EventID: p.ev.EventID, AccountID: p.ev.Sender.AccountID,
						Label: mevs[j].Label, Scenario: mevs[j].Scenario, Segment: mevs[j].Segment,
						Decision: string(out.Decision), Risk: int64(out.Risk), RuleScore: int64(out.RuleScore),
						MLScore: mlScore(out.MLScore), FiredRules: strings.Join(out.Fired, ","),
						ShadowRules: strings.Join(out.Shadow, ","), Reasons: strings.Join(out.Reasons, "; "),
						Amount: p.ev.Amount, EventTimeMS: p.ev.EventTimeMS,
					}
				}
			}(i, r)
		}
		wg.Wait()

		// Merge in event-major order: the parquet bytes do not depend on the
		// worker that produced a row.
		for j, p := range buffer {
			p.outcomes = make(map[string]metrics.Outcome, len(runners))
			var baseOutcome metrics.Outcome
			for i, r := range runners {
				p.outcomes[r.label] = outs[i][j]
				if r.label == "baseline" {
					baseOutcome = outs[i][j]
				}
				if err := dw.Write(rows[i][j]); err != nil {
					return err
				}
			}
			if inMetrics[j] {
				recordDelta(mevs[j], baseOutcome, p.outcomes, pairs, transitions, changes)
			}
			scored++
			buffer[j] = nil
		}
		buffer = buffer[:0]
		return nil
	}

	// The engine processes features in global arrival order; config evaluation
	// happens in the batch flush, so worker count cannot reorder state.
	var lastProgress time.Time
	err = ds.Stream(ctx, func(ev *events.Event) error {
		if !ds.InScope(ev, dataset.Options{From: opts.From, To: opts.To, SamplePct: opts.SamplePct, Scenario: opts.Scenario}) {
			return nil
		}
		if firstMS == 0 {
			firstMS = ev.EventTimeMS
			if opts.MetricFrom.IsZero() && opts.WarmupDays > 0 {
				opts.MetricFrom = time.UnixMilli(firstMS).AddDate(0, 0, opts.WarmupDays)
			}
		}
		processed++
		vector, status, ferr := fs.Compute(ctx, ev)
		if ferr != nil {
			return ferr
		}
		switch status {
		case features.StatusDuplicate:
			accs["baseline"].SkippedDup++
			return nil
		case features.StatusLate:
			accs["baseline"].SkippedLate++
			return nil
		case features.StatusOK:
		default:
			accs["baseline"].SkippedInvalid++
			return nil
		}
		// Evaluate now so outcomes are available for deltas; ML is applied in
		// the flush pass, where all configs see the same scored vector.
		p := &pending{ev: ev, vector: vector}
		buffer = append(buffer, p)
		if len(buffer) >= batchSize {
			return flush()
		}
		if e.Progress != nil && e.now().Sub(lastProgress) > 2*time.Second {
			lastProgress = e.now()
			elapsed := e.now().Sub(started)
			per := float64(processed) / elapsed.Seconds()
			e.Progress(Progress{Processed: processed, Events: int64(ds.Events), Elapsed: elapsed, PerSecond: per})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if err := dw.Close(); err != nil {
		return nil, err
	}
	if err := xw.Close(); err != nil {
		return nil, err
	}

	results := map[string]metrics.Result{}
	for label, acc := range accs {
		if label == "baseline" {
			// Skipped counters are shared; copy them onto every config.
			continue
		}
		acc.SkippedLate = accs["baseline"].SkippedLate
		acc.SkippedDup = accs["baseline"].SkippedDup
		acc.SkippedInvalid = accs["baseline"].SkippedInvalid
	}
	for label, acc := range accs {
		results[label] = acc.Finalize(opts.Seed)
	}
	if d, err := os.ReadFile(decPath); err == nil {
		sum := sha256.Sum256(d)
		_ = os.WriteFile(filepath.Join(opts.ArtifactDir, "decisions.sha256"), []byte(hex.EncodeToString(sum[:])), 0o640)
	}

	configs := make([]report.ConfigInfo, 0, len(runners))
	for _, r := range runners {
		configs = append(configs, report.ConfigInfo{
			Label: r.label, Rules: r.resolved.RulesRef, RulesHash: r.resolved.RuleSet.Hash,
			Model: r.resolved.Model, ModelHash: r.resolved.ModelHash,
			Combiner: r.resolved.Combiner, ConfigHash: r.resolved.ConfigHash,
		})
	}
	cost := loadCost()
	in := report.Input{
		JobID: opts.JobID, Dataset: report.DatasetInfo{
			Name: ds.Name, Hash: ds.Hash, Events: int64(ds.Events), Scored: scored,
			WarmupDays: opts.WarmupDays, MetricFrom: opts.MetricFrom.UTC().Format(time.RFC3339),
		},
		Mode: string(opts.Mode), StatePolicy: opts.StatePolicy, Seed: opts.Seed,
		EngineVersion: opts.EngineVersion, Configs: configs, Results: results,
		Pairs: pairs, Transitions: transitions, Changes: changes, Cost: cost,
	}
	if !opts.From.IsZero() {
		in.Dataset.From = opts.From.UTC().Format(time.RFC3339)
	}
	if !opts.To.IsZero() {
		in.Dataset.To = opts.To.UTC().Format(time.RFC3339)
	}
	rep := report.Build(in)
	applyCost(rep, &cost)
	reportJSON, err := rep.JSON()
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(opts.ArtifactDir, "report.json"), reportJSON, 0o640); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(opts.ArtifactDir, "report.md"), []byte(rep.Markdown()), 0o640); err != nil {
		return nil, err
	}
	for _, r := range runners {
		file := r.resolved.RuleSet.File()
		raw, _ := json.MarshalIndent(file, "", "  ")
		_ = os.WriteFile(filepath.Join(opts.ArtifactDir, "config-"+r.label+".json"), raw, 0o640)
	}
	elapsed := e.now().Sub(started)
	if e.Metrics != nil {
		e.Metrics.ReplayEvents.WithLabelValues(string(opts.Mode)).Add(float64(processed))
		e.Metrics.ReplayThroughput.Set(float64(processed) / elapsed.Seconds())
		e.Metrics.ReplayDuration.Observe(elapsed.Seconds())
	}
	return &Result{
		Report: rep, Decisions: decPath, Deltas: deltaPath, Scored: scored,
		Processed: processed, Elapsed: elapsed,
		EventsPerSec: float64(processed) / elapsed.Seconds(),
	}, nil
}

// pending is one event plus its computed features.
type pending struct {
	ev       *events.Event
	vector   features.FeatureVector
	outcomes map[string]metrics.Outcome
}

func outcomeOf(d *pipeline.Decision) metrics.Outcome {
	if d == nil {
		return metrics.Outcome{Decision: rules.Allow}
	}
	out := metrics.Outcome{
		Decision: d.Decision, Risk: d.RiskScore, RuleScore: d.RuleScore,
		MLScore: d.MLScore, MLStatus: d.MLStatus,
	}
	for _, r := range d.Reasons {
		if r.Source == "rule" {
			out.Fired = append(out.Fired, r.ID)
			desc := r.ID
			if r.Weight != 0 {
				desc = fmt.Sprintf("%s(%d)", r.ID, r.Weight)
			}
			out.Reasons = append(out.Reasons, desc)
		}
	}
	out.Shadow = append(out.Shadow, d.ShadowHits...)
	return out
}

func mlScore(v *int) int64 {
	if v == nil {
		return -1
	}
	return int64(*v)
}

func recordDelta(ev metrics.Event, base metrics.Outcome, outcomes map[string]metrics.Outcome, pairs map[string]*metrics.PairAccumulator, transitions map[string]map[[2]rules.Decision]int64, changes map[string][]report.Change) {
	for label, cand := range outcomes {
		pair := pairs[label]
		if pair != nil {
			pair.Add(ev, base, cand)
		}
		if transitions[label] != nil && base.Decision != cand.Decision {
			transitions[label][[2]rules.Decision{base.Decision, cand.Decision}]++
			ch := report.Change{
				EventID: ev.EventID, Label: ev.Label, Scenario: ev.Scenario,
				Amount: ev.Amount, From: string(base.Decision), To: string(cand.Decision),
				BaselineReason:  strings.Join(base.Reasons, "; "),
				CandidateReason: strings.Join(cand.Reasons, "; "),
			}
			changes[label] = appendTop(changes[label], ch, label, ev.Label)
		}
	}
}

// appendTop keeps at most 200 changes per candidate, split by label, highest
// amounts first. The report trims to 50.
func appendTop(list []report.Change, ch report.Change, label, eventLabel string) []report.Change {
	same := 0
	for _, x := range list {
		if x.Label == eventLabel {
			same++
		}
	}
	if same >= 200 {
		// Replace the smallest same-label change if this one is larger.
		min := -1
		for i, x := range list {
			if x.Label != eventLabel {
				continue
			}
			if min < 0 || x.Amount < list[min].Amount {
				min = i
			}
		}
		if min >= 0 && list[min].Amount < ch.Amount {
			list[min] = ch
		}
		return list
	}
	return append(list, ch)
}

// buildRunners creates one production pipeline per config with a static rule
// manager and a frozen list snapshot.
func (e *Engine) buildRunners(ctx context.Context, opts Options, fs pipeline.FeatureComputer) ([]*configRunner, error) {
	lists, err := e.listSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	all := append([]*replayconfig.Resolved{opts.Baseline}, opts.Candidates...)
	out := make([]*configRunner, 0, len(all))
	for i, r := range all {
		if r == nil {
			return nil, fmt.Errorf("replay: config %d is nil", i)
		}
		label := "baseline"
		if i > 0 {
			label = r.Label
		}
		pipe := pipeline.New(pipeline.Deps{
			Rules: rules.NewStatic(r.RuleSet), Features: fs, Scorer: nil,
			Lists: lists, Sink: pipeline.DiscardSink{}, Metrics: obs.NewMetrics("replay"),
			Logger: e.Logger, Config: pipeline.DefaultConfig(),
		})
		out = append(out, &configRunner{resolved: r, pipe: pipe, label: label})
	}
	return out, nil
}

func (e *Engine) listSnapshot(ctx context.Context) (*pipeline.ListCache, error) {
	src := pipeline.NewMemoryListSource()
	if e.PG != nil {
		rows, err := e.PG.Query(ctx,
			`SELECT id, list_type, subject, value, COALESCE(reason,''), expires_at
			   FROM list_entries ORDER BY id`)
		if err != nil {
			return nil, fmt.Errorf("replay: list snapshot: %w", err)
		}
		entries := []pipeline.ListEntry{}
		for rows.Next() {
			var x pipeline.ListEntry
			var expires *time.Time
			if err := rows.Scan(&x.ID, &x.ListType, &x.Subject, &x.Value, &x.Reason, &expires); err != nil {
				rows.Close()
				return nil, err
			}
			x.ExpiresAt = expires
			entries = append(entries, x)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		src = pipeline.NewMemoryListSourceFrom(entries)
	}
	cache := pipeline.NewListCache(src, e.Logger)
	if err := cache.Refresh(ctx); err != nil {
		return nil, err
	}
	return cache, nil
}

type parquetWriter[T any] struct {
	f *os.File
	w *parquet.GenericWriter[T]
}

func newWriter[T any](path string) (*parquetWriter[T], error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return nil, err
	}
	w := parquet.NewGenericWriter[T](f)
	return &parquetWriter[T]{f: f, w: w}, nil
}

func (p *parquetWriter[T]) Write(v T) error {
	_, err := p.w.Write([]T{v})
	return err
}

func (p *parquetWriter[T]) Close() error {
	if p.w != nil {
		if err := p.w.Close(); err != nil {
			p.f.Close()
			return err
		}
		p.w = nil
	}
	return p.f.Close()
}

func loadCost() report.CostModel {
	c := report.CostModel{
		Currency: "RWF", AnalystCostPerCase: 1200, StepUpDropoff: 0.08,
		AverageFee: 250, RecoveryFactor: 0.35,
	}
	raw, err := os.ReadFile(filepath.Join("config", "replay_cost.yaml"))
	if err != nil {
		return c
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		var f float64
		switch k {
		case "currency":
			c.Currency = v
		case "analyst_cost_per_case":
			fmt.Sscanf(v, "%f", &f)
			c.AnalystCostPerCase = f
		case "step_up_dropoff_probability":
			fmt.Sscanf(v, "%f", &f)
			c.StepUpDropoff = f
		case "average_fee":
			fmt.Sscanf(v, "%f", &f)
			c.AverageFee = f
		case "fraud_recovery_factor":
			fmt.Sscanf(v, "%f", &f)
			c.RecoveryFactor = f
		}
	}
	return c
}

func applyCost(rep *report.Report, cost *report.CostModel) {
	rep.CostModel = *cost
	rep.CostModel.NetBenefitCandidates = map[string]float64{}
	for label, m := range rep.Summary {
		inter := float64(m.Workload.Interventions)
		stepUps := float64(m.Decisions[string(rules.StepUp)])
		benefit := float64(m.Amounts["FRAUD:INTERVENED"])*cost.RecoveryFactor -
			inter*cost.AnalystCostPerCase -
			stepUps*cost.StepUpDropoff*cost.AverageFee
		if label == "baseline" {
			rep.CostModel.NetBenefitBaseline = benefit
		} else {
			rep.CostModel.NetBenefitCandidates[label] = benefit
		}
	}
}
