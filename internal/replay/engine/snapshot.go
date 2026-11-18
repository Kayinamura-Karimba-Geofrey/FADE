package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/dataset"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/metrics"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/report"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/scoring"
)

// snapshotRow is one stored decision joined with its label.
type snapshotRow struct {
	EventID      string
	AccountID    string
	Amount       int64
	EventTime    time.Time
	Features     []byte
	InputEvent   []byte
	ModelVersion string
	MLScore      *int
}

func (e *Engine) runSnapshot(ctx context.Context, ds *dataset.Dataset, opts Options) (*Result, error) {
	if e.PG == nil {
		return nil, fmt.Errorf("replay: snapshot mode requires Postgres")
	}
	started := e.now()
	runners, err := e.buildRunners(ctx, opts, nil)
	if err != nil {
		return nil, err
	}
	accs := map[string]*metrics.Accumulator{"baseline": metrics.NewAccumulator("baseline")}
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
	xw, err := newWriter[deltaRow](deltaPath)
	if err != nil {
		return nil, err
	}
	scorer := scoring.NewBatchClient(e.ScorerURL, 60*time.Second)
	seen := map[string]bool{}
	var scored int64
	from := opts.From
	if from.IsZero() {
		from = time.Unix(0, 0)
	}
	to := opts.To
	if to.IsZero() {
		to = e.now().Add(24 * time.Hour)
	}
	rows, err := e.PG.Query(ctx,
		`SELECT d.event_id, d.account_id, d.amount, d.event_time, d.features,
		        d.input_event, COALESCE(d.model_version,''), d.ml_score
		   FROM decisions d
		  WHERE d.decided_at >= $1 AND d.decided_at < $2
		  ORDER BY d.decided_at, d.event_id`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var row snapshotRow
		if err := rows.Scan(&row.EventID, &row.AccountID, &row.Amount, &row.EventTime,
			&row.Features, &row.InputEvent, &row.ModelVersion, &row.MLScore); err != nil {
			return nil, err
		}
		if seen[row.EventID] || len(row.InputEvent) == 0 || len(row.Features) == 0 {
			accs["baseline"].SkippedNoSnapshot++
			continue
		}
		seen[row.EventID] = true
		ev, verr := events.Parse(row.InputEvent, time.Time{})
		if verr != nil {
			accs["baseline"].SkippedInvalid++
			continue
		}
		var vector features.FeatureVector
		if err := json.Unmarshal(row.Features, &vector); err != nil {
			accs["baseline"].SkippedInvalid++
			continue
		}
		label := ds.Label(ev.EventID)
		mev := metrics.Event{
			EventID: ev.EventID, AccountID: ev.Sender.AccountID,
			Segment: ds.Segment(ev.Sender.AccountID), Label: label.Label,
			Scenario: label.Scenario, RunID: label.ScenarioRun,
			Amount: ev.Amount, EventTime: row.EventTime.UTC(),
		}
		outcomes := map[string]metrics.Outcome{}
		var baseOutcome metrics.Outcome
		for _, r := range runners {
			ml := rules.MLResult{Status: rules.MLNotConfigured}
			switch {
			case r.resolved.Model == "" || r.resolved.Model == "none":
			case r.resolved.Model == row.ModelVersion && row.MLScore != nil:
				ml = rules.MLResult{Status: rules.MLOK, Score: row.MLScore, MLScore: row.MLScore,
					ModelVersion: row.ModelVersion}
			case scorer.Available():
				vecs := []*features.FeatureVector{&vector}
				res, serr := scorer.ScoreBatch(ctx, vecs, r.resolved.Model, false)
				if serr != nil {
					if !opts.AllowMLDegraded {
						return nil, fmt.Errorf("replay: snapshot scoring %s: %w", r.resolved.Model, serr)
					}
					ml = rules.MLResult{Status: rules.MLDegraded, ModelVersion: r.resolved.Model}
				} else {
					ml = res[0]
				}
			}
			dec := r.pipe.DecideWithFeatures(ctx, ev, vector, pipeline.Options{MLOverride: &ml})
			out := outcomeOf(dec)
			outcomes[r.label] = out
			if r.label == "baseline" {
				baseOutcome = out
			}
			accs[r.label].Add(mev, out, r.resolved.Combiner)
			if err := dw.Write(decisionRow{
				ConfigLabel: r.label, EventID: ev.EventID, AccountID: ev.Sender.AccountID,
				Label: label.Label, Scenario: label.Scenario, Segment: mev.Segment,
				Decision: string(out.Decision), Risk: int64(out.Risk), RuleScore: int64(out.RuleScore),
				MLScore: mlScore(out.MLScore), FiredRules: join(out.Fired),
				ShadowRules: join(out.Shadow), Reasons: join(out.Reasons),
				Amount: ev.Amount, EventTimeMS: ev.EventTimeMS,
			}); err != nil {
				return nil, err
			}
		}
		recordDelta(mev, baseOutcome, outcomes, pairs, transitions, changes)
		scored++
	}
	if err := rows.Err(); err != nil {
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
		if label != "baseline" {
			acc.SkippedLate = accs["baseline"].SkippedLate
			acc.SkippedDup = accs["baseline"].SkippedDup
			acc.SkippedInvalid = accs["baseline"].SkippedInvalid
			acc.SkippedNoSnapshot = accs["baseline"].SkippedNoSnapshot
		}
		results[label] = acc.Finalize(opts.Seed)
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
	rep := report.Build(report.Input{
		JobID: opts.JobID,
		Dataset: report.DatasetInfo{Name: ds.Name, Hash: ds.Hash, Events: int64(ds.Events), Scored: scored,
			WarmupDays: opts.WarmupDays, MetricFrom: opts.MetricFrom.UTC().Format(time.RFC3339)},
		Mode: string(opts.Mode), StatePolicy: opts.StatePolicy, Seed: opts.Seed,
		EngineVersion: opts.EngineVersion, Configs: configs, Results: results,
		Pairs: pairs, Transitions: transitions, Changes: changes, Cost: cost,
		ExtraCaveats: []string{"Snapshot mode uses stored input events and features; rows without either are counted as skipped."},
	})
	applyCost(rep, &cost)
	raw, err := rep.JSON()
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(opts.ArtifactDir, "report.json"), raw, 0o640); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(opts.ArtifactDir, "report.md"), []byte(rep.Markdown()), 0o640); err != nil {
		return nil, err
	}
	elapsed := e.now().Sub(started)
	return &Result{Report: rep, Decisions: decPath, Deltas: deltaPath, Scored: scored,
		Processed: scored, Elapsed: elapsed, EventsPerSec: float64(scored) / elapsed.Seconds()}, nil
}

func join(v []string) string {
	out := ""
	for i, s := range v {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}

var _ = pgx.ErrNoRows
