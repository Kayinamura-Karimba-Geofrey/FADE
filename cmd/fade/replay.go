package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
	replayconfig "github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/dataset"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/engine"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/gate"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/report"
)

func cmdReplay(args []string) error {
	if len(args) == 0 {
		return errors.New("replay subcommand required: run, show, compare, list, tune")
	}
	switch args[0] {
	case "run":
		return replayRun(args[1:])
	case "show":
		return replayShow(args[1:])
	case "compare":
		return replayCompare(args[1:])
	case "list":
		return replayList(args[1:])
	case "tune":
		return replayTune(args[1:])
	case "sensitivity":
		return errors.New("sensitivity requires a cached feature table; this build supports full replay and threshold tuning, see docs/replay.md")
	default:
		return fmt.Errorf("unknown replay subcommand %q", args[0])
	}
}

func redisClient() (*redis.Client, error) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis %s: %w", addr, err)
	}
	return c, nil
}

func replayRun(args []string) error {
	fs := flag.NewFlagSet("replay run", flag.ContinueOnError)
	datasetDir := fs.String("dataset", "", "dataset directory (e.g. data/run42)")
	baseline := fs.String("baseline", "", "baseline config reference (prod@14 or file:rules/prod.json)")
	out := fs.String("out", "", "artifact directory (default replay/jobs/<job>)")
	mode := fs.String("mode", "full", "full or snapshot")
	workers := fs.Int("workers", 4, "config evaluation workers")
	seed := fs.Int64("seed", 11, "seed")
	warmup := fs.Int("warmup-days", 0, "warm-up days excluded from metrics")
	statePolicy := fs.String("state-policy", "ignore", "state feedback policy")
	from := fs.String("from", "", "event-time lower bound (RFC3339)")
	to := fs.String("to", "", "event-time upper bound (RFC3339)")
	scenario := fs.String("scenario", "", "only events of this fraud scenario")
	sample := fs.Float64("accounts-sample", 0, "deterministic account sample percentage")
	jobID := fs.Int64("job", 0, "job id (default unix nano)")
	keep := fs.Bool("keep-namespace", false, "keep the Redis namespace for debugging")
	allowDegraded := fs.Bool("allow-ml-degraded", false, "continue when the scorer fails")
	withExplain := fs.Bool("with-explain", false, "request SHAP in replay")
	var candidates stringList
	fs.Var(&candidates, "candidate", "label=reference (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *datasetDir == "" || *baseline == "" {
		return errors.New("-dataset and -baseline are required")
	}
	if *jobID == 0 {
		*jobID = time.Now().UnixNano()
	}
	if *out == "" {
		*out = filepath.Join("replay", "jobs", strconv.FormatInt(*jobID, 10))
	}
	fromT, err := parseOptionalTime(*from)
	if err != nil {
		return err
	}
	toT, err := parseOptionalTime(*to)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pg, err := optionalPool(ctx)
	if err != nil {
		return err
	}
	if pg != nil {
		defer pg.Close()
	}
	resolver := replayconfig.NewResolver(pg, ".")
	base, err := resolver.Resolve(ctx, "baseline", replayconfig.Ref{Rules: *baseline})
	if err != nil {
		return err
	}
	var cands []*replayconfig.Resolved
	for _, spec := range candidates {
		label, ref, err := parseCandidate(spec)
		if err != nil {
			return err
		}
		r, err := resolver.Resolve(ctx, label, ref)
		if err != nil {
			return err
		}
		cands = append(cands, r)
	}
	if len(cands) == 0 {
		return errors.New("at least one -candidate is required")
	}
	rdb, err := redisClient()
	if err != nil {
		return err
	}
	defer rdb.Close()
	fcfg, err := config.LoadFeatures(filepath.Join("config", "features.yaml"))
	if err != nil {
		return err
	}
	e := &engine.Engine{
		PG: pg, Redis: rdb, ScorerURL: os.Getenv("SCORER_URL"),
		FeatureCfg: fcfg, LateTol: time.Minute, DedupeTTL: 24 * time.Hour,
		Logger: obs.NewLogger("info"), Now: time.Now,
		Progress: func(p engine.Progress) {
			fmt.Fprintf(os.Stderr, "replay: %d/%d events (%.0f/s)\n", p.Processed, p.Events, p.PerSecond)
		},
	}
	res, err := e.Run(ctx, engine.Options{
		JobID: *jobID, DatasetDir: *datasetDir, Mode: engine.Mode(*mode),
		StatePolicy: *statePolicy, Seed: *seed, Workers: *workers, WarmupDays: *warmup,
		From: fromT, To: toT, Scenario: *scenario, SamplePct: *sample,
		AllowMLDegraded: *allowDegraded, WithExplain: *withExplain,
		KeepNamespace: *keep, ArtifactDir: *out, EngineVersion: version,
		Baseline: base, Candidates: cands,
	})
	if err != nil {
		return err
	}
	return writeJSON(os.Stdout, map[string]any{
		"job_id": *jobID, "artifact_dir": *out, "scored": res.Scored,
		"events_per_second": res.EventsPerSec, "elapsed_seconds": res.Elapsed.Seconds(),
	})
}

func optionalPool(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("FADE_TEST_DSN")
	}
	if dsn == "" {
		return nil, nil
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func parseCandidate(spec string) (string, replayconfig.Ref, error) {
	label, rest, ok := strings.Cut(spec, "=")
	if !ok || label == "" || rest == "" {
		return "", replayconfig.Ref{}, fmt.Errorf("candidate %q must be label=reference", spec)
	}
	ref := replayconfig.Ref{}
	for _, part := range strings.Split(rest, "+") {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "model:"):
			ref.Model = strings.TrimPrefix(part, "model:")
		case strings.HasPrefix(part, "combiner:"):
			raw := strings.TrimPrefix(part, "combiner:")
			var patch replayconfig.CombinerPatch
			if err := json.Unmarshal([]byte(raw), &patch); err != nil {
				return "", ref, fmt.Errorf("candidate %q combiner: %w", label, err)
			}
			ref.Combiner = &patch
		default:
			if ref.Rules != "" {
				return "", ref, fmt.Errorf("candidate %q has two rule references", label)
			}
			ref.Rules = part
		}
	}
	if ref.Rules == "" && ref.Model == "" && ref.Combiner == nil {
		return "", ref, fmt.Errorf("candidate %q has no reference", label)
	}
	return label, ref, nil
}

func parseOptionalTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time %q: %w", s, err)
	}
	return t.UTC(), nil
}

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func replayShow(args []string) error {
	fs := flag.NewFlagSet("replay show", flag.ContinueOnError)
	dir := fs.String("out", "replay/jobs", "artifact root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: fade replay show JOB")
	}
	path := filepath.Join(*dir, fs.Arg(0), "report.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(raw)
	return err
}

func replayCompare(args []string) error {
	fs := flag.NewFlagSet("replay compare", flag.ContinueOnError)
	dir := fs.String("out", "replay/jobs", "artifact root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: fade replay compare JOBA JOBB")
	}
	var reports []*report.Report
	for _, id := range fs.Args() {
		raw, err := os.ReadFile(filepath.Join(*dir, id, "report.json"))
		if err != nil {
			return err
		}
		var r report.Report
		if err := json.Unmarshal(raw, &r); err != nil {
			return err
		}
		reports = append(reports, &r)
	}
	fmt.Printf("%-24s %-10s %-10s %-10s %-12s\n", "config", "recall", "fpr", "workload", "amount_saved")
	for _, r := range reports {
		labels := make([]string, 0, len(r.Summary))
		for l := range r.Summary {
			labels = append(labels, l)
		}
		sort.Strings(labels)
		for _, l := range labels {
			m := r.Summary[l]
			fmt.Printf("%-24s %-10.4f %-10.4f %-10.1f %-12d\n", fmt.Sprintf("%d/%s", r.JobID, l),
				m.Estimates["fraud_caught_rate"].Point, m.Estimates["false_positive_rate"].Point,
				m.Workload.PerDay, m.Amounts["FRAUD:INTERVENED"])
		}
	}
	return nil
}

func replayList(args []string) error {
	fs := flag.NewFlagSet("replay list", flag.ContinueOnError)
	dir := fs.String("out", "replay/jobs", "artifact root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	entries, err := os.ReadDir(*dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(*dir, e.Name(), "report.json"))
		if err != nil {
			continue
		}
		var r report.Report
		if json.Unmarshal(raw, &r) == nil {
			fmt.Printf("%s\t%s\t%d events\n", e.Name(), r.Dataset.Name, r.Dataset.Scored)
		}
	}
	return nil
}

func replayTune(args []string) error {
	fs := flag.NewFlagSet("replay tune", flag.ContinueOnError)
	dir := fs.String("out", "replay/jobs", "artifact root")
	candidate := fs.String("candidate", "", "candidate label")
	objective := fs.String("objective", "net_benefit", "net_benefit, recall_at_fpr:0.01 or min_workload:recall>=0.85")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 || *candidate == "" {
		return errors.New("usage: fade replay tune JOB -candidate LABEL")
	}
	raw, err := os.ReadFile(filepath.Join(*dir, fs.Arg(0), "report.json"))
	if err != nil {
		return err
	}
	var r report.Report
	if err := json.Unmarshal(raw, &r); err != nil {
		return err
	}
	m, ok := r.Summary[*candidate]
	if !ok {
		return fmt.Errorf("candidate %q not in report", *candidate)
	}
	type point struct {
		stepUp, hold, block        int
		recall, fpr, workload, net float64
	}
	var pts []point
	for step := 20; step <= 60; step += 5 {
		for hold := step + 5; hold <= 85; hold += 5 {
			for block := hold + 5; block <= 95; block += 5 {
				var fraud, fraudHold, legit, legitAlert int64
				for b := 0; b <= 100; b++ {
					fraud += m.RiskHistogram[0][b]
					legit += m.RiskHistogram[1][b]
					if b >= hold {
						fraudHold += m.RiskHistogram[0][b]
					}
					if b >= step {
						legitAlert += m.RiskHistogram[1][b]
					}
				}
				recall := 0.0
				if fraud > 0 {
					recall = float64(fraudHold) / float64(fraud)
				}
				fpr := 0.0
				if legit > 0 {
					fpr = float64(legitAlert) / float64(legit)
				}
				workload := float64(fraudHold) / float64(max64(m.Days, 1))
				net := float64(m.Amounts["FRAUD:INTERVENED"])*r.CostModel.RecoveryFactor -
					workload*float64(max64(m.Days, 1))*r.CostModel.AnalystCostPerCase
				pts = append(pts, point{step, hold, block, recall, fpr, workload, net})
			}
		}
	}
	best := pts[0]
	for _, p := range pts[1:] {
		switch {
		case strings.HasPrefix(*objective, "recall_at_fpr:"):
			lim, _ := strconv.ParseFloat(strings.TrimPrefix(*objective, "recall_at_fpr:"), 64)
			if p.fpr <= lim && p.recall > best.recall {
				best = p
			}
		case strings.HasPrefix(*objective, "min_workload:recall>="):
			lim, _ := strconv.ParseFloat(strings.TrimPrefix(*objective, "min_workload:recall>="), 64)
			if p.recall >= lim && p.workload < best.workload {
				best = p
			}
		default:
			if p.net > best.net {
				best = p
			}
		}
	}
	return writeJSON(os.Stdout, map[string]any{
		"candidate": *candidate, "objective": *objective,
		"step_up": best.stepUp, "hold": best.hold, "block": best.block,
		"recall_intervened": best.recall, "fpr": best.fpr,
		"workload_per_day": best.workload, "net_benefit": best.net,
		"warning": "Tuning on the evaluation period overfits; use --tune-on/--eval-on for split-sample validation.",
	})
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// --------------------------------------------------------------------- gate

func cmdGate(args []string) error {
	fs := flag.NewFlagSet("gate", flag.ContinueOnError)
	job := fs.String("job", "", "job id under replay/jobs")
	reportPath := fs.String("report", "", "explicit report.json path")
	candidate := fs.String("candidate", "", "candidate label")
	out := fs.String("out", "gate.json", "gate result path")
	policyPath := fs.String("policy", "config/gate.yaml", "gate policy")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *candidate == "" {
		return errors.New("-candidate is required")
	}
	path := *reportPath
	if path == "" {
		if *job == "" {
			return errors.New("-job or -report is required")
		}
		path = filepath.Join("replay", "jobs", *job, "report.json")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var rep report.Report
	if err := json.Unmarshal(raw, &rep); err != nil {
		return err
	}
	policy, err := gate.LoadPolicy(*policyPath)
	if err != nil {
		policy = gate.DefaultPolicy()
	}
	res, err := gate.Evaluate(&rep, *candidate, policy)
	if err != nil {
		return err
	}
	if err := writeJSONFile(*out, res); err != nil {
		return err
	}
	fmt.Printf("gate %s: %s\n", *candidate, passWord(res.Passed))
	for _, c := range res.Checks {
		fmt.Printf("  [%s] %-32s measured %.4f threshold %.4f %s\n",
			passWord(c.Passed), c.Name, c.Measured, c.Threshold, c.Detail)
	}
	if !res.Passed {
		return fmt.Errorf("gate refused: candidate %s failed one or more checks", *candidate)
	}
	return nil
}

func passWord(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

func writeJSONFile(path string, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o640)
}

var _ = dataset.AccountHash
var _ = sort.Strings
