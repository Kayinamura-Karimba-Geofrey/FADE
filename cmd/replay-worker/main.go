// Command replay-worker executes queued Phase 6 replay jobs. It owns no HTTP
// surface; decision-api only enqueues and reads.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/audit"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
	replayconfig "github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/engine"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/metrics"
)

var version = "1.0.0"

const workerID = "replay-worker"

type job struct {
	ID          int64
	Dataset     string
	Mode        string
	StatePolicy string
	Seed        int64
	Options     json.RawMessage
}

type optionsDoc struct {
	Baseline struct {
		Rules    string          `json:"rules"`
		Model    string          `json:"model"`
		Combiner json.RawMessage `json:"combiner"`
	} `json:"baseline"`
	Candidates []struct {
		Label    string          `json:"label"`
		Rules    string          `json:"rules"`
		Model    string          `json:"model"`
		Combiner json.RawMessage `json:"combiner"`
	} `json:"candidates"`
	From       string `json:"from"`
	To         string `json:"to"`
	WarmupDays int    `json:"warmup_days"`
	Options    struct {
		WithExplain     bool `json:"with_explain"`
		AllowMLDegraded bool `json:"allow_ml_degraded"`
	} `json:"options"`
}

func main() {
	concurrency := flag.Int("concurrency", 2, "maximum concurrent jobs")
	flag.Parse()
	logger := obs.NewLogger(os.Getenv("LOG_LEVEL"))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		logger.Error("DATABASE_URL is required")
		os.Exit(1)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		logger.Error("postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	rdb := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379"), Password: os.Getenv("REDIS_PASSWORD")})
	fcfg, err := config.LoadFeatures(env("FEATURES_CONFIG", filepath.Join("config", "features.yaml")))
	if err != nil {
		logger.Error("features config", "error", err)
		os.Exit(1)
	}
	scorer := os.Getenv("SCORER_URL")
	metrics := obs.NewMetrics("replay-worker")
	auditStore := audit.NewPostgresStore(pool)

	staleTicker := time.NewTicker(30 * time.Second)
	defer staleTicker.Stop()
	pollTicker := time.NewTicker(2 * time.Second)
	defer pollTicker.Stop()
	sem := make(chan struct{}, *concurrency)
	logger.Info("replay-worker started", "version", version, "concurrency", *concurrency)
	for {
		select {
		case <-ctx.Done():
			logger.Info("replay-worker stopping")
			return
		case <-staleTicker.C:
			if n, err := reclaimStale(ctx, pool); err != nil {
				logger.Warn("stale reclaim", "error", err)
			} else if n > 0 {
				logger.Info("reclaimed stale jobs", "count", n)
			}
		case <-pollTicker.C:
			select {
			case sem <- struct{}{}:
			default:
				continue
			}
			j, err := claim(ctx, pool)
			if err != nil {
				<-sem
				logger.Warn("claim job", "error", err)
				continue
			}
			if j == nil {
				<-sem
				continue
			}
			go func(j *job) {
				defer func() { <-sem }()
				runJob(ctx, logger, pool, rdb, scorer, fcfg, auditStore, metrics, j)
			}(j)
		}
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func claim(ctx context.Context, pool *pgxpool.Pool) (*job, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var j job
	err = tx.QueryRow(ctx,
		`SELECT id, dataset, mode, state_policy, seed, options
		   FROM replay_jobs WHERE status='QUEUED'
		   ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1`).
		Scan(&j.ID, &j.Dataset, &j.Mode, &j.StatePolicy, &j.Seed, &j.Options)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx,
		`UPDATE replay_jobs SET status='RUNNING', started_at=now(), heartbeat_at=now(),
		        attempts=attempts+1, progress=0 WHERE id=$1`, j.ID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &j, nil
}

func reclaimStale(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	tag, err := pool.Exec(ctx,
		`UPDATE replay_jobs SET status='QUEUED'
		  WHERE status='RUNNING' AND heartbeat_at < now() - interval '60 seconds'`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func runJob(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool, rdb *redis.Client,
	scorer string, fcfg config.Features, auditStore audit.Store, m *obs.Metrics, j *job) {
	started := time.Now()
	logger = logger.With("job_id", j.ID)
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT attempts FROM replay_jobs WHERE id=$1`, j.ID).Scan(&attempts); err == nil && attempts > 1 {
		m.ReplayResume.Inc()
	}
	logger.Info("replay job started")
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Heartbeat and cancellation poll.
	heartbeatDone := make(chan struct{})
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		defer close(heartbeatDone)
		for {
			select {
			case <-jobCtx.Done():
				return
			case <-t.C:
				_, _ = pool.Exec(jobCtx, `UPDATE replay_jobs SET heartbeat_at=now() WHERE id=$1`, j.ID)
				var status string
				if err := pool.QueryRow(jobCtx, `SELECT status FROM replay_jobs WHERE id=$1`, j.ID).Scan(&status); err == nil && status == "CANCELLED" {
					cancel()
					return
				}
			}
		}
	}()

	var doc optionsDoc
	if err := json.Unmarshal(j.Options, &doc); err != nil {
		failJob(ctx, pool, j.ID, "decode options: "+err.Error())
		return
	}
	resolver := replayconfig.NewResolver(pool, ".")
	baseRef := replayconfig.Ref{Rules: doc.Baseline.Rules, Model: doc.Baseline.Model}
	if len(doc.Baseline.Combiner) > 0 {
		var patch replayconfig.CombinerPatch
		if json.Unmarshal(doc.Baseline.Combiner, &patch) == nil {
			baseRef.Combiner = &patch
		}
	}
	base, err := resolver.Resolve(ctx, "baseline", baseRef)
	if err != nil {
		failJob(ctx, pool, j.ID, err.Error())
		return
	}
	var cands []*replayconfig.Resolved
	for _, c := range doc.Candidates {
		ref := replayconfig.Ref{Rules: c.Rules, Model: c.Model}
		if len(c.Combiner) > 0 {
			var patch replayconfig.CombinerPatch
			if json.Unmarshal(c.Combiner, &patch) == nil {
				ref.Combiner = &patch
			}
		}
		r, err := resolver.Resolve(ctx, c.Label, ref)
		if err != nil {
			failJob(ctx, pool, j.ID, err.Error())
			return
		}
		cands = append(cands, r)
	}

	from, _ := time.Parse(time.RFC3339, doc.From)
	to, _ := time.Parse(time.RFC3339, doc.To)
	eng := &engine.Engine{
		PG: pool, Redis: rdb, ScorerURL: scorer, FeatureCfg: fcfg,
		LateTol: time.Minute, DedupeTTL: 24 * time.Hour, Logger: logger, Metrics: m,
		Progress: func(p engine.Progress) {
			progress := 0.0
			if p.Events > 0 {
				progress = 100 * float64(p.Processed) / float64(p.Events)
			}
			_, _ = pool.Exec(context.Background(),
				`UPDATE replay_jobs SET progress=$2, heartbeat_at=now() WHERE id=$1`, j.ID, progress)
		},
	}
	artifact := filepath.Join("replay", "jobs", strconv.FormatInt(j.ID, 10))
	res, err := eng.Run(jobCtx, engine.Options{
		JobID: j.ID, DatasetDir: filepath.Join("data", j.Dataset), Mode: engine.Mode(j.Mode),
		StatePolicy: j.StatePolicy, Seed: j.Seed, Workers: 4, WarmupDays: doc.WarmupDays,
		From: from, To: to, AllowMLDegraded: doc.Options.AllowMLDegraded,
		WithExplain: doc.Options.WithExplain, ArtifactDir: artifact,
		EngineVersion: version, Baseline: base, Candidates: cands,
	})
	if err != nil {
		if jobCtx.Err() != nil {
			_, _ = pool.Exec(context.Background(), `UPDATE replay_jobs SET status='CANCELLED', finished_at=now() WHERE id=$1`, j.ID)
			m.ReplayJobs.WithLabelValues("CANCELLED").Inc()
			logger.Info("replay job cancelled")
			return
		}
		failJob(ctx, pool, j.ID, err.Error())
		return
	}
	if err := persist(ctx, pool, j.ID, res, artifact); err != nil {
		failJob(ctx, pool, j.ID, "persist results: "+err.Error())
		return
	}
	_, _ = pool.Exec(ctx,
		`UPDATE replay_jobs SET status='DONE', progress=100, finished_at=now(), report_path=$2, report_hash=$3
		  WHERE id=$1`, j.ID, artifact, res.ReportHash)
	if auditStore != nil {
		row := audit.NewRow(workerID, string(audit.ActionReplayFinish), "replay_job",
			strconv.FormatInt(j.ID, 10), nil, mustJSON(map[string]any{
				"job_id": j.ID, "status": "DONE", "report_hash": res.ReportHash,
				"events": res.Processed, "scored": res.Scored,
			}), time.Now())
		if _, err := auditStore.Append(ctx, row); err != nil {
			logger.Warn("audit finish", "error", err)
		}
	}
	m.ReplayJobs.WithLabelValues("DONE").Inc()
	logger.Info("replay job finished", "elapsed", time.Since(started).String(),
		"events_per_second", res.EventsPerSec, "scored", res.Scored)
}

func persist(ctx context.Context, pool *pgxpool.Pool, id int64, res *engine.Result, dir string) error {
	hash, err := fileHash(filepath.Join(dir, "report.json"))
	if err != nil {
		return err
	}
	res.ReportHash = hash
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	configHashes := map[string]string{}
	for _, c := range res.Report.Configs {
		configHashes[c.Label] = c.ConfigHash
	}
	for _, label := range sortedLabels(res.Report.Summary) {
		m := res.Report.Summary[label]
		raw, _ := json.Marshal(m)
		scen, _ := json.Marshal(res.Report.Scenarios[label])
		rules, _ := json.Marshal(res.Report.Rules[label])
		daily, _ := json.Marshal(res.Report.Daily[label])
		seg, _ := json.Marshal(res.Report.Segments[label])
		sweep, _ := json.Marshal(res.Report.Sweep[label])
		configHash := configHashes[label]
		_, err := tx.Exec(ctx,
			`INSERT INTO replay_results (job_id, config_label, config_hash, metrics,
			   per_scenario, per_rule, daily, segments, sweep)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			 ON CONFLICT (job_id, config_label) DO UPDATE SET
			   metrics=EXCLUDED.metrics, per_scenario=EXCLUDED.per_scenario,
			   per_rule=EXCLUDED.per_rule, daily=EXCLUDED.daily,
			   segments=EXCLUDED.segments, sweep=EXCLUDED.sweep`,
			id, label, configHash, raw, scen, rules, daily, seg, sweep)
		if err != nil {
			return err
		}
	}
	for _, c := range res.Report.Candidates {
		raw, _ := json.Marshal(c.Delta)
		top, _ := json.Marshal(c.TopFraud)
		trans, _ := json.Marshal(c.Delta.Transition)
		_, err := tx.Exec(ctx,
			`INSERT INTO replay_deltas (job_id, candidate_label, summary, top_changes, transition)
			 VALUES ($1,$2,$3,$4,$5)
			 ON CONFLICT (job_id, candidate_label) DO UPDATE SET
			   summary=EXCLUDED.summary, top_changes=EXCLUDED.top_changes,
			   transition=EXCLUDED.transition`,
			id, c.Label, raw, top, trans)
		if err != nil {
			return err
		}
	}
	hashes, _ := json.Marshal(configHashes)
	if _, err := tx.Exec(ctx,
		`UPDATE replay_jobs SET dataset_hash=$2, config_hashes=$3, report_hash=$4,
		        report_path=$5, engine_version=$6 WHERE id=$1`,
		id, res.Report.Dataset.Hash, hashes, hash, dir, res.Report.EngineVersion); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func sortedLabels(m map[string]metrics.Result) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// config labels are short; insertion sort keeps this dependency-free.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func failJob(ctx context.Context, pool *pgxpool.Pool, id int64, msg string) {
	_, _ = pool.Exec(ctx, `UPDATE replay_jobs SET status='FAILED', error=$2, finished_at=now() WHERE id=$1`, id, msg)
}

func fileHash(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func mustJSON(v any) []byte {
	raw, _ := json.Marshal(v)
	return raw
}

var _ = fmt.Sprint
