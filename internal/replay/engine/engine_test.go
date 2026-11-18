package engine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
	replayconfig "github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/dataset"
)

func redisForTest(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("FADE_TEST_REDIS")
	if addr == "" {
		t.Skip("set FADE_TEST_REDIS to run replay engine integration tests")
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis: %v", err)
	}
	return c
}

func loadFeatureCfg(t *testing.T) config.Features {
	t.Helper()
	cfg, err := config.LoadFeatures(filepath.Join("..", "..", "..", "config", "features.yaml"))
	if err != nil {
		t.Fatalf("features config: %v", err)
	}
	return cfg
}

func TestGoldenReplayIsWorkerCountIndependent(t *testing.T) {
	rdb := redisForTest(t)
	root := filepath.Join("..", "..", "..")
	resolver := replayconfig.NewResolver(nil, root)
	ref := replayconfig.Ref{Rules: "file:rules/prod.json"}
	base, err := resolver.Resolve(context.Background(), "baseline", ref)
	if err != nil {
		t.Fatal(err)
	}
	cand, err := resolver.Resolve(context.Background(), "neutral", ref)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := dataset.Load(filepath.Join(root, "data", "golden"))
	if err != nil {
		t.Fatal(err)
	}
	jobID := time.Now().UnixNano()
	run := func(workers int) ([]byte, []byte) {
		dir := t.TempDir()
		e := &Engine{
			Redis: rdb, FeatureCfg: loadFeatureCfg(t),
			LateTol: time.Minute, DedupeTTL: 24 * time.Hour,
			Logger: obs.NewLogger("error"), Now: time.Now,
		}
		res, err := e.RunDataset(context.Background(), ds, Options{
			JobID: jobID, DatasetDir: filepath.Join(root, "data", "golden"),
			Mode: ModeFull, StatePolicy: "ignore", Seed: 11, Workers: workers,
			ArtifactDir: dir, Baseline: base, Candidates: []*replayconfig.Resolved{cand},
		})
		if err != nil {
			t.Fatalf("workers=%d: %v", workers, err)
		}
		if res.Scored == 0 {
			t.Fatalf("workers=%d: nothing scored", workers)
		}
		raw, err := os.ReadFile(filepath.Join(dir, "report.json"))
		if err != nil {
			t.Fatal(err)
		}
		digest, err := os.ReadFile(filepath.Join(dir, "decisions.sha256"))
		if err != nil {
			t.Fatal(err)
		}
		return raw, digest
	}
	serialReport, serialDigest := run(1)
	for _, workers := range []int{4, 16} {
		rep, digest := run(workers)
		if !bytes.Equal(serialReport, rep) {
			t.Fatalf("workers=%d changed report.json bytes", workers)
		}
		if !bytes.Equal(serialDigest, digest) {
			t.Fatalf("workers=%d changed decisions.parquet digest", workers)
		}
	}
}

func TestGoldenReplayProducesDeterministicReport(t *testing.T) {
	rdb := redisForTest(t)
	root := filepath.Join("..", "..", "..")
	resolver := replayconfig.NewResolver(nil, root)
	ref := replayconfig.Ref{Rules: "file:rules/prod.json"}
	base, err := resolver.Resolve(context.Background(), "baseline", ref)
	if err != nil {
		t.Fatal(err)
	}
	cand, err := resolver.Resolve(context.Background(), "neutral", ref)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := dataset.Load(filepath.Join(root, "data", "golden"))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	e := &Engine{
		Redis: rdb, ScorerURL: "", FeatureCfg: loadFeatureCfg(t),
		LateTol: time.Minute, DedupeTTL: 24 * time.Hour,
		Logger: obs.NewLogger("error"), Now: time.Now,
	}
	opts := Options{
		JobID: time.Now().UnixNano(), DatasetDir: filepath.Join(root, "data", "golden"),
		Mode: ModeFull, StatePolicy: "ignore", Seed: 11, ArtifactDir: dir,
		Baseline: base, Candidates: []*replayconfig.Resolved{cand},
	}
	res, err := e.RunDataset(context.Background(), ds, opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Report == nil || res.Scored == 0 {
		t.Fatalf("empty report: %+v", res)
	}
	if len(res.Report.Candidates) != 1 {
		t.Fatalf("candidates = %d", len(res.Report.Candidates))
	}
	for _, tr := range res.Report.Candidates[0].Delta.Transition {
		if tr.Count != 0 {
			t.Fatalf("identical configs differ: %+v", tr)
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) < 1000 {
		t.Fatalf("report too small: %d", len(raw))
	}
}
