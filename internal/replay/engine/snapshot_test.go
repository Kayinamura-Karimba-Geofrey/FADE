package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
	replayconfig "github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/dataset"
)

func TestSnapshotModeUsesStoredInputs(t *testing.T) {
	dsn := os.Getenv("FADE_TEST_PG")
	if dsn == "" {
		t.Skip("set FADE_TEST_PG to run snapshot integration tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	root := filepath.Join("..", "..", "..")
	ds, err := dataset.Load(filepath.Join(root, "data", "golden"))
	if err != nil {
		t.Fatal(err)
	}
	featureFile, err := os.Open(filepath.Join(root, "data", "golden", "features.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer featureFile.Close()
	features := map[string]json.RawMessage{}
	sc := bufio.NewScanner(featureFile)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		var probe struct {
			EventID string `json:"event_id"`
		}
		_ = json.Unmarshal(sc.Bytes(), &probe)
		features[probe.EventID] = append([]byte(nil), sc.Bytes()...)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM decisions WHERE id LIKE 'replay-snapshot-%'`); err != nil {
		t.Fatal(err)
	}
	inserted := 0
	err = ds.Stream(ctx, func(ev *events.Event) error {
		if inserted >= 30 {
			return nil
		}
		feat, ok := features[ev.EventID]
		if !ok {
			return nil
		}
		raw, _ := json.Marshal(ev)
		reasons, _ := json.Marshal([]map[string]any{{"source": "rule", "id": "R-VEL-001", "text": "stored"}})
		_, err := pool.Exec(ctx,
			`INSERT INTO decisions (id, event_id, account_id, amount, txn_type, decision,
			   risk_score, rule_score, ml_status, rule_set_version, reasons, features,
			   input_event, latency_us, event_time, decided_at)
			 VALUES ($1,$2,$3,$4,$5,'HOLD',70,60,'NOT_CONFIGURED',1,$6,$7,$8,100,$9,$9)
			 ON CONFLICT DO NOTHING`,
			fmt.Sprintf("replay-snapshot-%d", inserted), ev.EventID, ev.Sender.AccountID,
			ev.Amount, ev.TxnType, reasons, feat, raw, time.UnixMilli(ev.EventTimeMS).UTC())
		if err != nil {
			return err
		}
		inserted++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if inserted == 0 {
		t.Fatal("no decisions inserted")
	}
	resolver := replayconfig.NewResolver(pool, root)
	base, err := resolver.Resolve(ctx, "baseline", replayconfig.Ref{Rules: "file:rules/prod.json"})
	if err != nil {
		t.Fatal(err)
	}
	cand, err := resolver.Resolve(ctx, "neutral", replayconfig.Ref{Rules: "file:rules/prod.json"})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	e := &Engine{PG: pool, FeatureCfg: loadFeatureCfg(t), Logger: obs.NewLogger("error"), Now: time.Now}
	res, err := e.RunDataset(ctx, ds, Options{
		JobID: time.Now().UnixNano(), Mode: ModeSnapshot, StatePolicy: "ignore", Seed: 11,
		ArtifactDir: dir, Baseline: base, Candidates: []*replayconfig.Resolved{cand},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Scored < int64(inserted) {
		t.Fatalf("scored %d, inserted %d", res.Scored, inserted)
	}
	for _, tr := range res.Report.Candidates[0].Delta.Transition {
		if tr.Count != 0 {
			t.Fatalf("identical snapshot configs differ: %+v", tr)
		}
	}
}
