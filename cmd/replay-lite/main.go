// Command replay-lite replays a dataset through the real decision pipeline for
// rule tuning. It is intentionally smaller than the Phase 6 replay job: one
// pass, one rule set, JSON + Markdown reports.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/dataset"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

type ruleStat struct {
	RuleID    string  `json:"rule_id"`
	Mode      string  `json:"mode"`
	Fires     int     `json:"fires"`
	TP        int     `json:"tp"`
	FP        int     `json:"fp"`
	Precision float64 `json:"precision"`
}

type scenarioStat struct {
	Scenario string  `json:"scenario"`
	Fraud    int     `json:"fraud"`
	Caught   int     `json:"caught"`
	Blocked  int     `json:"blocked"`
	Recall   float64 `json:"recall"`
}

type falsePositive struct {
	EventID  string   `json:"event_id"`
	Decision string   `json:"decision"`
	Risk     int      `json:"risk_score"`
	Amount   int64    `json:"amount"`
	Scenario string   `json:"scenario,omitempty"`
	Reasons  []string `json:"reasons"`
}

type report struct {
	Dataset        string          `json:"dataset"`
	RuleSet        string          `json:"rule_set"`
	RuleVersion    int             `json:"rule_version"`
	Events         int             `json:"events"`
	Decisions      map[string]int  `json:"decisions"`
	Fraud          int             `json:"fraud"`
	Legit          int             `json:"legit"`
	Caught         int             `json:"caught"`
	Blocked        int             `json:"blocked"`
	FalsePositives int             `json:"false_positives"`
	FPR            float64         `json:"fpr"`
	Recall         float64         `json:"recall"`
	Degraded       int             `json:"degraded"`
	Scenarios      []scenarioStat  `json:"scenarios"`
	Rules          []ruleStat      `json:"rules"`
	TopFalsePos    []falsePositive `json:"top_false_positives"`
	ElapsedSeconds float64         `json:"elapsed_seconds"`
}

func main() {
	set := flag.String("set", "data/small", "dataset directory")
	rulesPath := flag.String("rules", "rules/prod.json", "rule set JSON")
	redisAddr := flag.String("redis", "localhost:6390", "isolated Redis address")
	redisDB := flag.Int("db", 15, "Redis database used for the run")
	featureCfg := flag.String("feature-config", "config/features.yaml", "feature config")
	out := flag.String("out", "", "output prefix (writes .json and .md)")
	limit := flag.Int("limit", 0, "limit events (0 = all)")
	flag.Parse()

	if err := run(*set, *rulesPath, *redisAddr, *redisDB, *featureCfg, *out, *limit); err != nil {
		fmt.Fprintln(os.Stderr, "replay-lite:", err)
		os.Exit(1)
	}
}

func run(set, rulesPath, redisAddr string, redisDB int, featureCfg, out string, limit int) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg, err := config.LoadFeatures(featureCfg)
	if err != nil {
		return err
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, DB: redisDB})
	ctx := context.Background()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		return fmt.Errorf("flush isolated redis db %d: %w", redisDB, err)
	}
	defer func() { _ = rdb.FlushDB(context.Background()); _ = rdb.Close() }()

	ruleBytes, err := os.ReadFile(rulesPath)
	if err != nil {
		return err
	}
	rs, err := rules.Load(ruleBytes)
	if err != nil {
		return err
	}

	// A manager is only needed for its atomic pointer; install the file in a
	// temporary directory.
	tmp, err := os.MkdirTemp("", "replay-lite-rules")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	if err := os.WriteFile(filepath.Join(tmp, "prod.json"), ruleBytes, 0o644); err != nil {
		return err
	}
	manager := rules.NewManager(tmp, logger, nil, rules.Hooks{})
	if _, err := manager.Reload(ctx, "replay-lite"); err != nil {
		return err
	}

	labels, err := dataset.ReadLabels(filepath.Join(set, "labels.parquet"))
	if err != nil {
		return err
	}
	// A full-dataset replay must accept events that arrive out of order by
	// more than the live 60 s tolerance; window semantics still use event time.
	featureStore := features.NewStore(rdb, cfg, 365*24*time.Hour, 24*time.Hour, features.Hooks{})
	p := pipeline.New(pipeline.Deps{
		Rules: manager, Features: featureStore, Idem: pipeline.NewMemoryIdempotency(),
		Lists: pipeline.NewListCache(pipeline.NewMemoryListSource(), logger),
		Sink:  pipeline.DiscardSink{}, Logger: logger, Config: pipeline.DefaultConfig(),
	})

	rep := report{
		Dataset: set, RuleSet: rs.RuleSet, RuleVersion: rs.Version,
		Decisions: map[string]int{},
	}
	ruleIdx := map[string]int{}
	start := time.Now()
	parsed := 0
	err = dataset.ForEachEventJSONLZstd(filepath.Join(set, "events.jsonl.zst"), func(raw []byte) error {
		if limit > 0 && parsed >= limit {
			return errStop
		}
		parsed++
		ev, verr := events.Parse(raw, time.Time{})
		if verr != nil {
			return nil // fixture rows that do not meet the frozen schema are skipped
		}
		dec, derr := p.Decide(ctx, ev, pipeline.Options{IncludeExplain: true})
		if derr != nil {
			return fmt.Errorf("decide %s: %w", ev.EventID, derr)
		}
		label := labels[ev.EventID]
		isFraud := label.Label == "FRAUD"
		rep.Events++
		rep.Decisions[string(dec.Decision)]++
		if isFraud {
			rep.Fraud++
		} else if label.Label == "LEGIT" {
			rep.Legit++
		}
		if dec.Degraded {
			rep.Degraded++
		}
		caught := dec.Decision == rules.StepUp || dec.Decision == rules.Hold || dec.Decision == rules.Block
		blocked := dec.Decision == rules.Hold || dec.Decision == rules.Block
		if isFraud && caught {
			rep.Caught++
		}
		if isFraud && blocked {
			rep.Blocked++
		}
		if !isFraud && label.Label == "LEGIT" && caught {
			rep.FalsePositives++
		}
		scenario := label.Scenario
		if scenario == "" {
			scenario = "UNLABELLED"
		}
		if isFraud {
			ensureScenario(&rep, scenario)
			for i := range rep.Scenarios {
				if rep.Scenarios[i].Scenario == scenario {
					rep.Scenarios[i].Fraud++
					if caught {
						rep.Scenarios[i].Caught++
					}
					if blocked {
						rep.Scenarios[i].Blocked++
					}
				}
			}
		}

		for _, r := range dec.Reasons {
			if r.Source != "rule" {
				continue
			}
			id := r.ID
			idx, ok := ruleIdx[id]
			if !ok {
				idx = len(rep.Rules)
				ruleIdx[id] = idx
				rep.Rules = append(rep.Rules, ruleStat{RuleID: id, Mode: string(r.Mode)})
			}
			rep.Rules[idx].Fires++
			if isFraud {
				rep.Rules[idx].TP++
			} else if label.Label == "LEGIT" {
				rep.Rules[idx].FP++
			}
		}

		if !isFraud && caught && len(rep.TopFalsePos) < 200 {
			fp := falsePositive{EventID: ev.EventID, Decision: string(dec.Decision),
				Risk: dec.RiskScore, Amount: ev.Amount, Scenario: scenario}
			for _, r := range dec.Reasons {
				fp.Reasons = append(fp.Reasons, r.ID+": "+r.Text)
			}
			rep.TopFalsePos = append(rep.TopFalsePos, fp)
		}
		return nil
	})
	if err != nil && err != errStop {
		return err
	}
	rep.ElapsedSeconds = time.Since(start).Seconds()
	if rep.Legit > 0 {
		rep.FPR = float64(rep.FalsePositives) / float64(rep.Legit)
	}
	if rep.Fraud > 0 {
		rep.Recall = float64(rep.Caught) / float64(rep.Fraud)
	}
	for i := range rep.Rules {
		n := rep.Rules[i].TP + rep.Rules[i].FP
		if n > 0 {
			rep.Rules[i].Precision = float64(rep.Rules[i].TP) / float64(n)
		}
	}
	sort.Slice(rep.Rules, func(i, j int) bool { return rep.Rules[i].Fires > rep.Rules[j].Fires })
	sort.Slice(rep.Scenarios, func(i, j int) bool { return rep.Scenarios[i].Scenario < rep.Scenarios[j].Scenario })
	sort.Slice(rep.TopFalsePos, func(i, j int) bool { return rep.TopFalsePos[i].Risk > rep.TopFalsePos[j].Risk })
	if len(rep.TopFalsePos) > 20 {
		rep.TopFalsePos = rep.TopFalsePos[:20]
	}
	for i := range rep.Scenarios {
		if rep.Scenarios[i].Fraud > 0 {
			rep.Scenarios[i].Recall = float64(rep.Scenarios[i].Caught) / float64(rep.Scenarios[i].Fraud)
		}
	}

	js, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(js))
	md := renderMarkdown(&rep)
	if out != "" {
		if err := os.WriteFile(out+".json", js, 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(out+".md", []byte(md), 0o644); err != nil {
			return err
		}
	}
	fmt.Fprintln(os.Stderr, md)
	return nil
}

func ensureScenario(rep *report, scenario string) {
	for _, s := range rep.Scenarios {
		if s.Scenario == scenario {
			return
		}
	}
	rep.Scenarios = append(rep.Scenarios, scenarioStat{Scenario: scenario})
}

func renderMarkdown(r *report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Replay-lite: %s\n\n", r.Dataset)
	fmt.Fprintf(&b, "- Rule set: `%s` v%d\n- Events: %d (fraud %d, legit %d)\n", r.RuleSet, r.RuleVersion, r.Events, r.Fraud, r.Legit)
	fmt.Fprintf(&b, "- Caught (>= STEP_UP): %d (%.2f%%)\n", r.Caught, r.Recall*100)
	fmt.Fprintf(&b, "- Blocked (>= HOLD): %d\n", r.Blocked)
	fmt.Fprintf(&b, "- FPR: %.2f%%\n- Degraded: %d\n- Elapsed: %.1fs\n\n", r.FPR*100, r.Degraded, r.ElapsedSeconds)
	b.WriteString("## Decisions\n\n| Decision | Count |\n| --- | ---: |\n")
	for _, d := range []string{"ALLOW", "STEP_UP", "HOLD", "BLOCK"} {
		fmt.Fprintf(&b, "| %s | %d |\n", d, r.Decisions[d])
	}
	b.WriteString("\n## Per-scenario recall\n\n| Scenario | Fraud | Caught | Blocked | Recall |\n| --- | ---: | ---: | ---: | ---: |\n")
	for _, s := range r.Scenarios {
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %.1f%% |\n", s.Scenario, s.Fraud, s.Caught, s.Blocked, s.Recall*100)
	}
	b.WriteString("\n## Rule tuning table\n\n| Rule | Mode | Fires | TP | FP | Precision |\n| --- | --- | ---: | ---: | ---: | ---: |\n")
	for _, s := range r.Rules {
		fmt.Fprintf(&b, "| %s | %s | %d | %d | %d | %.1f%% |\n", s.RuleID, s.Mode, s.Fires, s.TP, s.FP, s.Precision*100)
	}
	b.WriteString("\n## Top false positives\n\n")
	for _, fp := range r.TopFalsePos {
		fmt.Fprintf(&b, "- %s %s risk=%d amount=%d: %s\n", fp.EventID, fp.Decision, fp.Risk, fp.Amount, strings.Join(fp.Reasons, " | "))
	}
	return b.String()
}

var errStop = fmt.Errorf("limit reached")
