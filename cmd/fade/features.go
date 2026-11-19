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
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
)

// cmdFeatures implements `fade features backfill`.
//
// The feature store is derived state: Redis windows are rebuilt from the
// durable `txn.raw` topic. Backfill replays a closed event-time window through
// the same Lua aggregate the feature worker uses, so the operation is
// idempotent per event_id (the dedupe key is written inside the same script).
// Running it twice over the same window therefore produces identical counters.
func cmdFeatures(args []string) error {
	if len(args) == 0 {
		return errors.New("features subcommand required: backfill")
	}
	switch args[0] {
	case "backfill":
		return featuresBackfill(args[1:])
	default:
		return fmt.Errorf("unknown features subcommand %q", args[0])
	}
}

func featuresBackfill(args []string) error {
	fs := flag.NewFlagSet("features backfill", flag.ContinueOnError)
	from := fs.String("from", "", "event-time lower bound (RFC3339, inclusive)")
	to := fs.String("to", "", "event-time upper bound (RFC3339, inclusive)")
	brokers := fs.String("brokers", envOr("KAFKA_BROKERS", "localhost:19092"), "comma-separated brokers")
	topic := fs.String("topic", envOr("KAFKA_RAW_TOPIC", "txn.raw"), "raw transaction topic")
	redisAddr := fs.String("redis", envOr("REDIS_ADDR", "localhost:6379"), "Redis address")
	redisPass := fs.String("redis-password", os.Getenv("REDIS_PASSWORD"), "Redis password")
	configPath := fs.String("config", envOr("FEATURE_CONFIG", filepath.Join("config", "features.yaml")), "feature config")
	eventsPath := fs.String("events", "", "read events from a JSONL file instead of Kafka (testing/offline)")
	dryRun := fs.Bool("dry-run", false, "scan and count without writing Redis")
	batch := fs.Int("batch", 500, "events per flush before reporting progress")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *from == "" || *to == "" {
		return errors.New("-from and -to are required (RFC3339)")
	}
	fromT, err := time.Parse(time.RFC3339, *from)
	if err != nil {
		return fmt.Errorf("invalid -from: %w", err)
	}
	toT, err := time.Parse(time.RFC3339, *to)
	if err != nil {
		return fmt.Errorf("invalid -to: %w", err)
	}
	if !toT.After(fromT) {
		return errors.New("-to must be after -from")
	}

	cfg, err := config.LoadFeatures(*configPath)
	if err != nil {
		return fmt.Errorf("load features config: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr, Password: *redisPass})
	defer func() { _ = rdb.Close() }()
	if !*dryRun {
		pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
		defer pingCancel()
		if err := rdb.Ping(pingCtx).Err(); err != nil {
			return fmt.Errorf("redis %s: %w", *redisAddr, err)
		}
	}

	store := features.NewStore(rdb, cfg, time.Minute, 24*time.Hour, features.Hooks{})

	log := obs.NewLogger("info")
	counts := map[string]int{"scanned": 0, "applied": 0, "duplicate": 0, "late": 0, "skipped": 0, "invalid": 0}
	started := time.Now()
	apply := func(raw []byte, ingest time.Time) error {
		counts["scanned"]++
		ev, verr := events.Parse(raw, ingest)
		if verr != nil {
			counts["invalid"]++
			return nil
		}
		evTime := time.UnixMilli(ev.EventTimeMS).UTC()
		if evTime.Before(fromT) || evTime.After(toT) {
			counts["skipped"]++
			return nil
		}
		if *dryRun {
			counts["applied"]++
			return nil
		}
		_, status, err := store.Compute(ctx, ev)
		if err != nil {
			return fmt.Errorf("event %s: %w", ev.EventID, err)
		}
		switch status {
		case features.StatusDuplicate:
			counts["duplicate"]++
		case features.StatusLate:
			counts["late"]++
		default:
			counts["applied"]++
			if counts["applied"]%*batch == 0 {
				log.Info("backfill progress", "applied", counts["applied"], "scanned", counts["scanned"])
			}
		}
		return nil
	}

	if *eventsPath != "" {
		raw, err := os.ReadFile(*eventsPath)
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			// A historical file has no ingest timestamp; the event's own time
			// is the only truthful ingest time for an offline backfill.
			var meta struct {
				EventTime string `json:"event_time"`
			}
			_ = json.Unmarshal([]byte(line), &meta)
			ingest, perr := time.Parse("2006-01-02T15:04:05.000Z", meta.EventTime)
			if perr != nil {
				counts["invalid"]++
				counts["scanned"]++
				continue
			}
			if err := apply([]byte(line), ingest); err != nil {
				return err
			}
		}
		return writeBackfillResult(fromT, toT, *topic, *redisAddr, *dryRun, counts, started)
	}

	// No consumer group: backfill must not move the live worker's offsets.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().At(fromT.UnixMilli())),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.MaxBufferedRecords(50_000),
	)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer client.Close()

	seen := 0
	idle := 0

	for {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if fe.Err == context.Canceled {
					return nil
				}
				return fmt.Errorf("fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
			}
		}
		records := fetches.Records()
		if len(records) == 0 {
			idle++
			// Three consecutive empty polls past the upper bound mean the
			// window is drained (offset-by-time can land mid-partition).
			if idle >= 3 && seen > 0 {
				break
			}
			if idle > 30 {
				break
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		idle = 0
		for _, rec := range records {
			seen++
			if err := apply(rec.Value, rec.Timestamp); err != nil {
				return err
			}
		}
		if len(records) > 0 {
			last := records[len(records)-1]
			if ev, verr := events.Parse(last.Value, last.Timestamp); verr == nil {
				if time.UnixMilli(ev.EventTimeMS).UTC().After(toT) {
					idle++
					if idle >= 3 {
						break
					}
				}
			}
		}
	}

	return writeBackfillResult(fromT, toT, *topic, *redisAddr, *dryRun, counts, started)
}

func writeBackfillResult(fromT, toT time.Time, topic, redisAddr string, dryRun bool,
	counts map[string]int, started time.Time) error {
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := map[string]any{
		"from": fromT.UTC().Format(time.RFC3339), "to": toT.UTC().Format(time.RFC3339),
		"topic": topic, "redis": redisAddr, "dry_run": dryRun,
		"counts": counts, "elapsed_seconds": time.Since(started).Seconds(),
	}
	return writeJSON(os.Stdout, out)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

var _ = json.Marshal
