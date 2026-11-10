// Command feature-parity replays a JSONL event stream through the Go feature
// store and writes canonical feature JSONL. It is the Go side of the Python
// reference parity harness; Kafka transport is exercised separately.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "feature-parity:", err)
		os.Exit(1)
	}
}

func run() error {
	eventsPath := flag.String("events", "", "JSONL events in arrival order")
	outPath := flag.String("out", "", "feature JSONL output")
	configPath := flag.String("config", "config/features.parity.yaml", "feature YAML")
	redisAddr := flag.String("redis", "localhost:6379", "Redis host:port")
	redisDB := flag.Int("db", 15, "Redis database")
	reset := flag.Bool("reset", false, "FLUSHDB the parity Redis database first")
	flag.Parse()
	if *eventsPath == "" || *outPath == "" {
		return errors.New("--events and --out are required")
	}

	cfg, err := config.Load(*configPath, os.LookupEnv)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr, DB: *redisDB})
	defer func() { _ = rdb.Close() }()
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return fmt.Errorf("redis %s db %d: %w", *redisAddr, *redisDB, err)
	}
	if *reset {
		if err := rdb.FlushDB(ctx).Err(); err != nil {
			return fmt.Errorf("flush parity redis: %w", err)
		}
	}

	in, err := os.Open(*eventsPath)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(*outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	store := features.NewStore(rdb, cfg.Features, cfg.LateTolerance, cfg.DedupeTTL, features.Hooks{})
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	writer := bufio.NewWriterSize(out, 1<<20)
	defer writer.Flush()

	var ok, duplicate, late, invalid int
	for line := 1; scanner.Scan(); line++ {
		raw := scanner.Bytes()
		if len(raw) == 0 {
			continue
		}
		ev, verr := events.Parse(raw, time.Time{})
		if verr != nil {
			invalid++
			fmt.Fprintf(os.Stderr, "line %d: %v\n", line, verr)
			continue
		}
		vector, status, err := store.Compute(ctx, ev)
		if err != nil {
			return fmt.Errorf("line %d event %s: %w", line, ev.EventID, err)
		}
		switch status {
		case features.StatusDuplicate:
			duplicate++
			continue
		case features.StatusLate:
			late++
			continue
		}
		payload, err := json.Marshal(vector)
		if err != nil {
			return fmt.Errorf("marshal %s: %w", ev.EventID, err)
		}
		if _, err := writer.Write(append(payload, '\n')); err != nil {
			return err
		}
		ok++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "feature-parity: %d ok, %d duplicate, %d late, %d invalid\n",
		ok, duplicate, late, invalid)
	return nil
}
