// Command feature-load drives the feature store at a target sustained rate and
// reports throughput and latency percentiles. It is a local benchmark tool; it
// does not publish to Kafka.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "feature-load:", err)
		os.Exit(1)
	}
}

func run() error {
	rate := flag.Int("rate", 1000, "target events per second")
	duration := flag.Duration("duration", 10*time.Second, "measurement duration")
	warmup := flag.Duration("warmup", 2*time.Second, "warmup duration")
	workers := flag.Int("workers", 32, "concurrent workers")
	redisAddr := flag.String("redis", "localhost:6380", "Redis host:port")
	configPath := flag.String("config", "config/features.yaml", "feature YAML")
	maxP99 := flag.Float64("max-p99-ms", 0, "fail when p99 exceeds this value (0 disables)")
	flag.Parse()
	if *rate < 1 || *workers < 1 {
		return fmt.Errorf("--rate and --workers must be positive")
	}
	cfg, err := config.LoadFeatures(*configPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer func() { _ = rdb.Close() }()
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		pingCancel()
		return fmt.Errorf("redis %s: %w", *redisAddr, err)
	}
	pingCancel()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		return fmt.Errorf("flush redis: %w", err)
	}
	store := features.NewStore(rdb, cfg, 60*time.Second, 24*time.Hour, features.Hooks{})

	var seq atomic.Int64
	var recording atomic.Bool
	var total atomic.Int64
	latencies := make([][]time.Duration, *workers)
	var wg sync.WaitGroup
	interval := time.Duration(float64(time.Second) * float64(*workers) / float64(*rate))
	if interval < time.Microsecond {
		interval = time.Microsecond
	}
	base := time.Now().UTC()
	for w := 0; w < *workers; w++ {
		latencies[w] = make([]time.Duration, 0, *rate / *workers * int(duration.Seconds()) + 1)
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			local := 0
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				n := seq.Add(1)
				ev := loadEvent(int(n), worker, local, base, cfg)
				local++
				start := time.Now()
				_, _, err := store.Compute(ctx, ev)
				elapsed := time.Since(start)
				if err != nil {
					fmt.Fprintf(os.Stderr, "event %s: %v\n", ev.EventID, err)
					continue
				}
				total.Add(1)
				if recording.Load() {
					latencies[worker] = append(latencies[worker], elapsed)
				}
			}
		}(w)
	}

	time.Sleep(*warmup)
	total.Store(0)
	recording.Store(true)
	start := time.Now()
	time.Sleep(*duration)
	elapsed := time.Since(start)
	recording.Store(false)
	cancel()
	wg.Wait()

	var all []time.Duration
	for _, wl := range latencies {
		all = append(all, wl...)
	}
	if len(all) == 0 {
		return fmt.Errorf("no events measured")
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	percentile := func(p float64) time.Duration {
		index := int(float64(len(all)-1) * p)
		return all[index]
	}
	throughput := float64(total.Load()) / elapsed.Seconds()
	p50, p95, p99 := percentile(0.50), percentile(0.95), percentile(0.99)
	fmt.Printf("events=%d elapsed=%.2fs throughput=%.0f/s p50=%s p95=%s p99=%s\n",
		total.Load(), elapsed.Seconds(), throughput, p50, p95, p99)
	if *maxP99 > 0 && p99 > time.Duration(*maxP99*float64(time.Millisecond)) {
		return fmt.Errorf("p99 %s exceeds %.1fms", p99, *maxP99)
	}
	return nil
}

func loadEvent(seq, worker, local int, base time.Time, cfg config.Features) *events.Event {
	// 60% non-cycle retail payments, 20% P2P, 10% cash-in, 10% cash-out.
	txn := events.MerchantPay
	switch seq % 10 {
	case 7, 8:
		txn = events.P2PSend
	case 9:
		txn = events.CashIn
	case 6:
		txn = events.CashOut
	}
	sender := fmt.Sprintf("acct_%06d", worker*1000+local%1000)
	recipient := fmt.Sprintf("acct_%06d", (worker*7919+local*13)%100000)
	var agentID *string
	if txn == events.CashIn || txn == events.CashOut {
		recipient = "spec_agent_000"
		agent := recipient
		agentID = &agent
	}
	ev := &events.Event{
		EventID:     fmt.Sprintf("01M6ZW06807TFTK1P7CPF%03d%03d", worker, local%1000),
		EventTimeMS: base.Add(time.Duration(seq) * time.Millisecond).UnixMilli(),
		TxnType:     txn,
		Amount:      1000 + int64(seq%9000),
		Currency:    "RWF",
		Sender: events.Sender{
			AccountID: sender, DeviceID: "dev_" + sender, SIMSerialHash: "sim_" + sender,
			Geo: events.Geo{Lat: -1.9, Lon: 30.0}, KYCTier: 2,
		},
		Recipient: events.Recipient{AccountID: recipient},
		Channel:   events.ChannelUSSD,
		AgentID:   agentID,
		SessionID: "pos-2M6ZW068",
	}
	ev.Fee = events.FeeForAmount(ev.Amount)
	if txn == events.CashIn || txn == events.CashOut {
		ev.Channel = events.ChannelAgent
		ev.Recipient.IsAgent = true
	}
	_ = cfg
	return ev
}
