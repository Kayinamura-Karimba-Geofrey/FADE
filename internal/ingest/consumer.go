package ingest

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
)

// NewClient builds the shared consumer/producer client. One client is used so
// DLQ and feature output publishes share the same broker connections.
func NewClient(cfg *config.Config) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.ClientID("fade-feature-worker"),
		kgo.ConsumerGroup(cfg.KafkaGroupID),
		kgo.ConsumeTopics(cfg.KafkaTopic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250 * time.Millisecond),
		kgo.FetchMaxBytes(50 << 20),
		kgo.FetchMaxPartitionBytes(10 << 20),
		kgo.MaxBufferedRecords(20_000),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return client, nil
}

// NewKgoPublisher adapts the shared franz-go client to Publisher.
func NewKgoPublisher(client *kgo.Client) Publisher {
	return kgoPublisher{client: client}
}

// Consumer polls the raw topic. Within a partition, records are sharded by
// Kafka key, so all events for one sender stay ordered while distinct accounts
// can be processed concurrently. Offsets are committed only after the whole
// poll batch succeeds.
type Consumer struct {
	client    *kgo.Client
	processor *Processor
	metrics   *Metrics
	log       *slog.Logger
	workers   int
	inflight  int
	started   atomic.Bool
}

// NewConsumer constructs a consumer over an existing client.
func NewConsumer(client *kgo.Client, processor *Processor, metrics *Metrics, log *slog.Logger,
	workers, inflight int) *Consumer {
	if workers < 1 {
		workers = 1
	}
	if inflight < workers {
		inflight = workers
	}
	return &Consumer{
		client: client, processor: processor, metrics: metrics, log: log,
		workers: workers, inflight: inflight,
	}
}

// Run consumes until ctx is cancelled or a fatal fetch/commit error occurs.
func (c *Consumer) Run(ctx context.Context) error {
	c.started.Store(true)
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			c.client.AllowRebalance()
			return nil
		}
		var firstErr error
		for _, ferr := range fetches.Errors() {
			c.log.Error("kafka fetch error", "topic", ferr.Topic, "partition", ferr.Partition, "error", ferr.Err)
			if !errors.Is(ferr.Err, context.Canceled) {
				firstErr = ferr.Err
			}
		}
		if firstErr == nil {
			var mu sync.Mutex
			var lag int64
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				if p.Err != nil {
					c.log.Error("partition fetch error", "topic", p.Topic, "partition", p.Partition, "error", p.Err)
					return
				}
				if len(p.Records) == 0 {
					return
				}
				lag += p.HighWatermark - p.Records[len(p.Records)-1].Offset - 1
				if err := c.processPartition(ctx, p.Records); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				}
			})
			c.metrics.ConsumerLag.Set(float64(lag))
		}
		c.client.AllowRebalance()
		if firstErr != nil {
			return firstErr
		}
	}
}

func (c *Consumer) processPartition(ctx context.Context, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, c.inflight)
	jobs := make([]chan int, c.workers)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	for i := 0; i < c.workers; i++ {
		jobs[i] = make(chan int, 64)
		wg.Add(1)
		go func(ch chan int) {
			defer wg.Done()
			for idx := range ch {
				if err := c.processWithRetry(batchCtx, records[idx]); err != nil {
					errOnce.Do(func() {
						firstErr = err
						cancel()
					})
				}
				<-sem
			}
		}(jobs[i])
	}

dispatch:
	for idx, rec := range records {
		select {
		case sem <- struct{}{}:
		case <-batchCtx.Done():
			break dispatch
		}
		if batchCtx.Err() != nil {
			break
		}
		shard := keyShard(rec.Key, c.workers)
		jobs[shard] <- idx
	}
	for i := range jobs {
		close(jobs[i])
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return c.commit(ctx, records[len(records)-1])
}

func (c *Consumer) processWithRetry(ctx context.Context, rec *kgo.Record) error {
	backoff := 25 * time.Millisecond
	for attempt := 0; ; attempt++ {
		err := c.processor.Handle(ctx, rec)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.metrics.Retries.Inc()
		c.log.Warn("transient processing failure", "topic", rec.Topic, "partition", rec.Partition,
			"offset", rec.Offset, "attempt", attempt+1, "error", err)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff *= 2
		if backoff > 2*time.Second {
			backoff = 2 * time.Second
		}
	}
}

func (c *Consumer) commit(ctx context.Context, rec *kgo.Record) error {
	commitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.client.CommitRecords(commitCtx, rec); err != nil {
		c.metrics.Commits.WithLabelValues("error").Inc()
		return fmt.Errorf("commit offset %d/%d: %w", rec.Partition, rec.Offset, err)
	}
	c.metrics.Commits.WithLabelValues("ok").Inc()
	return nil
}

// Ping verifies the Kafka connection for readiness.
func (c *Consumer) Ping(ctx context.Context) error {
	if !c.started.Load() {
		return errors.New("consumer not started")
	}
	return c.client.Ping(ctx)
}

// Close releases broker connections.
func (c *Consumer) Close() { c.client.Close() }

func keyShard(key []byte, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32() % uint32(n))
}
