package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
)

const (
	persistQueueSize = 20000
	persistBatchSize = 500
	persistFlush     = 200 * time.Millisecond
	spoolMaxBytes    = 1 << 30 // 1 GiB
	spoolFile        = "decisions.ndjson"
)

// Publisher publishes decisions to the txn.decisions topic.
type Publisher interface {
	PublishDecisions(ctx context.Context, batch []*StoredDecision) error
}

// AsyncPersister batches decisions into Postgres and publishes them to Kafka.
// The response path only calls Enqueue, which never waits on Postgres.
type AsyncPersister struct {
	queue     chan *StoredDecision
	store     Store
	publisher Publisher
	spoolDir  string
	logger    *slog.Logger
	metrics   *obs.Metrics

	spoolMu sync.Mutex
	stop    chan struct{}
	done    sync.WaitGroup
}

// NewAsyncPersister builds the writer. store may be nil (decisions dropped
// after publishing); spoolDir empty disables disk spooling.
func NewAsyncPersister(store Store, publisher Publisher, spoolDir string, logger *slog.Logger, metrics *obs.Metrics) *AsyncPersister {
	if logger == nil {
		logger = slog.Default()
	}
	return &AsyncPersister{
		queue: make(chan *StoredDecision, persistQueueSize), store: store,
		publisher: publisher, spoolDir: spoolDir, logger: logger, metrics: metrics, stop: make(chan struct{}),
	}
}

// Enqueue never blocks the response on the database. A full queue spills the
// decision to disk synchronously.
func (a *AsyncPersister) Enqueue(d *StoredDecision) {
	select {
	case a.queue <- d:
		a.setQueueDepth(len(a.queue))
	default:
		if err := a.spool([]*StoredDecision{d}); err != nil {
			a.logger.Error("decision queue full and spool failed", "error", err)
		}
	}
}

// Start launches the batching worker.
func (a *AsyncPersister) Start(ctx context.Context) {
	a.done.Add(1)
	go a.run(ctx)
}

// Close flushes remaining decisions (best effort) and waits for the worker.
func (a *AsyncPersister) Close() {
	close(a.stop)
	a.done.Wait()
}

func (a *AsyncPersister) run(ctx context.Context) {
	defer a.done.Done()
	ticker := time.NewTicker(persistFlush)
	defer ticker.Stop()
	batch := make([]*StoredDecision, 0, persistBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		a.flush(ctx, batch)
		batch = batch[:0]
	}
	for {
		select {
		case <-a.stop:
			for {
				select {
				case rec := <-a.queue:
					batch = append(batch, rec)
					if len(batch) >= persistBatchSize {
						flush()
					}
				default:
					flush()
					a.setQueueDepth(0)
					return
				}
			}
		case <-ctx.Done():
			flush()
			return
		case rec := <-a.queue:
			batch = append(batch, rec)
			a.setQueueDepth(len(a.queue))
			if len(batch) >= persistBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
			if a.spoolSize() > 0 {
				if err := a.drainSpool(ctx); err != nil {
					a.logger.Warn("spool drain failed", "error", err)
				}
			}
		}
	}
}

func (a *AsyncPersister) flush(ctx context.Context, batch []*StoredDecision) {
	// Replay the spool first so persistence order is preserved.
	if a.spoolSize() > 0 {
		if err := a.drainSpool(ctx); err != nil {
			_ = a.spool(batch)
			a.reportBatch("spooled")
			return
		}
	}
	if a.publisher != nil {
		pubCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if err := a.publisher.PublishDecisions(pubCtx, batch); err != nil {
			a.logger.Warn("decision publish failed", "error", err)
		}
		cancel()
	}
	if a.store == nil {
		a.reportBatch("dropped")
		return
	}
	writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.store.WriteDecisions(writeCtx, batch); err != nil {
		a.logger.Error("decision batch write failed; spooling", "error", err, "batch", len(batch))
		_ = a.spool(batch)
		a.reportBatch("spooled")
		return
	}
	a.reportBatch("ok")
}

func (a *AsyncPersister) spoolPath() string {
	return filepath.Join(a.spoolDir, spoolFile)
}

func (a *AsyncPersister) spool(batch []*StoredDecision) error {
	if a.spoolDir == "" {
		return nil
	}
	if err := os.MkdirAll(a.spoolDir, 0o755); err != nil {
		return err
	}
	a.spoolMu.Lock()
	defer a.spoolMu.Unlock()
	path := a.spoolPath()
	if st, err := os.Stat(path); err == nil && st.Size() >= spoolMaxBytes {
		_ = os.Remove(path + ".1")
		_ = os.Rename(path, path+".1")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, rec := range batch {
		if err := enc.Encode(rec); err != nil {
			return err
		}
	}
	a.reportSpool()
	return nil
}

type spoolRecord struct {
	Decision    *Decision `json:"decision"`
	AccountID   string    `json:"account_id"`
	RecipientID string    `json:"recipient_id,omitempty"`
	Amount      int64     `json:"amount"`
	TxnType     string    `json:"txn_type"`
	EventTime   time.Time `json:"event_time"`
	LatencyUS   int64     `json:"latency_us"`
}

func (rec *StoredDecision) MarshalJSON() ([]byte, error) {
	return json.Marshal(spoolRecord{
		Decision: rec.Decision, AccountID: rec.AccountID, RecipientID: rec.RecipientID,
		Amount: rec.Amount, TxnType: rec.TxnType, EventTime: rec.EventTime, LatencyUS: rec.LatencyUS,
	})
}

func (rec *StoredDecision) UnmarshalJSON(raw []byte) error {
	var s spoolRecord
	if err := json.Unmarshal(raw, &s); err != nil {
		return err
	}
	rec.Decision = s.Decision
	rec.AccountID, rec.RecipientID, rec.Amount = s.AccountID, s.RecipientID, s.Amount
	rec.TxnType, rec.EventTime, rec.LatencyUS = s.TxnType, s.EventTime, s.LatencyUS
	if rec.Decision != nil {
		rec.Decision.AccountID = s.AccountID
		rec.Decision.RecipientID = s.RecipientID
		rec.Decision.Amount = s.Amount
		rec.Decision.TxnType = s.TxnType
		rec.Decision.EventTime = s.EventTime
		rec.Decision.LatencyUS = s.LatencyUS
	}
	return nil
}

func (a *AsyncPersister) drainSpool(ctx context.Context) error {
	if a.store == nil || a.spoolDir == "" {
		return nil
	}
	a.spoolMu.Lock()
	defer a.spoolMu.Unlock()
	path := a.spoolPath()
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	batch := make([]*StoredDecision, 0, persistBatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := a.store.WriteDecisions(writeCtx, batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	for sc.Scan() {
		var rec StoredDecision
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			continue // corrupt line: skip, keep the rest
		}
		batch = append(batch, &rec)
		if len(batch) >= persistBatchSize {
			if err := flush(); err != nil {
				a.reportBatch("error")
				return err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if err := flush(); err != nil {
		a.reportBatch("error")
		return err
	}
	_ = f.Close()
	if err := os.Remove(path); err != nil {
		return err
	}
	a.reportBatch("ok")
	a.reportSpool()
	a.logger.Info("decision spool drained")
	return nil
}

func (a *AsyncPersister) spoolSize() int64 {
	if a.spoolDir == "" {
		return 0
	}
	st, err := os.Stat(a.spoolPath())
	if err != nil {
		return 0
	}
	return st.Size()
}

func (a *AsyncPersister) reportSpool() {
	if a.metrics != nil {
		a.metrics.SpoolBytes.Set(float64(a.spoolSize()))
	}
}

func (a *AsyncPersister) setQueueDepth(n int) {
	if a.metrics != nil {
		a.metrics.PersistQueueDepth.Set(float64(n))
	}
}

func (a *AsyncPersister) reportBatch(result string) {
	if a.metrics != nil {
		a.metrics.PersistBatches.WithLabelValues(result).Inc()
	}
}
