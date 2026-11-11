package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

type captureWriter struct {
	mu      sync.Mutex
	batches [][]*StoredDecision
	err     error
}

func (c *captureWriter) WriteDecisions(_ context.Context, batch []*StoredDecision) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	cp := append([]*StoredDecision(nil), batch...)
	c.batches = append(c.batches, cp)
	return nil
}

func (c *captureWriter) GetDecision(context.Context, string) (*StoredDecision, error) {
	return nil, ErrNotFound
}
func (c *captureWriter) ListDecisions(context.Context, DecisionFilter) ([]*StoredDecision, string, error) {
	return nil, "", nil
}
func (c *captureWriter) DecisionByEventID(context.Context, string) (*StoredDecision, error) {
	return nil, ErrNotFound
}
func (c *captureWriter) RuleMetrics(context.Context, time.Duration) (*RuleMetricResult, error) {
	return nil, nil
}
func (c *captureWriter) setErr(err error) {
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
}
func (c *captureWriter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, b := range c.batches {
		n += len(b)
	}
	return n
}

type fakePublisher struct {
	mu    sync.Mutex
	count int
	err   error
}

func (p *fakePublisher) PublishDecisions(_ context.Context, batch []*StoredDecision) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.count += len(batch)
	return nil
}

func storedDecision(id string) *StoredDecision {
	return &StoredDecision{
		Decision:  &Decision{DecisionID: id, AccountID: "acct_000123", Decision: rules.Decision("ALLOW")},
		AccountID: "acct_000123", RecipientID: "acct_000456", Amount: 1000,
		TxnType: "P2P_SEND", EventTime: time.Now().UTC(), LatencyUS: 42,
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestAsyncPersisterWritesBatchesAndPublishes(t *testing.T) {
	store := &captureWriter{}
	pub := &fakePublisher{}
	a := NewAsyncPersister(store, pub, "", quietLogger(), nil)
	a.Start(context.Background())
	a.Enqueue(storedDecision("dec_1"))
	a.Enqueue(storedDecision("dec_2"))
	a.Close()
	if got := store.count(); got != 2 {
		t.Fatalf("stored=%d", got)
	}
	if pub.count != 2 {
		t.Fatalf("published=%d", pub.count)
	}
}

func TestAsyncPersisterSpoolsThenDrains(t *testing.T) {
	dir := t.TempDir()
	store := &captureWriter{err: errors.New("postgres down")}
	a := NewAsyncPersister(store, nil, dir, quietLogger(), nil)
	a.Start(context.Background())
	a.Enqueue(storedDecision("dec_spool"))
	path := filepath.Join(dir, spoolFile)
	waitFor(t, "spool file", func() bool {
		st, err := os.Stat(path)
		return err == nil && st.Size() > 0
	})
	a.Close()

	// A second writer with a healthy store drains the spool and also writes
	// the new record.
	store.setErr(nil)
	b := NewAsyncPersister(store, &fakePublisher{err: errors.New("kafka down")}, dir, quietLogger(), nil)
	b.Start(context.Background())
	b.Enqueue(storedDecision("dec_live"))
	waitFor(t, "drained spool", func() bool {
		_, err := os.Stat(path)
		return errors.Is(err, os.ErrNotExist) && store.count() >= 2
	})
	b.Close()
}

func TestAsyncPersisterPublisherAndNilStore(t *testing.T) {
	pub := &fakePublisher{err: errors.New("kafka down")}
	a := NewAsyncPersister(nil, pub, "", quietLogger(), nil)
	a.Start(context.Background())
	a.Enqueue(storedDecision("dec_drop"))
	a.Close()
	if pub.count != 0 {
		t.Fatal("failed publish must not count")
	}
}

func TestStoredDecisionJSONRoundTrip(t *testing.T) {
	in := storedDecision("dec_json")
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out StoredDecision
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out.Decision == nil || out.Decision.DecisionID != "dec_json" || out.AccountID != in.AccountID ||
		out.RecipientID != in.RecipientID || out.Amount != in.Amount || out.TxnType != in.TxnType ||
		out.LatencyUS != in.LatencyUS || !out.EventTime.Equal(in.EventTime) ||
		out.Decision.AccountID != in.AccountID {
		t.Fatalf("roundtrip=%+v decision=%+v", out, out.Decision)
	}
	if err := json.Unmarshal([]byte(`{`), &out); err == nil {
		t.Fatal("invalid JSON must fail")
	}
}

func TestDrainSpoolSkipsCorruptLines(t *testing.T) {
	dir := t.TempDir()
	good, _ := json.Marshal(storedDecision("dec_good"))
	if err := os.WriteFile(filepath.Join(dir, spoolFile), append([]byte("{not json}\n"), append(good, '\n')...), 0o640); err != nil {
		t.Fatal(err)
	}
	store := &captureWriter{}
	a := NewAsyncPersister(store, nil, dir, quietLogger(), nil)
	if err := a.drainSpool(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.count() != 1 {
		t.Fatalf("stored=%d", store.count())
	}
	if _, err := os.Stat(filepath.Join(dir, spoolFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("spool file must be removed after drain")
	}
}

func TestListCacheAddDeleteAndCheck(t *testing.T) {
	ctx := context.Background()
	src := NewMemoryListSource()
	cache := NewListCache(src, quietLogger())
	past := time.Now().Add(-time.Minute)
	if _, err := src.AddList(ctx, ListEntry{
		ListType: ListBlock, Subject: SubjectDevice, Value: "dev_expired", ExpiresAt: &past,
	}); err != nil {
		t.Fatal(err)
	}
	blocked, err := cache.Add(ctx, ListEntry{
		ListType: ListBlock, Subject: SubjectAccount, Value: "acct_blocked", Reason: "fraud",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Add(ctx, ListEntry{
		ListType: ListBlock, Subject: SubjectDevice, Value: "dev_blocked",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Add(ctx, ListEntry{
		ListType: ListAllow, Subject: SubjectMSISDN, Value: "msisdn_ok",
	}); err != nil {
		t.Fatal(err)
	}

	if b, a := cache.Check("acct_blocked", "", ""); b == nil || b.Value != "acct_blocked" || a != nil {
		t.Fatalf("block hit=%+v %+v", b, a)
	}
	if b, a := cache.Check("", "", "msisdn_ok"); b != nil || a == nil || a.Value != "msisdn_ok" {
		t.Fatalf("allow hit=%+v %+v", b, a)
	}
	if b, a := cache.Check("", "dev_expired", ""); b != nil || a != nil {
		t.Fatal("expired entries must be ignored")
	}
	if b, a := cache.Check("", "", ""); b != nil || a != nil {
		t.Fatal("empty subjects must not match")
	}

	entries, err := cache.Entries(ctx, ListBlock, "", "", 10)
	if err != nil || len(entries) != 3 {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
	entries, err = cache.Entries(ctx, "", "", "acct_blocked", 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("filtered entries=%v err=%v", entries, err)
	}

	ok, err := cache.Delete(ctx, blocked.ID)
	if err != nil || !ok {
		t.Fatalf("delete=%v err=%v", ok, err)
	}
	if ok, err := cache.Delete(ctx, 9999); err != nil || ok {
		t.Fatalf("delete missing=%v err=%v", ok, err)
	}
	if b, _ := cache.Check("acct_blocked", "", ""); b != nil {
		t.Fatal("deleted entry still blocks")
	}

	// Refresh propagates external writes and an expired entry stays out.
	if got := cache.Refresh(ctx); got != nil {
		t.Fatal(got)
	}
}

func TestListCacheStartStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cache := NewListCache(NewMemoryListSource(), quietLogger())
	cache.ttl = 5 * time.Millisecond
	cache.Start(ctx)
	time.Sleep(25 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
	if _, err := cache.Entries(context.Background(), "", "", "", 1); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryIdempotencyLifecycle(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryIdempotency()
	decision, owner, err := m.Begin(ctx, "k1", "h1")
	if err != nil || !owner || decision != nil {
		t.Fatalf("begin owner=%v err=%v", owner, err)
	}
	if _, ok, _ := m.Get(ctx, "k1"); ok {
		t.Fatal("pending reservation must not be readable")
	}

	// A concurrent duplicate waits for the owner's decision.
	got := make(chan []byte, 1)
	go func() {
		d, owner, err := m.Begin(ctx, "k1", "h1")
		if err == nil && !owner {
			got <- d
		}
	}()
	time.Sleep(5 * time.Millisecond)
	if err := m.Complete(ctx, "k1", "h1", []byte(`{"decision":"ALLOW"}`), time.Hour); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-got:
		if string(d) != `{"decision":"ALLOW"}` {
			t.Fatalf("waiter got %s", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not receive decision")
	}

	if _, _, err := m.Begin(ctx, "k1", "other"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflict err=%v", err)
	}
	if d, ok, err := m.Get(ctx, "k1"); err != nil || !ok || len(d) == 0 {
		t.Fatalf("get=%s ok=%v err=%v", d, ok, err)
	}

	// Completed entries survive Begin.
	if d, owner, err := m.Begin(ctx, "k1", "h1"); err != nil || owner || len(d) == 0 {
		t.Fatalf("replay owner=%v d=%s err=%v", owner, d, err)
	}

	// Expiry removes done and pending entries.
	now := time.Now()
	m.now = func() time.Time { return now.Add(2 * time.Hour) }
	if _, ok, _ := m.Get(ctx, "k1"); ok {
		t.Fatal("expired entry still readable")
	}
	if _, owner, err := m.Begin(ctx, "pending", "h"); err != nil || !owner {
		t.Fatalf("pending owner=%v err=%v", owner, err)
	}
	m.now = func() time.Time { return now.Add(24 * time.Hour) }
	if _, owner, err := m.Begin(ctx, "pending", "h"); err != nil || !owner {
		t.Fatalf("expired pending reservation must be stealable: owner=%v err=%v", owner, err)
	}

	if err := m.Complete(ctx, "orphan", "h", []byte("x"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := m.Complete(ctx, "orphan", "other", []byte("x"), time.Hour); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("complete conflict err=%v", err)
	}
}
