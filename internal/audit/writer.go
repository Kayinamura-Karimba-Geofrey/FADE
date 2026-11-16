package audit

import (
	"context"
	"fmt"
	"time"
)

// WriterConfig configures the audit writer.
type WriterConfig struct {
	// IPHashSalt is the deployment secret used to hash client IPs. An empty
	// salt disables IP hashing; callers must then pass an already-hashed value.
	IPHashSalt []byte
	// Clock is injectable for deterministic tests.
	Clock func() time.Time
	Hooks Hooks
}

// Writer records governance events. v1 writes audit rows synchronously: an
// admin action does not return success until its audit row is committed, so a
// rule can never change without a corresponding chain entry.
type Writer struct {
	store Store
	cfg   WriterConfig
}

// NewWriter builds a writer over a store.
func NewWriter(store Store, cfg WriterConfig) *Writer {
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	return &Writer{store: store, cfg: cfg}
}

// Record validates, redacts and appends one audit entry. It returns only after
// the store has committed the row.
func (w *Writer) Record(ctx context.Context, in Input) (Entry, error) {
	if err := in.Validate(); err != nil {
		return Entry{}, err
	}
	before, err := RedactPayload(in.ObjectType, in.Before)
	if err != nil {
		return Entry{}, err
	}
	after, err := RedactPayload(in.ObjectType, in.After)
	if err != nil {
		return Entry{}, err
	}
	at := in.At
	if at.IsZero() {
		at = w.cfg.Clock()
	}
	row := Row{
		Actor: in.Actor, Action: string(in.Action), ObjectType: in.ObjectType,
		ObjectID: in.ObjectID, Before: before, After: after, At: NormalizeAt(at),
		RequestID: in.RequestID, SourceIPHash: in.SourceIPHash,
	}
	if row.SourceIPHash != "" && len(w.cfg.IPHashSalt) > 0 && !looksHashed(row.SourceIPHash) {
		row.SourceIPHash = HashIP(w.cfg.IPHashSalt, row.SourceIPHash)
	}
	started := time.Now()
	entry, err := w.store.Append(ctx, row)
	if w.cfg.Hooks.OnAppend != nil {
		w.cfg.Hooks.OnAppend(row.Action, time.Since(started).Seconds(), err)
	}
	if err != nil {
		return Entry{}, fmt.Errorf("audit: record %s: %w", in.Action, err)
	}
	return entry, nil
}

// Store exposes the backing store.
func (w *Writer) Store() Store { return w.store }

// HashClientIP returns the salted hash used in source_ip_hash fields.
func (w *Writer) HashClientIP(ip string) string { return HashIP(w.cfg.IPHashSalt, ip) }
