package audit

import (
	"context"
	"sync"
)

// MemoryStore is an in-memory Store used by unit tests, the CLI self-check and
// the reproduction sample when no database is configured. It enforces exactly
// the same chain rules as Postgres.
type MemoryStore struct {
	mu          sync.Mutex
	entries     []Entry
	checkpoints []Checkpoint
	writable    bool
}

// NewMemoryStore returns an empty chain.
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

// Append implements Store.
func (m *MemoryStore) Append(_ context.Context, row Row) (Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	prev, seq := GenesisHash, int64(1)
	if len(m.entries) > 0 {
		last := m.entries[len(m.entries)-1]
		prev, seq = last.Hash, last.Seq+1
	}
	e, err := BuildEntry(seq, prev, row)
	if err != nil {
		return Entry{}, err
	}
	m.entries = append(m.entries, e)
	return e, nil
}

// Head implements Store.
func (m *MemoryStore) Head(context.Context) (Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) == 0 {
		return GenesisEntry(), nil
	}
	return m.entries[len(m.entries)-1], nil
}

// Latest implements Store.
func (m *MemoryStore) Latest(context.Context) (Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) == 0 {
		return Entry{}, ErrChainEmpty
	}
	return m.entries[len(m.entries)-1], nil
}

// AtSeq implements Store.
func (m *MemoryStore) AtSeq(_ context.Context, seq int64) (Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if seq == 0 {
		return GenesisEntry(), nil
	}
	for _, e := range m.entries {
		if e.Seq == seq {
			return e, nil
		}
	}
	return Entry{}, ErrSeqNotFound
}

// CountTo implements Store.
func (m *MemoryStore) CountTo(_ context.Context, seq int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, e := range m.entries {
		if e.Seq <= seq {
			n++
		}
	}
	return n, nil
}

// Count implements Store.
func (m *MemoryStore) Count(context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.entries)), nil
}

// Range implements Store.
func (m *MemoryStore) Range(ctx context.Context, from, to int64, fn func(Entry) error) error {
	m.mu.Lock()
	batch := make([]Entry, 0, len(m.entries))
	for _, e := range m.entries {
		if e.Seq < from || (to != 0 && e.Seq > to) {
			continue
		}
		batch = append(batch, e)
	}
	m.mu.Unlock()
	for _, e := range batch {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

// List implements Store.
func (m *MemoryStore) List(_ context.Context, f Filter) ([]Entry, int64, error) {
	validateFilter(&f)
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Entry, 0, f.Limit)
	var next int64
	for _, e := range m.entries {
		if e.Seq <= f.AfterSeq {
			continue
		}
		if f.Actor != "" && e.Row.Actor != f.Actor {
			continue
		}
		if f.Action != "" && e.Row.Action != f.Action {
			continue
		}
		if f.ObjectType != "" && e.Row.ObjectType != f.ObjectType {
			continue
		}
		if f.ObjectID != "" && e.Row.ObjectID != f.ObjectID {
			continue
		}
		if !f.From.IsZero() && mustParseAt(e.Row.At).Before(f.From) {
			continue
		}
		if !f.To.IsZero() && mustParseAt(e.Row.At).After(f.To) {
			continue
		}
		if len(out) == f.Limit {
			next = out[len(out)-1].Seq
			break
		}
		out = append(out, e)
	}
	return out, next, nil
}

// InsertCheckpoint implements Store.
func (m *MemoryStore) InsertCheckpoint(_ context.Context, cp Checkpoint, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkpoints = append(m.checkpoints, cp)
	return nil
}

// LatestCheckpoint implements Store.
func (m *MemoryStore) LatestCheckpoint(context.Context) (*Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.checkpoints) == 0 {
		return nil, nil
	}
	cp := m.checkpoints[len(m.checkpoints)-1]
	return &cp, nil
}

// Checkpoints implements Store.
func (m *MemoryStore) Checkpoints(_ context.Context, limit int) ([]Checkpoint, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Checkpoint, 0, limit)
	for i := len(m.checkpoints) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, m.checkpoints[i])
	}
	return out, nil
}

// Writable implements Store. The memory store is always writable; the real
// permission check lives in Postgres.
func (m *MemoryStore) Writable(context.Context) (bool, error) { return m.writable, nil }

// SetWritable toggles the reported role capability (tests).
func (m *MemoryStore) SetWritable(v bool) { m.writable = v }

// Entries returns a copy of the chain (tests).
func (m *MemoryStore) Entries() []Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Entry, len(m.entries))
	copy(out, m.entries)
	return out
}
