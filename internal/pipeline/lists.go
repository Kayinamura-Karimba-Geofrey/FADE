package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// List types and subjects from the admin contract.
const (
	ListAllow = "ALLOW"
	ListBlock = "BLOCK"

	SubjectAccount = "ACCOUNT"
	SubjectDevice  = "DEVICE"
	SubjectMSISDN  = "MSISDN"
)

// ListEntry is one allow/block entry.
type ListEntry struct {
	ID        int64      `json:"id"`
	ListType  string     `json:"list_type"`
	Subject   string     `json:"subject"`
	Value     string     `json:"value"`
	Reason    string     `json:"reason,omitempty"`
	CreatedBy string     `json:"created_by,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at"`
}

// ListSource is the durable list store (Postgres, or memory in tests).
type ListSource interface {
	LoadLists(ctx context.Context) ([]ListEntry, error)
	AddList(ctx context.Context, e ListEntry) (ListEntry, error)
	DeleteList(ctx context.Context, id int64) (bool, error)
	ListEntries(ctx context.Context, listType, subject, value string, limit int) ([]ListEntry, error)
}

// MemoryListSource is the in-process list store.
type MemoryListSource struct {
	mu     sync.Mutex
	nextID int64
	items  []ListEntry
}

// NewMemoryListSource returns an empty source.
func NewMemoryListSource() *MemoryListSource { return &MemoryListSource{nextID: 1} }

// LoadLists implements ListSource.
func (m *MemoryListSource) LoadLists(context.Context) ([]ListEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]ListEntry(nil), m.items...), nil
}

// AddList implements ListSource.
func (m *MemoryListSource) AddList(_ context.Context, e ListEntry) (ListEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e.ID = m.nextID
	m.nextID++
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now().UTC()
	}
	for _, old := range m.items {
		if old.ListType == e.ListType && old.Subject == e.Subject && old.Value == e.Value {
			return old, nil
		}
	}
	m.items = append(m.items, e)
	return e, nil
}

// DeleteList implements ListSource.
func (m *MemoryListSource) DeleteList(_ context.Context, id int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, e := range m.items {
		if e.ID == id {
			m.items = append(m.items[:i], m.items[i+1:]...)
			return true, nil
		}
	}
	return false, nil
}

// ListEntries implements ListSource.
func (m *MemoryListSource) ListEntries(_ context.Context, listType, subject, value string, limit int) ([]ListEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ListEntry, 0, len(m.items))
	for _, e := range m.items {
		if listType != "" && e.ListType != listType {
			continue
		}
		if subject != "" && e.Subject != subject {
			continue
		}
		if value != "" && e.Value != value {
			continue
		}
		out = append(out, e)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// ListCache keeps active list entries in memory, refreshed every 10 seconds
// and immediately after an administrative change.
type ListCache struct {
	mu      sync.RWMutex
	source  ListSource
	ttl     time.Duration
	logger  *slog.Logger
	now     func() time.Time
	block   map[string]ListEntry
	allow   map[string]ListEntry
	refresh time.Time
}

// NewListCache builds a cache over source.
func NewListCache(source ListSource, logger *slog.Logger) *ListCache {
	if logger == nil {
		logger = slog.Default()
	}
	return &ListCache{
		source: source, ttl: 10 * time.Second, logger: logger, now: time.Now,
		block: map[string]ListEntry{}, allow: map[string]ListEntry{},
	}
}

func listKey(subject, value string) string { return subject + "\x00" + value }

// Refresh reloads all entries from the source.
func (c *ListCache) Refresh(ctx context.Context) error {
	if c.source == nil {
		return nil
	}
	entries, err := c.source.LoadLists(ctx)
	if err != nil {
		return err
	}
	now := c.now()
	block := make(map[string]ListEntry, len(entries))
	allow := make(map[string]ListEntry, len(entries))
	for _, e := range entries {
		if e.ExpiresAt != nil && !e.ExpiresAt.After(now) {
			continue
		}
		key := listKey(e.Subject, e.Value)
		if e.ListType == ListBlock {
			block[key] = e
		} else {
			allow[key] = e
		}
	}
	c.mu.Lock()
	c.block, c.allow, c.refresh = block, allow, now
	c.mu.Unlock()
	return nil
}

// Start refreshes in the background every 10 seconds until ctx is cancelled.
func (c *ListCache) Start(ctx context.Context) {
	go func() {
		_ = c.Refresh(ctx)
		ticker := time.NewTicker(c.ttl)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.Refresh(ctx); err != nil {
					c.logger.Warn("list refresh failed", "error", err)
				}
			}
		}
	}()
}

// Check returns the first block-list hit and the first allow-list hit for the
// event subjects, in ACCOUNT, DEVICE, MSISDN order.
func (c *ListCache) Check(account, device, msisdn string) (*ListEntry, *ListEntry) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := c.now()
	check := func(subject, value string) (*ListEntry, *ListEntry) {
		key := listKey(subject, value)
		var b, a *ListEntry
		if e, ok := c.block[key]; ok && (e.ExpiresAt == nil || e.ExpiresAt.After(now)) {
			cp := e
			b = &cp
		}
		if e, ok := c.allow[key]; ok && (e.ExpiresAt == nil || e.ExpiresAt.After(now)) {
			cp := e
			a = &cp
		}
		return b, a
	}
	var allow *ListEntry
	for _, s := range []struct{ subject, value string }{
		{SubjectAccount, account}, {SubjectDevice, device}, {SubjectMSISDN, msisdn},
	} {
		if s.value == "" {
			continue
		}
		b, a := check(s.subject, s.value)
		if b != nil {
			return b, allow
		}
		if allow == nil {
			allow = a
		}
	}
	return nil, allow
}

// Add writes through the source and invalidates the cache.
func (c *ListCache) Add(ctx context.Context, e ListEntry) (ListEntry, error) {
	created, err := c.source.AddList(ctx, e)
	if err != nil {
		return ListEntry{}, err
	}
	_ = c.Refresh(ctx)
	return created, nil
}

// Delete removes an entry and invalidates the cache.
func (c *ListCache) Delete(ctx context.Context, id int64) (bool, error) {
	ok, err := c.source.DeleteList(ctx, id)
	if err != nil {
		return false, err
	}
	_ = c.Refresh(ctx)
	return ok, nil
}

// Entries lists raw entries for the admin API.
func (c *ListCache) Entries(ctx context.Context, listType, subject, value string, limit int) ([]ListEntry, error) {
	return c.source.ListEntries(ctx, listType, subject, value, limit)
}
