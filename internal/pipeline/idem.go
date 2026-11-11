package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	idemPendingTTL = 5 * time.Second
	idemPollWindow = 50 * time.Millisecond
	idemPollEvery  = 2 * time.Millisecond
)

type idemEnvelope struct {
	Status   string          `json:"s"` // "pending" or "done"
	Hash     string          `json:"h"`
	Decision json.RawMessage `json:"d,omitempty"`
}

// Idempotency reserves a key before feature state is updated, so concurrent
// duplicates never double-update windows.
type Idempotency interface {
	// Begin returns a stored decision when the key is already complete, or
	// owner=true when the caller owns the reservation.
	Begin(ctx context.Context, key, hash string) (decision []byte, owner bool, err error)
	Complete(ctx context.Context, key, hash string, decision []byte, ttl time.Duration) error
	Get(ctx context.Context, key string) (decision []byte, ok bool, err error)
}

var beginScript = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if v then return v end
local ok = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
if ok then return '__OWNER__' end
return redis.call('GET', KEYS[1]) or ''
`)

// RedisIdempotency stores 24 h decision JSON under idem:{key}.
type RedisIdempotency struct {
	client redis.UniversalClient
}

// NewRedisIdempotency wraps a Redis client.
func NewRedisIdempotency(client redis.UniversalClient) *RedisIdempotency {
	return &RedisIdempotency{client: client}
}

// Begin implements Idempotency.
func (r *RedisIdempotency) Begin(ctx context.Context, key, hash string) ([]byte, bool, error) {
	pending, err := json.Marshal(idemEnvelope{Status: "pending", Hash: hash})
	if err != nil {
		return nil, false, err
	}
	deadline := time.Now().Add(idemPollWindow)
	for attempt := 0; attempt < 4; attempt++ {
		raw, err := beginScript.Run(ctx, r.client, []string{"idem:" + key},
			pending, idemPendingTTL.Milliseconds()).Text()
		if err != nil {
			return nil, false, err
		}
		if raw == "__OWNER__" {
			return nil, true, nil
		}
		if raw == "" {
			continue
		}
		var env idemEnvelope
		if err := json.Unmarshal([]byte(raw), &env); err != nil {
			return nil, false, err
		}
		if env.Hash != hash {
			return nil, false, ErrIdempotencyConflict
		}
		if env.Status == "done" {
			return env.Decision, false, nil
		}
		// Another request owns the reservation: poll briefly for the decision.
		for time.Now().Before(deadline) {
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case <-time.After(idemPollEvery):
			}
			got, ok, err := r.Get(ctx, key)
			if err != nil {
				return nil, false, err
			}
			if ok {
				return got, false, nil
			}
		}
		return nil, false, ErrIdempotencyInFlight
	}
	return nil, false, ErrIdempotencyInFlight
}

// Complete stores the decision and extends the key to the 24 h TTL.
func (r *RedisIdempotency) Complete(ctx context.Context, key, hash string, decision []byte, ttl time.Duration) error {
	raw, err := json.Marshal(idemEnvelope{Status: "done", Hash: hash, Decision: decision})
	if err != nil {
		return err
	}
	return r.client.Set(ctx, "idem:"+key, raw, ttl).Err()
}

// Get returns a completed decision.
func (r *RedisIdempotency) Get(ctx context.Context, key string) ([]byte, bool, error) {
	raw, err := r.client.Get(ctx, "idem:"+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var env idemEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, false, err
	}
	if env.Status != "done" {
		return nil, false, nil
	}
	return env.Decision, true, nil
}

// MemoryIdempotency is the in-process fallback and test double.
type MemoryIdempotency struct {
	mu   sync.Mutex
	data map[string]*memIdem
	now  func() time.Time
}

type memIdem struct {
	hash     string
	decision []byte
	pending  bool
	expires  time.Time
	done     chan struct{}
}

// NewMemoryIdempotency builds an empty store.
func NewMemoryIdempotency() *MemoryIdempotency {
	return &MemoryIdempotency{data: map[string]*memIdem{}, now: time.Now}
}

// Begin implements Idempotency.
func (m *MemoryIdempotency) Begin(ctx context.Context, key, hash string) ([]byte, bool, error) {
	deadline := time.Now().Add(idemPollWindow)
	for {
		m.mu.Lock()
		m.expireLocked()
		entry, ok := m.data[key]
		if !ok {
			entry = &memIdem{hash: hash, pending: true, expires: time.Now().Add(idemPendingTTL), done: make(chan struct{})}
			m.data[key] = entry
			m.mu.Unlock()
			return nil, true, nil
		}
		if entry.hash != hash {
			m.mu.Unlock()
			return nil, false, ErrIdempotencyConflict
		}
		if !entry.pending {
			d := entry.decision
			m.mu.Unlock()
			return d, false, nil
		}
		done := entry.done
		m.mu.Unlock()
		wait := time.Until(deadline)
		if wait <= 0 {
			return nil, false, ErrIdempotencyInFlight
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, false, ctx.Err()
		case <-done:
			timer.Stop()
		case <-timer.C:
			return nil, false, ErrIdempotencyInFlight
		}
	}
}

// Complete implements Idempotency.
func (m *MemoryIdempotency) Complete(_ context.Context, key, hash string, decision []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.data[key]
	if !ok {
		entry = &memIdem{hash: hash, done: make(chan struct{})}
		m.data[key] = entry
	}
	if entry.hash != hash {
		return ErrIdempotencyConflict
	}
	entry.pending = false
	entry.decision = decision
	entry.expires = time.Now().Add(ttl)
	select {
	case <-entry.done:
	default:
		close(entry.done)
	}
	return nil
}

// Get implements Idempotency.
func (m *MemoryIdempotency) Get(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLocked()
	entry, ok := m.data[key]
	if !ok || entry.pending {
		return nil, false, nil
	}
	return entry.decision, true, nil
}

func (m *MemoryIdempotency) expireLocked() {
	now := m.now()
	for k, v := range m.data {
		if now.After(v.expires) {
			if v.pending {
				close(v.done)
			}
			delete(m.data, k)
		}
	}
}
