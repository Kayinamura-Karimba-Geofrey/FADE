package api

import (
	"net/http"
	"sync"
	"time"
)

// RateLimit is one role's token-bucket setting. Zero disables limiting.
type RateLimit struct {
	PerSecond float64
	Burst     float64
}

type bucket struct {
	tokens float64
	last   time.Time
}

// RateLimiter applies a per-role token bucket.
type RateLimiter struct {
	mu      sync.Mutex
	limits  map[string]RateLimit
	buckets map[string]*bucket
	now     func() time.Time
}

// NewRateLimiter builds a limiter. Missing roles are unlimited.
func NewRateLimiter(limits map[string]RateLimit) *RateLimiter {
	return &RateLimiter{limits: limits, buckets: map[string]*bucket{}, now: time.Now}
}

// Allow consumes one token for role.
func (l *RateLimiter) Allow(role string) bool {
	limit, ok := l.limits[role]
	if !ok || limit.PerSecond <= 0 || limit.Burst <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b, ok := l.buckets[role]
	if !ok {
		b = &bucket{tokens: limit.Burst, last: now}
		l.buckets[role] = b
	}
	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed * limit.PerSecond
	if b.tokens > limit.Burst {
		b.tokens = limit.Burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Middleware rejects over-limit callers with 429.
func (l *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := PrincipalFrom(r.Context())
		role := ""
		if p != nil {
			role = p.Role
		}
		if !l.Allow(role) {
			w.Header().Set("Retry-After", "1")
			writeProblem(w, r, http.StatusTooManyRequests, "RATE_LIMITED", "Rate limited",
				"per-role request budget exhausted")
			return
		}
		next.ServeHTTP(w, r)
	})
}
