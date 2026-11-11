// Package pipeline implements FADE's synchronous decision path: idempotency,
// feature computation (Phase 2 ComputeFeatures), list/tier overrides, the rule
// engine, the rules-only combiner and asynchronous persistence.
package pipeline

import (
	"context"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// Reason is one explanation in the decision contract.
type Reason struct {
	Source string     `json:"source"`
	ID     string     `json:"id"`
	Text   string     `json:"text"`
	Weight int        `json:"weight,omitempty"`
	Mode   rules.Mode `json:"mode,omitempty"`
}

// Decision is the Phase 3 response contract. Internal fields carry persistence
// and query context and are never serialised to clients.
type Decision struct {
	DecisionID     string                  `json:"decision_id"`
	EventID        string                  `json:"event_id"`
	Decision       rules.Decision          `json:"decision"`
	RiskScore      int                     `json:"risk_score"`
	RuleScore      int                     `json:"rule_score"`
	MLScore        *int                    `json:"ml_score"`
	MLStatus       string                  `json:"ml_status"`
	Degraded       bool                    `json:"degraded"`
	RuleSetVersion int                     `json:"rule_set_version"`
	ModelVersion   *string                 `json:"model_version"`
	Reasons        []Reason                `json:"reasons"`
	ShadowHits     []string                `json:"shadow_hits"`
	Features       *features.FeatureVector `json:"features_snapshot,omitempty"`
	LatencyMS      float64                 `json:"latency_ms"`
	DecidedAt      time.Time               `json:"decided_at"`

	// Replay is surfaced as the Idempotent-Replay response header.
	Replay bool `json:"idempotent_replay,omitempty"`

	// Internal context (not part of the JSON contract).
	AccountID   string    `json:"-"`
	RecipientID string    `json:"-"`
	Amount      int64     `json:"-"`
	TxnType     string    `json:"-"`
	EventTime   time.Time `json:"-"`
	LatencyUS   int64     `json:"-"`
}

// Clone returns a shallow copy.
func (d *Decision) Clone() *Decision {
	if d == nil {
		return nil
	}
	cp := *d
	return &cp
}

// Options controls one decision request.
type Options struct {
	IdempotencyKey  string
	IncludeFeatures bool
	IncludeExplain  bool
	FailPolicyApply bool
	RequestID       string
}

// Config holds pipeline knobs.
type Config struct {
	RequestBudget  time.Duration
	FeatureTimeout time.Duration
	IdempotencyTTL time.Duration
	MaxConcurrency int
	TierLimits     map[int]int64
	Version        string
}

// DefaultConfig returns production defaults.
func DefaultConfig() Config {
	return Config{
		RequestBudget:  40 * time.Millisecond,
		FeatureTimeout: 20 * time.Millisecond,
		IdempotencyTTL: 24 * time.Hour,
		MaxConcurrency: 2000,
		TierLimits:     map[int]int64{1: 50_000, 2: 500_000, 3: 5_000_000},
		Version:        "0.4.0",
	}
}

// TierLimit returns the daily ceiling for a KYC tier (0 when unknown).
func (c Config) TierLimit(tier int64) int64 { return c.TierLimits[int(tier)] }

// FeatureComputer is the Phase 2 feature-store contract.
type FeatureComputer interface {
	Compute(ctx context.Context, ev *events.Event) (features.FeatureVector, features.Status, error)
}

// Error kinds exposed to HTTP mapping.
const (
	KindShed                = "SHED"
	KindIdemConflict        = "IDEMPOTENCY_CONFLICT"
	KindIdempotencyInFlight = "IDEMPOTENCY_IN_PROGRESS"
	KindInternal            = "INTERNAL"
)

// Error is a decision-path failure.
type Error struct {
	Kind    string
	Status  int
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Kind + ": " + e.Message + ": " + e.Err.Error()
	}
	return e.Kind + ": " + e.Message
}

// Unwrap exposes the cause.
func (e *Error) Unwrap() error { return e.Err }

// ErrShed is returned when the concurrency semaphore is full.
var ErrShed = &Error{Kind: KindShed, Status: 503, Message: "decision capacity exhausted"}

// ErrIdempotencyConflict means the same key was reused with a different body.
var ErrIdempotencyConflict = &Error{Kind: KindIdemConflict, Status: 422, Message: "idempotency key reused with a different payload"}

// ErrIdempotencyInFlight means a concurrent owner did not finish within 50 ms.
var ErrIdempotencyInFlight = &Error{Kind: KindIdempotencyInFlight, Status: 409, Message: "a decision for this idempotency key is still in progress"}

// Fail causes.
const (
	CauseRedis          = "REDIS_UNAVAILABLE"
	CauseFeatureTimeout = "FEATURE_TIMEOUT"
	CauseRulePanic      = "RULE_PANIC"
	CauseNoRuleSet      = "NO_RULE_SET"
)

// StoredDecision is the persisted/spooled form with the internal context.
type StoredDecision struct {
	Decision    *Decision
	AccountID   string
	RecipientID string
	Amount      int64
	TxnType     string
	EventTime   time.Time
	LatencyUS   int64
}

// ToStored wraps a decision for persistence.
func (d *Decision) ToStored() *StoredDecision {
	return &StoredDecision{
		Decision: d, AccountID: d.AccountID, RecipientID: d.RecipientID,
		Amount: d.Amount, TxnType: d.TxnType, EventTime: d.EventTime, LatencyUS: d.LatencyUS,
	}
}

// DecisionFilter filters the decision list endpoint.
type DecisionFilter struct {
	AccountID string
	Decision  string
	RuleID    string
	From, To  time.Time
	Limit     int
	Cursor    string
}

// RuleMetric is one row of GET /v1/metrics/rules.
type RuleMetric struct {
	RuleID     string  `json:"rule_id"`
	Mode       string  `json:"mode"`
	Fires      int64   `json:"fires"`
	Share      float64 `json:"share"`
	ShadowHits int64   `json:"shadow_hits"`
}

// RuleMetricResult is the endpoint payload.
type RuleMetricResult struct {
	WindowSeconds int64        `json:"window_seconds"`
	Decisions     int64        `json:"decisions"`
	Rules         []RuleMetric `json:"rules"`
}

// Store is the Postgres-backed decision/history surface. It is optional; when
// nil, decisions are produced but not queryable and duplicate lookups fall back
// to the idempotency cache.
type Store interface {
	WriteDecisions(ctx context.Context, batch []*StoredDecision) error
	GetDecision(ctx context.Context, id string) (*StoredDecision, error)
	ListDecisions(ctx context.Context, f DecisionFilter) ([]*StoredDecision, string, error)
	DecisionByEventID(ctx context.Context, eventID string) (*StoredDecision, error)
	RuleMetrics(ctx context.Context, window time.Duration) (*RuleMetricResult, error)
}

// DecisionSink accepts decisions off the hot path.
type DecisionSink interface {
	Enqueue(d *StoredDecision)
}

// DiscardSink drops decisions (tests, replay).
type DiscardSink struct{}

// Enqueue implements DecisionSink.
func (DiscardSink) Enqueue(*StoredDecision) {}
