// Package rules implements FADE's hot-reloadable, rules-only decision engine.
//
// A rule set is authored as JSON, validated and compiled once into typed
// closures over feature accessors. Evaluation performs no map lookups, uses no
// reflection and makes no heap allocations; hot reload swaps whole immutable
// rule sets through an atomic pointer.
package rules

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
)

// Decision is the terminal outcome for a transaction.
type Decision string

// Decisions ordered by increasing severity.
const (
	Allow   Decision = "ALLOW"
	StepUp  Decision = "STEP_UP"
	Hold    Decision = "HOLD"
	Block   Decision = "BLOCK"
	Unknown Decision = ""
)

// Severity ranks decisions for max().
func (d Decision) Severity() int {
	switch d {
	case Allow:
		return 0
	case StepUp:
		return 1
	case Hold:
		return 2
	case Block:
		return 3
	default:
		return -1
	}
}

// Valid reports whether d is one of the four contract decisions.
func (d Decision) Valid() bool { return d.Severity() >= 0 }

// MaxDecision returns the more severe of a and b.
func MaxDecision(a, b Decision) Decision {
	if a.Severity() >= b.Severity() {
		return a
	}
	return b
}

// Mode selects whether a rule contributes to the risk score.
type Mode string

// Rule modes.
const (
	Enforce Mode = "enforce"
	Shadow  Mode = "shadow"
)

// Limits bound compilation and the fixed-size result.
const (
	MaxRules         = 256
	MaxDepth         = 6
	MaxConditions    = 64
	MaxListValues    = 32
	MaxReasonLen     = 280
	MaxFailModeValue = 8
)

// Thresholds are score cut-offs. A score at or above a threshold maps to that
// decision; the highest crossed threshold wins.
type Thresholds struct {
	StepUp int `json:"step_up"`
	Hold   int `json:"hold"`
	Block  int `json:"block"`
}

// Floors raise the risk score to the floor of a rule action. Nil means "use the
// matching threshold".
type Floors struct {
	StepUp *int `json:"STEP_UP,omitempty"`
	Hold   *int `json:"HOLD,omitempty"`
	Block  *int `json:"BLOCK,omitempty"`
}

// CombinerConfig is the authored combiner configuration.
type CombinerConfig struct {
	Thresholds *Thresholds `json:"thresholds,omitempty"`
	Floors     *Floors     `json:"floors,omitempty"`
}

// Config is the authored rule-set configuration.
type Config struct {
	Combiner CombinerConfig    `json:"combiner"`
	FailMode map[string]string `json:"fail_mode,omitempty"`
}

// Combiner is the compiled, validated combiner configuration.
type Combiner struct {
	StepUpThreshold int
	HoldThreshold   int
	BlockThreshold  int
	StepUpFloor     int
	HoldFloor       int
	BlockFloor      int
}

// DefaultCombiner is the documented Phase 3 policy.
func DefaultCombiner() Combiner {
	return Combiner{
		StepUpThreshold: 40, HoldThreshold: 65, BlockThreshold: 85,
		StepUpFloor: 40, HoldFloor: 65, BlockFloor: 85,
	}
}

// Decide maps a risk score to a decision.
func (c Combiner) Decide(score int) Decision {
	switch {
	case score >= c.BlockThreshold:
		return Block
	case score >= c.HoldThreshold:
		return Hold
	case score >= c.StepUpThreshold:
		return StepUp
	default:
		return Allow
	}
}

// Floor returns the configured score floor for an action.
func (c Combiner) Floor(d Decision) int {
	switch d {
	case Block:
		return c.BlockFloor
	case Hold:
		return c.HoldFloor
	case StepUp:
		return c.StepUpFloor
	default:
		return 0
	}
}

// Apply combines rule score and rule action into the final rules-only score and
// decision. The action is authoritative even with inconsistent floors.
func (c Combiner) Apply(score int, action Decision) (int, Decision) {
	if f := c.Floor(action); f > score {
		score = f
	}
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	return score, MaxDecision(c.Decide(score), action)
}

// Fail modes.
const (
	FailOpen   = "open"
	FailClosed = "closed"
	FailStepUp = "step_up"
)

// Condition is one node of a rule predicate tree. Exactly one form is used:
// a comparison (F set) or a group (All/Any/Not set).
type Condition struct {
	F   string          `json:"f,omitempty"`
	Op  string          `json:"op,omitempty"`
	V   json.RawMessage `json:"v,omitempty"`
	All []Condition     `json:"all,omitempty"`
	Any []Condition     `json:"any,omitempty"`
	Not *Condition      `json:"not,omitempty"`
}

// Rule is one authored rule.
type Rule struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Enabled   *bool     `json:"enabled"`
	Mode      Mode      `json:"mode"`
	Weight    *int      `json:"weight"`
	Action    Decision  `json:"action"`
	When      Condition `json:"when"`
	Reason    string    `json:"reason"`
	Tags      []string  `json:"tags,omitempty"`
	AppliesTo []string  `json:"applies_to,omitempty"`
	Stateless bool      `json:"stateless,omitempty"`
}

// RuleSetFile is the on-disk JSON contract.
type RuleSetFile struct {
	RuleSet   string `json:"rule_set"`
	Version   int    `json:"version"`
	Author    string `json:"author"`
	Changelog string `json:"changelog,omitempty"`
	Config    Config `json:"config"`
	Rules     []Rule `json:"rules"`
}

// FieldKind is the value domain of an identifier in the rule DSL.
type FieldKind uint8

// Identifier kinds.
const (
	KindInt FieldKind = iota
	KindFloat
	KindBool
	KindString
)

// String implements fmt.Stringer for documentation and errors.
func (k FieldKind) String() string {
	switch k {
	case KindInt:
		return "int"
	case KindFloat:
		return "float"
	case KindBool:
		return "bool"
	case KindString:
		return "string"
	default:
		return "unknown"
	}
}

// Field describes one addressable identifier and its zero-allocation accessor.
type Field struct {
	Name  string
	Kind  FieldKind
	Raw   bool // derived from the event, not from the feature vector
	Int   func(*EvalContext) int64
	Float func(*EvalContext) float64
	Bool  func(*EvalContext) bool
	Str   func(*EvalContext) string
}

// EvalContext carries the feature vector plus the raw event fields the DSL
// exposes. It is passed by pointer so accessor closures never copy a vector.
type EvalContext struct {
	Vector        *features.FeatureVector
	Amount        int64
	Fee           int64
	TxnType       string
	Channel       string
	KYCTier       int64
	BalanceBefore int64
}

// BalanceBeforeRatio is amount / balance_before, 0 when the balance is 0.
func (c *EvalContext) BalanceBeforeRatio() float64 {
	if c.BalanceBefore <= 0 {
		return 0
	}
	return float64(c.Amount) / float64(c.BalanceBefore)
}

// ValidationError is one compile-time failure with a JSON-path-like location.
type ValidationError struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

func (e ValidationError) Error() string { return e.Path + ": " + e.Message }

// CompileError aggregates every validation failure for one rule set.
type CompileError struct {
	Errors []ValidationError
}

func (e *CompileError) Error() string {
	parts := make([]string, 0, len(e.Errors))
	for _, v := range e.Errors {
		parts = append(parts, v.Error())
	}
	return fmt.Sprintf("invalid rule set: %s", strings.Join(parts, "; "))
}

// Snapshot is immutable rule-set metadata for the admin API.
type Snapshot struct {
	RuleSet    string `json:"rule_set"`
	Version    int    `json:"version"`
	Hash       string `json:"hash"`
	Rules      int    `json:"rules"`
	Active     bool   `json:"active"`
	Author     string `json:"author,omitempty"`
	Changelog  string `json:"changelog,omitempty"`
	CompiledAt string `json:"compiled_at,omitempty"`
	Source     string `json:"source,omitempty"`
}

// Reason is one explanation attached to a decision.
type Reason struct {
	Source string   `json:"source"`
	ID     string   `json:"id"`
	Text   string   `json:"text"`
	Weight int      `json:"weight,omitempty"`
	Mode   Mode     `json:"mode,omitempty"`
	Action Decision `json:"-"`
}

// FiredRule is a pointer into the immutable rule set; it never allocates.
type FiredRule struct {
	Rule *CompiledRule
}

// Result is the reusable evaluator output. Callers reuse one Result per worker
// so evaluation never allocates. The arrays are fixed at MaxRules.
type Result struct {
	RuleScore      int
	RuleAction     Decision
	RuleSetVersion int
	Fired          [MaxRules]FiredRule
	FiredCount     int
	Shadow         [MaxRules]string
	ShadowCount    int
	Truncated      bool
	// Failed is set by EvaluateSafe when a rule panicked. Evaluate itself
	// never sets it.
	Failed bool
	// ctx keeps the evaluator context on the caller-owned Result. Evaluate
	// stores a copy here so predicate closures receive a stable pointer and
	// the hot path does not allocate a context per request.
	ctx EvalContext
}

// Reset clears r for the next evaluation. Only the used prefix holds live
// pointers; clearing all 512 fixed slots on every request would cost more than
// evaluating a small rule set. Counts are authoritative and always reset.
func (r *Result) Reset() {
	for i := 0; i < r.FiredCount; i++ {
		r.Fired[i].Rule = nil
	}
	for i := 0; i < r.ShadowCount; i++ {
		r.Shadow[i] = ""
	}
	r.ctx = EvalContext{}
	r.RuleScore = 0
	r.RuleAction = Allow
	r.RuleSetVersion = 0
	r.FiredCount = 0
	r.ShadowCount = 0
	r.Truncated = false
	r.Failed = false
}

// HasBlock reports whether an enforce rule returned BLOCK.
func (r *Result) HasBlock() bool { return r.RuleAction == Block }

// Fingerprint is a stable description used in golden tests.
func (r *Result) Fingerprint() string {
	ids := make([]string, 0, r.FiredCount)
	for i := 0; i < r.FiredCount; i++ {
		ids = append(ids, r.Fired[i].Rule.ID)
	}
	sort.Strings(ids)
	return fmt.Sprintf("%d|%s|%s", r.RuleScore, r.RuleAction, strings.Join(ids, ","))
}
