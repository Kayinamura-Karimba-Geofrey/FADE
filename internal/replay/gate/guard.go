package gate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// ErrGateRequired is returned when an activation is attempted without a passing
// replay gate for the candidate configuration.
var ErrGateRequired = errors.New("gate: no passing promotion gate for this configuration")

// BreakGlassKey is the context key used to carry a break-glass reason.
type BreakGlassKey struct{}

// WithBreakGlass attaches a break-glass reason to ctx.
func WithBreakGlass(ctx context.Context, reason string) context.Context {
	return context.WithValue(ctx, BreakGlassKey{}, reason)
}

// BreakGlassReason returns the break-glass reason stored on ctx, if any.
func BreakGlassReason(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(BreakGlassKey{}).(string)
	if !ok || v == "" {
		return "", false
	}
	return v, true
}

// Verdict is a stored promotion-gate outcome.
type Verdict struct {
	JobID     int64
	RulesHash string
	Model     string
	Passed    bool
	CreatedAt time.Time
}

// VerdictStore looks up the newest promotion-gate result for a configuration.
type VerdictStore interface {
	Latest(ctx context.Context, rulesHash string) (Verdict, bool, error)
}

// Guard enforces that a passing gate exists before a rule set is activated.
type Guard struct {
	Store           VerdictStore
	Required        bool
	AllowBreakGlass bool
}

// Check evaluates whether activation is permitted for the given rule set.
func (g *Guard) Check(ctx context.Context, rs *rules.RuleSet) error {
	if rs == nil {
		return nil
	}
	rulesHash := rs.Hash
	if g == nil || !g.Required {
		return nil
	}
	if reason, ok := BreakGlassReason(ctx); ok {
		if !g.AllowBreakGlass {
			return fmt.Errorf("gate: break-glass disabled")
		}
		if reason == "" {
			return fmt.Errorf("gate: break-glass requires a reason")
		}
		return nil
	}
	if g.Store == nil {
		return fmt.Errorf("gate: required but no verdict store configured")
	}
	v, ok, err := g.Store.Latest(ctx, rulesHash)
	if err != nil {
		return fmt.Errorf("gate: lookup failed: %w", err)
	}
	if !ok || !v.Passed {
		return ErrGateRequired
	}
	return nil
}
