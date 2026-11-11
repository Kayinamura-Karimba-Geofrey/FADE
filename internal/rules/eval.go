package rules

import (
	"errors"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
)

// ErrRulePanic is returned by EvaluateSafe when a predicate panicked.
var ErrRulePanic = errors.New("rule evaluation panicked")

// evalHook is a fault-injection seam used by the rollback integration test.
// It is nil in production and adds one predictable branch to Evaluate.
var evalHook func(EvalContext)

// SetEvalHook installs a fault-injection hook. Tests only. The context is
// passed by value so installing a hook does not force it onto the heap.
func SetEvalHook(f func(EvalContext)) { evalHook = f }

// Evaluate runs the rule set for one transaction type and fills out.
//
// It performs zero heap allocations: rules are pre-indexed per transaction
// type, predicates are closures over their thresholds, and fired rules are
// stored as pointers in the caller-provided fixed-size array.
func (rs *RuleSet) Evaluate(fv *features.FeatureVector, ctx EvalContext, out *Result) {
	if evalHook != nil {
		evalHook(ctx)
	}
	out.Reset()
	ctx.Vector = fv
	out.ctx = ctx
	out.RuleSetVersion = rs.Version

	rules := rs.byTxn[ctx.TxnType]
	if rules == nil {
		rules = rs.byTxn[""]
	}
	action := Allow
	score := 0
	for _, r := range rules {
		if !r.match(&out.ctx) {
			continue
		}
		if r.Mode == Shadow {
			if out.ShadowCount < MaxRules {
				out.Shadow[out.ShadowCount] = r.ID
				out.ShadowCount++
			} else {
				out.Truncated = true
			}
			continue
		}
		if out.FiredCount < MaxRules {
			out.Fired[out.FiredCount].Rule = r
			out.FiredCount++
		} else {
			out.Truncated = true
		}
		score += r.Weight
		if sev := r.Action.Severity(); sev > action.Severity() {
			action = r.Action
		}
	}
	if score > 100 {
		score = 100
	}
	out.RuleScore = score
	out.RuleAction = action
}

// EvaluateSafe is Evaluate with per-request panic recovery. A panic becomes an
// error handled by the caller's fail policy; it never crashes the process.
func (rs *RuleSet) EvaluateSafe(fv *features.FeatureVector, ctx EvalContext, out *Result) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			out.Failed = true
			err = ErrRulePanic
		}
	}()
	rs.Evaluate(fv, ctx, out)
	return nil
}

// EvaluateNew is a convenience for tests and previews; it allocates a Result.
func (rs *RuleSet) EvaluateNew(fv *features.FeatureVector, ctx EvalContext) Result {
	var res Result
	rs.Evaluate(fv, ctx, &res)
	return res
}
