package rules

import (
	"fmt"
	"math"
	"sort"
)

// Interpret is the reference interpreter for one condition. It walks the
// authored tree and allocates; it is only used by property tests and previews.
func Interpret(c *Condition, ctx *EvalContext) (bool, error) {
	if c.Not != nil {
		ok, err := Interpret(c.Not, ctx)
		if err != nil {
			return false, err
		}
		return !ok, nil
	}
	if len(c.All) > 0 {
		for i := range c.All {
			ok, err := Interpret(&c.All[i], ctx)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
		}
		return true, nil
	}
	if len(c.Any) > 0 {
		for i := range c.Any {
			ok, err := Interpret(&c.Any[i], ctx)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}
	field, ok := Fields[c.F]
	if !ok {
		return false, fmt.Errorf("unknown identifier %q", c.F)
	}
	v, err := rawValue(c.V)
	if err != nil {
		return false, err
	}

	switch field.Kind {
	case KindInt:
		got := field.Int(ctx)
		if c.Op == "in" || c.Op == "not_in" {
			list, ok := v.([]any)
			if !ok {
				return false, fmt.Errorf("expected list")
			}
			found := false
			for _, item := range list {
				w, err := interpretInt(item)
				if err != nil {
					return false, err
				}
				if got == w {
					found = true
				}
			}
			if c.Op == "in" {
				return found, nil
			}
			return !found, nil
		}
		w, err := interpretInt(v)
		if err != nil {
			return false, err
		}
		return compareInt(got, c.Op, w), nil

	case KindFloat:
		got := field.Float(ctx)
		if c.Op == "in" || c.Op == "not_in" {
			list, ok := v.([]any)
			if !ok {
				return false, fmt.Errorf("expected list")
			}
			found := false
			for _, item := range list {
				w, err := interpretFloat(item)
				if err != nil {
					return false, err
				}
				if got == w {
					found = true
				}
			}
			if c.Op == "in" {
				return found, nil
			}
			return !found, nil
		}
		w, err := interpretFloat(v)
		if err != nil {
			return false, err
		}
		return compareFloat(got, c.Op, w), nil

	case KindBool:
		got := field.Bool(ctx)
		if c.Op == "in" || c.Op == "not_in" {
			list, ok := v.([]any)
			if !ok {
				return false, fmt.Errorf("expected list")
			}
			found := false
			for _, item := range list {
				w, ok := item.(bool)
				if !ok {
					return false, fmt.Errorf("expected bool")
				}
				if got == w {
					found = true
				}
			}
			if c.Op == "in" {
				return found, nil
			}
			return !found, nil
		}
		w, ok := v.(bool)
		if !ok {
			return false, fmt.Errorf("expected bool")
		}
		if c.Op == "==" {
			return got == w, nil
		}
		return got != w, nil

	case KindString:
		got := field.Str(ctx)
		if c.Op == "in" || c.Op == "not_in" {
			list, ok := v.([]any)
			if !ok {
				return false, fmt.Errorf("expected list")
			}
			found := false
			for _, item := range list {
				w, ok := item.(string)
				if !ok {
					return false, fmt.Errorf("expected string")
				}
				if got == w {
					found = true
				}
			}
			if c.Op == "in" {
				return found, nil
			}
			return !found, nil
		}
		w, ok := v.(string)
		if !ok {
			return false, fmt.Errorf("expected string")
		}
		if c.Op == "==" {
			return got == w, nil
		}
		return got != w, nil
	}
	return false, fmt.Errorf("unsupported kind")
}

func interpretInt(v any) (int64, error) {
	f, integral, ok := parseNumberLike(v)
	if !ok || !integral || math.Trunc(f) != f {
		return 0, fmt.Errorf("expected integer, got %v", v)
	}
	return int64(f), nil
}

func interpretFloat(v any) (float64, error) {
	f, _, ok := parseNumberLike(v)
	if !ok {
		return 0, fmt.Errorf("expected number, got %v", v)
	}
	return f, nil
}

func compareInt(got int64, op string, want int64) bool {
	switch op {
	case "==":
		return got == want
	case "!=":
		return got != want
	case ">":
		return got > want
	case ">=":
		return got >= want
	case "<":
		return got < want
	default:
		return got <= want
	}
}

func compareFloat(got float64, op string, want float64) bool {
	switch op {
	case "==":
		return got == want
	case "!=":
		return got != want
	case ">":
		return got > want
	case ">=":
		return got >= want
	case "<":
		return got < want
	default:
		return got <= want
	}
}

// InterpretRule checks one rule including enabled/applies_to.
func InterpretRule(r *Rule, ctx *EvalContext) (bool, error) {
	if r.Enabled == nil || !*r.Enabled {
		return false, nil
	}
	if len(r.AppliesTo) > 0 {
		found := false
		for _, t := range r.AppliesTo {
			if t == ctx.TxnType {
				found = true
				break
			}
		}
		if !found {
			return false, nil
		}
	}
	return Interpret(&r.When, ctx)
}

// InterpretFile is the allocation-tolerant reference evaluation of a whole
// document. Its score, action and fired-id set must equal RuleSet.Evaluate.
func InterpretFile(f *RuleSetFile, ctx *EvalContext) (Result, error) {
	var res Result
	res.RuleSetVersion = f.Version
	action := Allow
	for i := range f.Rules {
		r := &f.Rules[i]
		ok, err := InterpretRule(r, ctx)
		if err != nil {
			return res, fmt.Errorf("rule %s: %w", r.ID, err)
		}
		if !ok {
			continue
		}
		if r.Mode == Shadow {
			res.Shadow[res.ShadowCount] = r.ID
			res.ShadowCount++
			continue
		}
		res.Fired[res.FiredCount].Rule = &CompiledRule{ID: r.ID, Weight: *r.Weight, Action: r.Action}
		res.FiredCount++
		res.RuleScore += *r.Weight
		if r.Action.Severity() > action.Severity() {
			action = r.Action
		}
	}
	if res.RuleScore > 100 {
		res.RuleScore = 100
	}
	res.RuleAction = action
	return res, nil
}

// FiredIDsSorted returns enforce rule ids sorted, for equivalence assertions.
func (r *Result) FiredIDsSorted() []string {
	ids := make([]string, 0, r.FiredCount)
	for i := 0; i < r.FiredCount; i++ {
		ids = append(ids, r.Fired[i].Rule.ID)
	}
	sort.Strings(ids)
	return ids
}
