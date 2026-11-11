package rules

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strings"
)

// RuleSet is a validated, compiled, immutable rule set. It is safe for
// concurrent evaluation; hot reload swaps the whole object.
type RuleSet struct {
	RuleSet    string
	Version    int
	Author     string
	Changelog  string
	Hash       string
	Combiner   Combiner
	FailMode   map[string]string
	CompiledAt string
	Empty      bool

	file   RuleSetFile
	rules  []*CompiledRule
	byTxn  map[string][]*CompiledRule
	reason map[string]*CompiledRule
}

// CompiledRule is the executable form of one authored rule.
type CompiledRule struct {
	ID        string
	Name      string
	Mode      Mode
	Weight    int
	Action    Decision
	Stateless bool
	TxnTypes  []string
	Tags      []string
	Reason    *ReasonTemplate
	Cost      int
	match     func(*EvalContext) bool
}

// Match reports whether the predicate holds. It is exported for previews and
// tests; the hot path calls the same closure through Evaluate.
func (r *CompiledRule) Match(ctx *EvalContext) bool { return r.match(ctx) }

// Enabled reports whether the rule participates in evaluation.
func (r *CompiledRule) Enabled() bool { return r.match != nil }

// File returns the normalized authoring form.
func (r *RuleSet) File() RuleSetFile {
	out := r.file
	out.Rules = make([]Rule, len(r.file.Rules))
	copy(out.Rules, r.file.Rules)
	return out
}

// Rules returns the compiled rules in evaluation order.
func (r *RuleSet) Rules() []*CompiledRule {
	out := make([]*CompiledRule, len(r.rules))
	copy(out, r.rules)
	return out
}

// RulesFor returns the precomputed evaluation slice for a transaction type.
func (r *RuleSet) RulesFor(txnType string) []*CompiledRule {
	if list, ok := r.byTxn[txnType]; ok {
		return list
	}
	return r.byTxn[""]
}

// Snapshot returns immutable metadata for the admin API.
func (r *RuleSet) Snapshot(active bool, source string) Snapshot {
	return Snapshot{
		RuleSet:    r.RuleSet,
		Version:    r.Version,
		Hash:       r.Hash,
		Rules:      len(r.rules),
		Active:     active,
		Author:     r.Author,
		Changelog:  r.Changelog,
		CompiledAt: r.CompiledAt,
		Source:     source,
	}
}

var (
	idPattern      = regexp.MustCompile(`^R-[A-Z]{2,6}-[0-9]{3}$`)
	setNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{1,31}$`)
	tagPattern     = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)
	reasonToken    = regexp.MustCompile(`\{([^{}]*)\}`)
)

// TxnTypes is the frozen Phase 1 transaction-type set.
var TxnTypes = []string{
	"P2P_SEND", "CASH_IN", "CASH_OUT", "AIRTIME", "BILL_PAY", "MERCHANT_PAY", "BANK_TRANSFER",
}

func validTxnType(s string) bool {
	for _, t := range TxnTypes {
		if t == s {
			return true
		}
	}
	return false
}

// EmptyRuleSet is the allow-all set used when no valid rules exist. It keeps
// the pipeline alive in degraded mode.
func EmptyRuleSet() *RuleSet {
	rs := &RuleSet{
		RuleSet:    "empty",
		Version:    0,
		Author:     "system",
		Combiner:   DefaultCombiner(),
		FailMode:   map[string]string{"default": FailStepUp},
		CompiledAt: "",
		Empty:      true,
		byTxn:      map[string][]*CompiledRule{"": nil},
		reason:     map[string]*CompiledRule{},
	}
	for _, t := range TxnTypes {
		rs.byTxn[t] = nil
	}
	return rs
}

// Load parses and compiles a rule-set document.
func Load(data []byte) (*RuleSet, error) {
	var f RuleSetFile
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(&f); err != nil {
		return nil, &CompileError{Errors: []ValidationError{{Path: "$", Message: "invalid JSON: " + err.Error()}}}
	}
	if err := ensureEOF(dec); err != nil {
		return nil, &CompileError{Errors: []ValidationError{{Path: "$", Message: err.Error()}}}
	}
	return Compile(f)
}

func ensureEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing JSON after rule set")
	}
	return nil
}

// Compile validates f, accumulating every error, and builds the executable set.
func Compile(f RuleSetFile) (*RuleSet, error) {
	var errs []ValidationError
	add := func(path, format string, args ...any) {
		errs = append(errs, ValidationError{Path: path, Message: fmt.Sprintf(format, args...)})
	}

	if !setNamePattern.MatchString(f.RuleSet) {
		add("rule_set", "must match %s", setNamePattern)
	}
	if f.Version < 1 {
		add("version", "must be >= 1")
	}
	if strings.TrimSpace(f.Author) == "" {
		add("author", "must not be empty")
	}
	if len(f.Changelog) > 500 {
		add("changelog", "must be at most 500 characters")
	}

	combiner := DefaultCombiner()
	if f.Config.Combiner.Thresholds != nil {
		t := f.Config.Combiner.Thresholds
		combiner.StepUpThreshold = t.StepUp
		combiner.HoldThreshold = t.Hold
		combiner.BlockThreshold = t.Block
		if !(0 <= t.StepUp && t.StepUp < t.Hold && t.Hold < t.Block && t.Block <= 100) {
			add("config.combiner.thresholds", "must satisfy 0 <= step_up < hold < block <= 100")
		}
	}
	combiner.StepUpFloor = combiner.StepUpThreshold
	combiner.HoldFloor = combiner.HoldThreshold
	combiner.BlockFloor = combiner.BlockThreshold
	if fl := f.Config.Combiner.Floors; fl != nil {
		if fl.StepUp != nil {
			combiner.StepUpFloor = *fl.StepUp
		}
		if fl.Hold != nil {
			combiner.HoldFloor = *fl.Hold
		}
		if fl.Block != nil {
			combiner.BlockFloor = *fl.Block
		}
		if !(0 <= combiner.StepUpFloor && combiner.StepUpFloor <= combiner.HoldFloor &&
			combiner.HoldFloor <= combiner.BlockFloor && combiner.BlockFloor <= 100) {
			add("config.combiner.floors", "must be in [0,100] and satisfy STEP_UP <= HOLD <= BLOCK")
		}
	}

	failMode := make(map[string]string, len(f.Config.FailMode))
	for k, v := range f.Config.FailMode {
		if k != "default" && !validTxnType(k) {
			add("config.fail_mode."+k, "unknown transaction type")
		}
		switch v {
		case FailOpen, FailClosed, FailStepUp:
		default:
			add("config.fail_mode."+k, "must be open, closed or step_up")
		}
		failMode[k] = v
	}
	if _, ok := failMode["default"]; !ok {
		failMode["default"] = FailStepUp
	}

	if len(f.Rules) == 0 {
		add("rules", "at least one rule is required")
	}
	if len(f.Rules) > MaxRules {
		add("rules", "at most %d rules are allowed", MaxRules)
	}

	seenID := map[string]bool{}
	seenName := map[string]bool{}
	compiled := make([]*CompiledRule, 0, len(f.Rules))
	for i := range f.Rules {
		r := &f.Rules[i]
		path := fmt.Sprintf("rules[%d]", i)
		if !idPattern.MatchString(r.ID) {
			add(path+".id", "must match %s", idPattern)
		}
		if seenID[r.ID] {
			add(path+".id", "duplicate rule id %q", r.ID)
		}
		seenID[r.ID] = true
		if strings.TrimSpace(r.Name) == "" || len(r.Name) > 80 {
			add(path+".name", "must be 1-80 characters")
		}
		if seenName[r.Name] {
			add(path+".name", "duplicate rule name %q", r.Name)
		}
		seenName[r.Name] = true
		if r.Enabled == nil {
			add(path+".enabled", "is required")
		}
		if r.Mode != Enforce && r.Mode != Shadow {
			add(path+".mode", "must be enforce or shadow")
		}
		if r.Weight == nil {
			add(path+".weight", "is required")
		} else if *r.Weight < 0 || *r.Weight > 100 {
			add(path+".weight", "must be in [0,100]")
		}
		if !r.Action.Valid() {
			add(path+".action", "must be ALLOW, STEP_UP, HOLD or BLOCK")
		}
		if len(r.Tags) > 8 {
			add(path+".tags", "at most 8 tags are allowed")
		}
		for _, tag := range r.Tags {
			if !tagPattern.MatchString(tag) {
				add(path+".tags", "invalid tag %q", tag)
			}
		}
		if len(r.AppliesTo) > 0 {
			seenType := map[string]bool{}
			for _, t := range r.AppliesTo {
				if !validTxnType(t) {
					add(path+".applies_to", "unknown txn_type %q", t)
				}
				if seenType[t] {
					add(path+".applies_to", "duplicate txn_type %q", t)
				}
				seenType[t] = true
			}
		}
		if strings.TrimSpace(r.Reason) == "" || len(r.Reason) > MaxReasonLen {
			add(path+".reason", "must be 1-%d characters", MaxReasonLen)
		}

		nodes := 0
		match, cost, condErrs := compileCondition(path+".when", &r.When, 1, &nodes, r.Stateless)
		for _, ce := range condErrs {
			errs = append(errs, ce)
		}

		tmpl, reasonErrs := compileReason(path+".reason", r.Reason)
		for _, re := range reasonErrs {
			errs = append(errs, re)
		}

		if r.Enabled != nil && *r.Enabled && match != nil {
			weight := 0
			if r.Weight != nil {
				weight = *r.Weight
			}
			compiled = append(compiled, &CompiledRule{
				ID:        r.ID,
				Name:      r.Name,
				Mode:      r.Mode,
				Weight:    weight,
				Action:    r.Action,
				Stateless: r.Stateless,
				TxnTypes:  append([]string(nil), r.AppliesTo...),
				Tags:      append([]string(nil), r.Tags...),
				Reason:    tmpl,
				Cost:      cost,
				match:     match,
			})
		}
	}
	if len(errs) > 0 {
		return nil, &CompileError{Errors: errs}
	}

	rs := buildCompiled(f, combiner, failMode, compiled)
	hash, err := HashRuleSet(f)
	if err != nil {
		return nil, &CompileError{Errors: []ValidationError{{Path: "$", Message: "hash rule set: " + err.Error()}}}
	}
	rs.Hash = hash
	return rs, nil
}

// buildCompiled wires validated pieces into the immutable rule set.
func buildCompiled(f RuleSetFile, combiner Combiner, failMode map[string]string, compiled []*CompiledRule) *RuleSet {
	sort.SliceStable(compiled, func(i, j int) bool { return compiled[i].Cost < compiled[j].Cost })
	rs := &RuleSet{
		RuleSet:    f.RuleSet,
		Version:    f.Version,
		Author:     f.Author,
		Changelog:  f.Changelog,
		Combiner:   combiner,
		FailMode:   failMode,
		CompiledAt: nowRFC3339(),
		file:       f,
		rules:      compiled,
		byTxn:      make(map[string][]*CompiledRule, len(TxnTypes)),
		reason:     make(map[string]*CompiledRule, len(compiled)),
	}
	for _, r := range compiled {
		rs.reason[r.ID] = r
	}
	for _, t := range TxnTypes {
		list := make([]*CompiledRule, 0, len(compiled))
		for _, r := range compiled {
			if appliesTo(r, t) {
				list = append(list, r)
			}
		}
		rs.byTxn[t] = list
	}
	generic := make([]*CompiledRule, 0, len(compiled))
	for _, r := range compiled {
		if len(r.TxnTypes) == 0 {
			generic = append(generic, r)
		}
	}
	rs.byTxn[""] = generic
	return rs
}

func appliesTo(r *CompiledRule, txnType string) bool {
	if len(r.TxnTypes) == 0 {
		return true
	}
	for _, t := range r.TxnTypes {
		if t == txnType {
			return true
		}
	}
	return false
}

// compileCondition validates and compiles one node. It returns the whole list
// of errors under the node so validation never stops at the first failure.
func compileCondition(path string, c *Condition, depth int, nodes *int, stateless bool) (func(*EvalContext) bool, int, []ValidationError) {
	var errs []ValidationError
	add := func(p, format string, args ...any) {
		errs = append(errs, ValidationError{Path: p, Message: fmt.Sprintf(format, args...)})
	}
	if depth > MaxDepth {
		add(path, "nesting deeper than %d", MaxDepth)
		return nil, 0, errs
	}
	*nodes++
	if *nodes > MaxConditions {
		add(path, "more than %d conditions in one rule", MaxConditions)
		return nil, 0, errs
	}

	forms := 0
	if c.F != "" || c.Op != "" || len(c.V) > 0 {
		forms++
	}
	if len(c.All) > 0 {
		forms++
	}
	if len(c.Any) > 0 {
		forms++
	}
	if c.Not != nil {
		forms++
	}
	switch {
	case forms == 0:
		add(path, "condition needs f/op/v, all, any or not")
		return nil, 0, errs
	case forms > 1:
		add(path, "condition must use exactly one form")
		return nil, 0, errs
	}

	if c.Not != nil {
		child, cost, childErrs := compileCondition(path+".not", c.Not, depth+1, nodes, stateless)
		errs = append(errs, childErrs...)
		if child == nil {
			return nil, 0, errs
		}
		return func(ctx *EvalContext) bool { return !child(ctx) }, cost, errs
	}
	if len(c.All) > 0 || len(c.Any) > 0 {
		children := c.All
		kind := "all"
		if len(c.Any) > 0 {
			children, kind = c.Any, "any"
		}
		fns := make([]func(*EvalContext) bool, 0, len(children))
		cost := 0
		for i := range children {
			fn, cst, childErrs := compileCondition(fmt.Sprintf("%s.%s[%d]", path, kind, i), &children[i], depth+1, nodes, stateless)
			errs = append(errs, childErrs...)
			if fn != nil {
				fns = append(fns, fn)
				cost += cst
			}
		}
		if len(fns) != len(children) {
			return nil, 0, errs
		}
		if kind == "all" {
			return func(ctx *EvalContext) bool {
				for _, fn := range fns {
					if !fn(ctx) {
						return false
					}
				}
				return true
			}, cost, errs
		}
		return func(ctx *EvalContext) bool {
			for _, fn := range fns {
				if fn(ctx) {
					return true
				}
			}
			return false
		}, cost, errs
	}

	fn, cost, compErrs := compileComparison(path, c, stateless)
	errs = append(errs, compErrs...)
	return fn, cost, errs
}

var numericOps = map[string]bool{
	"==": true, "!=": true, ">": true, ">=": true, "<": true, "<=": true, "in": true, "not_in": true,
}

func compileComparison(path string, c *Condition, stateless bool) (func(*EvalContext) bool, int, []ValidationError) {
	var errs []ValidationError
	add := func(p, format string, args ...any) {
		errs = append(errs, ValidationError{Path: p, Message: fmt.Sprintf(format, args...)})
	}
	field, ok := Fields[c.F]
	if !ok {
		add(path+".f", "unknown identifier %q", c.F)
	}
	isList := c.Op == "in" || c.Op == "not_in"
	if !numericOps[c.Op] {
		add(path+".op", "unknown operator %q", c.Op)
		return nil, 0, errs
	}
	if !ok {
		return nil, 0, errs
	}
	if stateless && !field.Raw {
		add(path+".f", "stateless rules may only use raw identifiers (got %q)", c.F)
	}
	if field.Kind == KindBool && c.Op != "==" && c.Op != "!=" && !isList {
		add(path+".op", "bool identifiers support ==, !=, in, not_in only")
	}
	if field.Kind == KindString && c.Op != "==" && c.Op != "!=" && !isList {
		add(path+".op", "string identifiers support ==, !=, in, not_in only")
	}
	if len(c.V) == 0 {
		add(path+".v", "operator %q requires v", c.Op)
		return nil, 0, errs
	}

	cost := 2
	switch field.Kind {
	case KindBool:
		cost = 1
	case KindFloat, KindString:
		cost = 3
	}

	switch field.Kind {
	case KindInt:
		get := field.Int
		if isList {
			want, err := intList(c.V)
			if err != nil {
				add(path+".v", "%v", err)
				return nil, 0, errs
			}
			if c.Op == "in" {
				return func(ctx *EvalContext) bool {
					got := get(ctx)
					for _, w := range want {
						if got == w {
							return true
						}
					}
					return false
				}, cost + len(want), errs
			}
			return func(ctx *EvalContext) bool {
				got := get(ctx)
				for _, w := range want {
					if got == w {
						return false
					}
				}
				return true
			}, cost + len(want), errs
		}
		w, err := scalarInt(c.V)
		if err != nil {
			add(path+".v", "%v", err)
			return nil, 0, errs
		}
		return intOp(get, c.Op, w), cost, errs

	case KindFloat:
		get := field.Float
		if isList {
			want, err := floatList(c.V)
			if err != nil {
				add(path+".v", "%v", err)
				return nil, 0, errs
			}
			if c.Op == "in" {
				return func(ctx *EvalContext) bool {
					got := get(ctx)
					for _, w := range want {
						if got == w {
							return true
						}
					}
					return false
				}, cost + len(want), errs
			}
			return func(ctx *EvalContext) bool {
				got := get(ctx)
				for _, w := range want {
					if got == w {
						return false
					}
				}
				return true
			}, cost + len(want), errs
		}
		w, err := scalarFloat(c.V)
		if err != nil {
			add(path+".v", "%v", err)
			return nil, 0, errs
		}
		return floatOp(get, c.Op, w), cost, errs

	case KindBool:
		get := field.Bool
		if isList {
			want, err := boolList(c.V)
			if err != nil {
				add(path+".v", "%v", err)
				return nil, 0, errs
			}
			if c.Op == "in" {
				return func(ctx *EvalContext) bool {
					got := get(ctx)
					for _, w := range want {
						if got == w {
							return true
						}
					}
					return false
				}, cost + len(want), errs
			}
			return func(ctx *EvalContext) bool {
				got := get(ctx)
				for _, w := range want {
					if got == w {
						return false
					}
				}
				return true
			}, cost + len(want), errs
		}
		w, err := scalarBool(c.V)
		if err != nil {
			add(path+".v", "%v", err)
			return nil, 0, errs
		}
		if c.Op == "==" {
			return func(ctx *EvalContext) bool { return get(ctx) == w }, cost, errs
		}
		return func(ctx *EvalContext) bool { return get(ctx) != w }, cost, errs

	case KindString:
		get := field.Str
		if isList {
			want, err := stringList(c.V)
			if err != nil {
				add(path+".v", "%v", err)
				return nil, 0, errs
			}
			if c.Op == "in" {
				return func(ctx *EvalContext) bool {
					got := get(ctx)
					for _, w := range want {
						if got == w {
							return true
						}
					}
					return false
				}, cost + len(want), errs
			}
			return func(ctx *EvalContext) bool {
				got := get(ctx)
				for _, w := range want {
					if got == w {
						return false
					}
				}
				return true
			}, cost + len(want), errs
		}
		w, err := scalarString(c.V)
		if err != nil {
			add(path+".v", "%v", err)
			return nil, 0, errs
		}
		if c.Op == "==" {
			return func(ctx *EvalContext) bool { return get(ctx) == w }, cost, errs
		}
		return func(ctx *EvalContext) bool { return get(ctx) != w }, cost, errs
	}
	return nil, 0, errs
}

func intOp(get func(*EvalContext) int64, op string, w int64) func(*EvalContext) bool {
	switch op {
	case "==":
		return func(c *EvalContext) bool { return get(c) == w }
	case "!=":
		return func(c *EvalContext) bool { return get(c) != w }
	case ">":
		return func(c *EvalContext) bool { return get(c) > w }
	case ">=":
		return func(c *EvalContext) bool { return get(c) >= w }
	case "<":
		return func(c *EvalContext) bool { return get(c) < w }
	default:
		return func(c *EvalContext) bool { return get(c) <= w }
	}
}

func floatOp(get func(*EvalContext) float64, op string, w float64) func(*EvalContext) bool {
	switch op {
	case "==":
		return func(c *EvalContext) bool { return get(c) == w }
	case "!=":
		return func(c *EvalContext) bool { return get(c) != w }
	case ">":
		return func(c *EvalContext) bool { return get(c) > w }
	case ">=":
		return func(c *EvalContext) bool { return get(c) >= w }
	case "<":
		return func(c *EvalContext) bool { return get(c) < w }
	default:
		return func(c *EvalContext) bool { return get(c) <= w }
	}
}

// rawValue decodes c.V into int/float64/bool/string/[]any.
func rawValue(raw json.RawMessage) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("invalid v: %w", err)
	}
	return v, nil
}

func scalarInt(raw json.RawMessage) (int64, error) {
	v, err := rawValue(raw)
	if err != nil {
		return 0, err
	}
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, fmt.Errorf("expected integer, got %s", n)
		}
		return i, nil
	case int:
		return int64(n), nil
	case int64:
		return n, nil
	case float64:
		if n != math.Trunc(n) {
			return 0, fmt.Errorf("expected integer, got %v", n)
		}
		return int64(n), nil
	default:
		return 0, fmt.Errorf("expected integer, got %T", v)
	}
}

func scalarFloat(raw json.RawMessage) (float64, error) {
	v, err := rawValue(raw)
	if err != nil {
		return 0, err
	}
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0, fmt.Errorf("expected number, got %s", n)
		}
		return f, nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case float64:
		return n, nil
	default:
		return 0, fmt.Errorf("expected number, got %T", v)
	}
}

func scalarBool(raw json.RawMessage) (bool, error) {
	v, err := rawValue(raw)
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("expected bool, got %T", v)
	}
	return b, nil
}

func scalarString(raw json.RawMessage) (string, error) {
	v, err := rawValue(raw)
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("expected string, got %T", v)
	}
	return s, nil
}

func listValues(raw json.RawMessage) ([]any, error) {
	v, err := rawValue(raw)
	if err != nil {
		return nil, err
	}
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expected list, got %T", v)
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("list must not be empty")
	}
	if len(list) > MaxListValues {
		return nil, fmt.Errorf("list may contain at most %d values", MaxListValues)
	}
	return list, nil
}

func intList(raw json.RawMessage) ([]int64, error) {
	vals, err := listValues(raw)
	if err != nil {
		return nil, err
	}
	out := make([]int64, 0, len(vals))
	for _, v := range vals {
		b, _ := json.Marshal(v)
		n, err := scalarInt(b)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func floatList(raw json.RawMessage) ([]float64, error) {
	vals, err := listValues(raw)
	if err != nil {
		return nil, err
	}
	out := make([]float64, 0, len(vals))
	for _, v := range vals {
		b, _ := json.Marshal(v)
		n, err := scalarFloat(b)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func boolList(raw json.RawMessage) ([]bool, error) {
	vals, err := listValues(raw)
	if err != nil {
		return nil, err
	}
	out := make([]bool, 0, len(vals))
	for _, v := range vals {
		b, _ := json.Marshal(v)
		n, err := scalarBool(b)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func stringList(raw json.RawMessage) ([]string, error) {
	vals, err := listValues(raw)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		b, _ := json.Marshal(v)
		n, err := scalarString(b)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

// parseNumberLike is used by the reference interpreter for the same coercions.
func parseNumberLike(v any) (float64, bool, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0, false, false
		}
		_, ierr := n.Int64()
		return f, ierr == nil, true
	case int:
		return float64(n), true, true
	case int64:
		return float64(n), true, true
	case float64:
		return n, n == math.Trunc(n), true
	default:
		return 0, false, false
	}
}
