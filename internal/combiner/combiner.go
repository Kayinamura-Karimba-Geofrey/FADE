// Package combiner implements the Phase 4 score fusion. It is a pure function
// with no I/O and no global state.
package combiner

import (
	"math"
	"reflect"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// Overrides carries the post-rule overrides applied by the pipeline.
type Overrides struct {
	Blocked    bool
	Allowed    bool
	TierBreach bool
}

// Config is the fusion configuration.
type Config struct {
	RuleWeight       float64
	MLWeight         float64
	StepUpThreshold  int
	HoldThreshold    int
	BlockThreshold   int
	StepUpFloor      int
	HoldFloor        int
	BlockFloor       int
	AllowMLOnlyBlock bool
}

// FromRuleSet builds a Config from a compiled rule set.
func FromRuleSet(rs *rules.RuleSet) Config {
	cfg := Config{RuleWeight: 0.55, MLWeight: 0.45,
		StepUpThreshold: 40, HoldThreshold: 65, BlockThreshold: 85,
		StepUpFloor: 40, HoldFloor: 65, BlockFloor: 85}
	if rs == nil {
		return cfg
	}
	c := rs.Combiner
	if c.RuleWeight > 0 {
		cfg.RuleWeight = c.RuleWeight
	}
	if c.MLWeight > 0 {
		cfg.MLWeight = c.MLWeight
	}
	if c.StepUpThreshold > 0 {
		cfg.StepUpThreshold = c.StepUpThreshold
	}
	if c.HoldThreshold > 0 {
		cfg.HoldThreshold = c.HoldThreshold
	}
	if c.BlockThreshold > 0 {
		cfg.BlockThreshold = c.BlockThreshold
	}
	if c.StepUpFloor > 0 {
		cfg.StepUpFloor = c.StepUpFloor
	}
	if c.HoldFloor > 0 {
		cfg.HoldFloor = c.HoldFloor
	}
	if c.BlockFloor > 0 {
		cfg.BlockFloor = c.BlockFloor
	}
	cfg.AllowMLOnlyBlock = c.AllowMLOnlyBlock
	return cfg
}

// CombineTyped is the hot-path entry used by the pipeline.
func CombineTyped(rs *rules.RuleSet, res *rules.Result, ml rules.MLResult) (int, rules.Decision) {
	ruleScore := 0
	ruleAction := rules.Allow
	if res != nil {
		ruleScore = res.RuleScore
		ruleAction = res.RuleAction
	}
	return CombineCore(ruleScore, ruleAction, ml, Overrides{}, FromRuleSet(rs))
}

// Combine is a flexible adapter accepting (rs,res,ml) or (ruleResult,ml,overrides,cfg).
func Combine(args ...any) (int, rules.Decision) {
	ruleScore := 0
	ruleAction := rules.Allow
	ml := rules.MLResult{Status: rules.MLNotConfigured}
	ov := Overrides{}
	cfg := Config{RuleWeight: 0.55, MLWeight: 0.45,
		StepUpThreshold: 40, HoldThreshold: 65, BlockThreshold: 85,
		StepUpFloor: 40, HoldFloor: 65, BlockFloor: 85}

	// old style: (rs, res, ml)
	if len(args) == 3 {
		if rs, ok := args[0].(*rules.RuleSet); ok {
			cfg = FromRuleSet(rs)
		}
		if res, ok := args[1].(*rules.Result); ok && res != nil {
			ruleScore = res.RuleScore
			ruleAction = res.RuleAction
		}
		if m, ok := args[2].(rules.MLResult); ok {
			ml = m
		}
		return CombineCore(ruleScore, ruleAction, ml, ov, cfg)
	}

	for _, a := range args {
		v := reflect.ValueOf(a)
		if v.Kind() == reflect.Pointer {
			v = v.Elem()
		}
		switch {
		case a == nil:
		case func() bool { _, ok := a.(*rules.RuleSet); return ok }():
			rs := a.(*rules.RuleSet)
			cfg = FromRuleSet(rs)
		case func() bool { _, ok := a.(*rules.Result); return ok }():
			res := a.(*rules.Result)
			if res != nil {
				ruleScore = res.RuleScore
				ruleAction = res.RuleAction
			}
		case func() bool { _, ok := a.(rules.MLResult); return ok }():
			ml = a.(rules.MLResult)
		default:
			if v.Kind() == reflect.Struct {
				// overrides?
				if f := v.FieldByName("Blocked"); f.IsValid() {
					ov.Blocked = f.Bool()
				}
				if f := v.FieldByName("Allowed"); f.IsValid() {
					ov.Allowed = f.Bool()
				}
				if f := v.FieldByName("TierBreach"); f.IsValid() {
					ov.TierBreach = f.Bool()
				}
				// config fields
				if f := v.FieldByName("RuleWeight"); f.IsValid() && f.Kind() == reflect.Float64 {
					if f.Float() != 0 {
						cfg.RuleWeight = f.Float()
					}
				}
				if f := v.FieldByName("MLWeight"); f.IsValid() && f.Kind() == reflect.Float64 {
					if f.Float() != 0 {
						cfg.MLWeight = f.Float()
					}
				}
				if f := v.FieldByName("StepUpThreshold"); f.IsValid() && (f.Kind() == reflect.Int || f.Kind() == reflect.Int64) {
					if f.Int() != 0 {
						cfg.StepUpThreshold = int(f.Int())
					}
				}
				if f := v.FieldByName("HoldThreshold"); f.IsValid() {
					if f.Int() != 0 {
						cfg.HoldThreshold = int(f.Int())
					}
				}
				if f := v.FieldByName("BlockThreshold"); f.IsValid() {
					if f.Int() != 0 {
						cfg.BlockThreshold = int(f.Int())
					}
				}
				if f := v.FieldByName("StepUpFloor"); f.IsValid() {
					if f.Int() != 0 {
						cfg.StepUpFloor = int(f.Int())
					}
				}
				if f := v.FieldByName("HoldFloor"); f.IsValid() {
					if f.Int() != 0 {
						cfg.HoldFloor = int(f.Int())
					}
				}
				if f := v.FieldByName("BlockFloor"); f.IsValid() {
					if f.Int() != 0 {
						cfg.BlockFloor = int(f.Int())
					}
				}
				if f := v.FieldByName("AllowMLOnlyBlock"); f.IsValid() && f.Kind() == reflect.Bool {
					cfg.AllowMLOnlyBlock = f.Bool()
				}
				// rules.Combiner uses StepUp/Hold/Block (not Threshold) and RuleWeight/MLWeight/AllowMLOnlyBlock
				if f := v.FieldByName("StepUp"); f.IsValid() && f.Kind() == reflect.Int {
					if cfg.StepUpThreshold == 40 && f.Int() != 0 {
						// ambiguous; prefer StepUpThreshold if set already
					}
				}
				// if it's rules.Combiner, map StepUp/Hold/Block fields
				if f := v.FieldByName("StepUp"); f.IsValid() && f.Kind() == reflect.Int && cfg.StepUpThreshold == 40 {
					if f.Int() != 0 {
						cfg.StepUpThreshold = int(f.Int())
					}
				}
				if f := v.FieldByName("Hold"); f.IsValid() && f.Kind() == reflect.Int && cfg.HoldThreshold == 65 {
					if f.Int() != 0 {
						cfg.HoldThreshold = int(f.Int())
					}
				}
				if f := v.FieldByName("Block"); f.IsValid() && f.Kind() == reflect.Int && cfg.BlockThreshold == 85 {
					if f.Int() != 0 {
						cfg.BlockThreshold = int(f.Int())
					}
				}
			}
		}
	}

	// defaults for floors
	if cfg.StepUpFloor == 0 {
		cfg.StepUpFloor = cfg.StepUpThreshold
	}
	if cfg.HoldFloor == 0 {
		cfg.HoldFloor = cfg.HoldThreshold
	}
	if cfg.BlockFloor == 0 {
		cfg.BlockFloor = cfg.BlockThreshold
	}
	return CombineCore(ruleScore, ruleAction, ml, ov, cfg)
}

// CombineCore implements fusion.
func CombineCore(ruleScore int, ruleAction rules.Decision, ml rules.MLResult, ov Overrides, cfg Config) (int, rules.Decision) {
	// defaults
	if cfg.StepUpThreshold == 0 && cfg.HoldThreshold == 0 && cfg.BlockThreshold == 0 {
		cfg.StepUpThreshold, cfg.HoldThreshold, cfg.BlockThreshold = 40, 65, 85
	}
	if cfg.RuleWeight == 0 && cfg.MLWeight == 0 {
		cfg.RuleWeight, cfg.MLWeight = 0.55, 0.45
	}
	if cfg.StepUpFloor == 0 {
		cfg.StepUpFloor = cfg.StepUpThreshold
	}
	if cfg.HoldFloor == 0 {
		cfg.HoldFloor = cfg.HoldThreshold
	}
	if cfg.BlockFloor == 0 {
		cfg.BlockFloor = cfg.BlockThreshold
	}

	status := ml.Status
	if status == "" {
		status = rules.MLNotConfigured
	}

	score := ruleScore
	mlOK := status == rules.MLOK

	if mlOK && ml.MLScore != nil {
		score = int(math.Round(cfg.RuleWeight*float64(ruleScore) + cfg.MLWeight*float64(*ml.MLScore)))
	} else {
		score = ruleScore
	}

	// hard rule floor
	if f := floorFor(cfg, ruleAction); f > score {
		score = f
	}
	// ML very high confidence => at least HOLD
	if mlOK && ml.FraudProb >= 0.98 {
		if score < cfg.HoldThreshold {
			score = cfg.HoldThreshold
		}
	}
	// ML-only block safety
	if ruleScore == 0 && mlOK && ml.MLScore != nil && *ml.MLScore >= 85 && !cfg.AllowMLOnlyBlock {
		if score > cfg.HoldThreshold {
			score = cfg.HoldThreshold
		}
	}

	decision := decide(score, cfg)
	if ruleAction.Severity() > decision.Severity() {
		decision = ruleAction
	}
	// ML very high
	if mlOK && ml.FraudProb >= 0.98 {
		if decision.Severity() < rules.Hold.Severity() {
			decision = rules.Hold
		}
	}
	// ML-only block cap
	if ruleScore == 0 && mlOK && ml.MLScore != nil && *ml.MLScore >= 85 && !cfg.AllowMLOnlyBlock {
		if decision == rules.Block {
			decision = rules.Hold
		}
	}

	// overrides
	if ov.Blocked {
		decision = rules.Block
		if score < cfg.BlockThreshold {
			score = cfg.BlockThreshold
		}
	}
	if !ov.Blocked && ov.Allowed {
		decision = rules.Allow
	}
	if ov.TierBreach {
		if decision.Severity() < rules.Hold.Severity() {
			decision = rules.Hold
		}
		if score < cfg.HoldThreshold {
			score = cfg.HoldThreshold
		}
	}

	score = clamp(score)
	return score, decision
}

func floorFor(cfg Config, d rules.Decision) int {
	switch d {
	case rules.Block:
		return cfg.BlockFloor
	case rules.Hold:
		return cfg.HoldFloor
	case rules.StepUp:
		return cfg.StepUpFloor
	default:
		return 0
	}
}

func decide(score int, cfg Config) rules.Decision {
	if score >= cfg.BlockThreshold {
		return rules.Block
	}
	if score >= cfg.HoldThreshold {
		return rules.Hold
	}
	if score >= cfg.StepUpThreshold {
		return rules.StepUp
	}
	return rules.Allow
}

func clamp(v int) int {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}
