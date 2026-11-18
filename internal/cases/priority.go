package cases

// PriorityWeights holds configurable weights.
type PriorityWeights struct {
	RepeatAccountBonus  int `json:"repeat_account_bonus"`
	BlockBonus          int `json:"block_bonus"`
	LinkedDecisionBonus int `json:"linked_decision_bonus"`
	LinkedDecisionCap   int `json:"linked_decision_cap"`
	AmountDivisor       int `json:"amount_divisor"`
	AmountCap           int `json:"amount_cap"`
}

func DefaultPriorityWeights() PriorityWeights {
	return PriorityWeights{
		RepeatAccountBonus:  10,
		BlockBonus:          15,
		LinkedDecisionBonus: 5,
		LinkedDecisionCap:   20,
		AmountDivisor:       100000,
		AmountCap:           20,
	}
}

// ComputePriority implements the documented formula.
// priority = risk_score + min(20, amount/100000) + (10 if repeat) + (15 if BLOCK) + (5 per linked, cap 20); clamp 0-100.
func ComputePriority(riskScore int, amountTotal int64, repeatAccount bool, decisionClass string, linkedCount int, w PriorityWeights) int {
	p := riskScore
	if w.AmountDivisor > 0 {
		bonus := int(amountTotal / int64(w.AmountDivisor))
		if bonus > w.AmountCap {
			bonus = w.AmountCap
		}
		p += bonus
	}
	if repeatAccount {
		p += w.RepeatAccountBonus
	}
	if decisionClass == "BLOCK" {
		p += w.BlockBonus
	}
	linkedBonus := linkedCount * w.LinkedDecisionBonus
	if linkedBonus > w.LinkedDecisionCap {
		linkedBonus = w.LinkedDecisionCap
	}
	p += linkedBonus
	if p < 0 {
		p = 0
	}
	if p > 100 {
		p = 100
	}
	return p
}
