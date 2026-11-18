package cases

import "fmt"

// Allowed transitions.
var allowedTransitions = map[Status]map[Status]bool{
	StatusOpen:      {StatusInReview: true},
	StatusInReview:  {StatusOpen: true, StatusEscalated: true, StatusResolved: true},
	StatusEscalated: {StatusInReview: true, StatusResolved: true},
	StatusResolved:  {StatusReopened: true},
	StatusReopened:  {StatusInReview: true},
}

func CanTransition(from, to Status) bool {
	if m, ok := allowedTransitions[from]; ok {
		return m[to]
	}
	return false
}

// ValidateTransition returns an error if the transition is not allowed.
func ValidateTransition(from, to Status) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("illegal transition %s -> %s", from, to)
	}
	return nil
}

// OutcomeForStatus ensures resolved cases have a valid outcome.
func ValidOutcome(o Outcome) bool {
	return o == OutcomeFraud || o == OutcomeLegit || o == OutcomeInconclusive
}
