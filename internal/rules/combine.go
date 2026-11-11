package rules

import (
	"context"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
)

// ML status values in the API contract.
const (
	MLNotConfigured = "NOT_CONFIGURED"
	MLOK            = "OK"
	MLError         = "ERROR"
	MLTimeout       = "TIMEOUT"
)

// MLResult is a model output. A nil Score means "no score", which is the
// Phase 3 NoopScorer contract.
type MLResult struct {
	Score        *int
	Status       string
	ModelVersion string
}

// Scorer is the Phase 4 plug point. Phase 3 always ships NoopScorer, so the
// rules-only combiner is used without any API change later.
type Scorer interface {
	Score(ctx context.Context, fv *features.FeatureVector) (MLResult, error)
}

// NoopScorer reports that no ML model is configured.
type NoopScorer struct{}

// Score implements Scorer.
func (NoopScorer) Score(context.Context, *features.FeatureVector) (MLResult, error) {
	return MLResult{Status: MLNotConfigured}, nil
}

// Combine produces the final risk score and decision. Phase 3 has no ML score:
// risk is the rule score raised to the floor of the rule action. When a Phase 4
// scorer returns a score it is added before the floor is applied.
func Combine(rs *RuleSet, res *Result, ml MLResult) (int, Decision) {
	score := res.RuleScore
	if ml.Score != nil {
		score += *ml.Score
	}
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	return rs.Combiner.Apply(score, res.RuleAction)
}
