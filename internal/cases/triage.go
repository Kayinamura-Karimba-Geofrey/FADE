package cases

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// DecisionRef is the slice of a stored decision the triage engine needs.
type DecisionRef struct {
	ID        string
	EventID   string
	AccountID string
	Decision  string
	RiskScore int
	Amount    int64
	DecidedAt time.Time
	Reasons   []string
}

// Decision loads one decision from the decision store.
func (s *Store) Decision(ctx context.Context, id string) (*DecisionRef, error) {
	var (
		d       DecisionRef
		reasons []byte
	)
	err := s.pool.QueryRow(ctx, `
		SELECT id, event_id, account_id, decision, risk_score, amount, decided_at, reasons
		FROM decisions WHERE id=$1 ORDER BY decided_at DESC LIMIT 1`, id).
		Scan(&d.ID, &d.EventID, &d.AccountID, &d.Decision, &d.RiskScore, &d.Amount, &d.DecidedAt, &reasons)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	d.Reasons = ParseReasons(reasons)
	return &d, nil
}

// DecisionPage returns decisions after a watermark, oldest first.
func (s *Store) DecisionPage(ctx context.Context, after time.Time, limit int) ([]DecisionRef, error) {
	if limit <= 0 || limit > 1000 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, event_id, account_id, decision, risk_score, amount, decided_at, reasons
		FROM decisions WHERE decided_at > $1 ORDER BY decided_at LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DecisionRef
	for rows.Next() {
		var (
			d       DecisionRef
			reasons []byte
		)
		if err := rows.Scan(&d.ID, &d.EventID, &d.AccountID, &d.Decision, &d.RiskScore,
			&d.Amount, &d.DecidedAt, &reasons); err != nil {
			return nil, err
		}
		d.Reasons = ParseReasons(reasons)
		out = append(out, d)
	}
	return out, rows.Err()
}

// ParseReasons flattens the stored reason documents to rule ids. Unparseable
// documents yield no reasons rather than an error: reasons are advisory input
// to grouping, never a correctness requirement.
func ParseReasons(raw []byte) []string {
	if len(raw) == 0 {
		return nil
	}
	var docs []struct {
		RuleID string `json:"rule_id"`
		ID     string `json:"id"`
		Code   string `json:"code"`
	}
	if err := json.Unmarshal(raw, &docs); err != nil {
		return nil
	}
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		switch {
		case d.RuleID != "":
			out = append(out, d.RuleID)
		case d.Code != "":
			out = append(out, d.Code)
		case d.ID != "":
			out = append(out, d.ID)
		}
	}
	return out
}

// RepeatAccount reports a prior case on the account in the last 30 days.
func (s *Store) RepeatAccount(ctx context.Context, accountID string, now time.Time) (bool, error) {
	var repeat bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM cases WHERE account_id=$1 AND opened_at >= $2)`,
		accountID, now.Add(-30*24*time.Hour)).Scan(&repeat)
	return repeat, err
}

// QASampleSelected decides, deterministically, whether an ALLOW decision is
// sampled for review. Hashing the decision id keeps the sample stable across
// retries and independent of ordering.
func QASampleSelected(decisionID string, rate float64) bool {
	if rate <= 0 || decisionID == "" {
		return false
	}
	if rate >= 1 {
		return true
	}
	sum := sha256.Sum256([]byte(decisionID))
	v := binary.BigEndian.Uint32(sum[:4]) % 1_000_000
	return float64(v) < rate*1_000_000
}

// OpenFromDecision applies the case-creation policy to one decision: HOLD and
// BLOCK always open a case, ALLOW is sampled, STEP_UP only after a failed
// challenge. Decisions for an account that already has an open case inside the
// grouping window are linked to it instead of opening a duplicate.
func (s *Store) OpenFromDecision(ctx context.Context, d *DecisionRef, source Source, now time.Time) (*Case, bool, error) {
	seen, err := s.HasDecision(ctx, d.ID)
	if err != nil {
		return nil, false, err
	}
	if seen {
		return nil, false, nil
	}
	if target, err := s.FindGroupTarget(ctx, d.AccountID, now); err != nil {
		return nil, false, err
	} else if target != nil && source == SourceDecision {
		linked, err := s.Link(ctx, target.ID, d.ID, d.Amount)
		return linked, false, err
	}
	repeat, err := s.RepeatAccount(ctx, d.AccountID, now)
	if err != nil {
		return nil, false, err
	}
	c, err := s.Create(ctx, CreateInput{
		Source: source, DecisionClass: d.Decision, PrimaryDecision: d.ID,
		AccountID: d.AccountID, EventID: d.EventID, RiskScore: d.RiskScore,
		AmountTotal: d.Amount, RepeatAccount: repeat, Reasons: d.Reasons,
		Now: now, Actor: "case-worker", ActorRole: "service",
	})
	if err != nil {
		return nil, false, err
	}
	return c, true, nil
}

// BreachDue escalates every case past its SLA, up to limit cases.
func (s *Store) BreachDue(ctx context.Context, now time.Time, limit int) ([]*Case, error) {
	if s.cfg.BreachPolicy == "none" || s.cfg.BreachPolicy == "notify" {
		return nil, nil
	}
	due, err := s.Overdue(ctx, now, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*Case, 0, len(due))
	for _, c := range due {
		updated, err := s.Breach(ctx, c.ID, now)
		if errors.Is(err, ErrIllegalTransition) {
			continue
		}
		if err != nil {
			return out, err
		}
		out = append(out, updated)
	}
	return out, nil
}
