package cases

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
)

// Store errors surfaced as Problem responses by the API layer.
var (
	ErrNotFound           = errors.New("case not found")
	ErrVersionConflict    = errors.New("case was modified by someone else")
	ErrIllegalTransition  = errors.New("illegal case transition")
	ErrSelfApproval       = errors.New("self approval is not allowed")
	ErrApprovalRequired   = errors.New("approval required")
	ErrNotPendingApproval = errors.New("case is not awaiting approval")
	ErrNoteTooLong        = errors.New("note is too long")
	ErrOutcomeRequired    = errors.New("an outcome is required")
	ErrFollowUpRequired   = errors.New("follow_up_at is required for inconclusive cases")
	ErrReasonRequired     = errors.New("a reason code is required")
)

// Store is the Postgres-backed case repository.
type Store struct {
	pool *pgxpool.Pool
	cfg  Config
}

// NewStore builds a case store.
func NewStore(pool *pgxpool.Pool, cfg Config) *Store {
	if cfg.Priority.AmountDivisor == 0 {
		cfg = DefaultConfig()
	}
	return &Store{pool: pool, cfg: cfg}
}

// Config returns the policy in force.
func (s *Store) Config() Config { return s.cfg }

// CreateInput opens a case.
type CreateInput struct {
	Source          Source
	DecisionClass   string
	PrimaryDecision string
	AccountID       string
	EventID         string
	RiskScore       int
	AmountTotal     int64
	RepeatAccount   bool
	ScenarioRun     string
	Reasons         []string
	Assignee        string
	SecondReviewOf  *int64
	Now             time.Time
	Actor           string
	ActorRole       string
}

// Resolution is the analyst verdict for a case.
type Resolution struct {
	Outcome         Outcome
	ReasonCode      string
	Confidence      string
	FollowUpAt      *time.Time
	PropagateLinked bool
	Now             time.Time
}

// Filter scopes a queue query.
type Filter struct {
	Statuses      []Status
	Assignee      string
	Unassigned    bool
	Source        Source
	DecisionClass string
	MinPriority   int
	AccountHash   string
	Limit         int
	Offset        int
}

const caseColumns = `id, source, coalesce(decision_class,''), coalesce(primary_decision_id,''),
	linked_decision_ids, account_id, risk_score, amount_total, priority, status, assignee,
	outcome, reason_code, outcome_confidence, pending_approval, approved_by, follow_up_at,
	claimed_at, last_activity_at, repeat_account, escalated, sla_due_at, sla_paused_at,
	version, opened_at, resolved_at, second_review_of, propagate_linked`

type row struct {
	ID              int64
	Source          string
	DecisionClass   string
	PrimaryDecision string
	Linked          []string
	AccountID       string
	RiskScore       int32
	AmountTotal     int64
	Priority        int32
	Status          string
	Assignee        *string
	Outcome         *string
	ReasonCode      *string
	OutcomeConf     *string
	PendingApproval bool
	ApprovedBy      *string
	FollowUpAt      *time.Time
	ClaimedAt       *time.Time
	LastActivityAt  *time.Time
	RepeatAccount   bool
	Escalated       bool
	SLADueAt        *time.Time
	SLAPausedAt     *time.Time
	Version         int32
	OpenedAt        time.Time
	ResolvedAt      *time.Time
	SecondReviewOf  *int64
	PropagateLinked bool
}

func (r row) toCase() *Case {
	c := &Case{
		ID: r.ID, Source: Source(r.Source), DecisionClass: r.DecisionClass,
		PrimaryDecision: r.PrimaryDecision, LinkedDecisions: r.Linked,
		AccountID: r.AccountID, RiskScore: int(r.RiskScore), AmountTotal: r.AmountTotal,
		Priority: int(r.Priority), Status: Status(r.Status), Assignee: r.Assignee,
		ReasonCode: r.ReasonCode, OutcomeConf: r.OutcomeConf,
		PendingApproval: r.PendingApproval, ApprovedBy: r.ApprovedBy,
		FollowUpAt: r.FollowUpAt, ClaimedAt: r.ClaimedAt, LastActivityAt: r.LastActivityAt,
		RepeatAccount: r.RepeatAccount, Escalated: r.Escalated,
		SLADueAt: r.SLADueAt, SLAPausedAt: r.SLAPausedAt, Version: int(r.Version),
		OpenedAt: r.OpenedAt, ResolvedAt: r.ResolvedAt, SecondReviewOf: r.SecondReviewOf,
		PropagateLinked: r.PropagateLinked,
	}
	if r.Outcome != nil {
		o := Outcome(*r.Outcome)
		c.Outcome = &o
	}
	return c
}

func scanCase(s interface{ Scan(...any) error }) (*Case, error) {
	var r row
	err := s.Scan(&r.ID, &r.Source, &r.DecisionClass, &r.PrimaryDecision, &r.Linked,
		&r.AccountID, &r.RiskScore, &r.AmountTotal, &r.Priority, &r.Status, &r.Assignee,
		&r.Outcome, &r.ReasonCode, &r.OutcomeConf, &r.PendingApproval, &r.ApprovedBy,
		&r.FollowUpAt, &r.ClaimedAt, &r.LastActivityAt, &r.RepeatAccount, &r.Escalated,
		&r.SLADueAt, &r.SLAPausedAt, &r.Version, &r.OpenedAt, &r.ResolvedAt, &r.SecondReviewOf,
		&r.PropagateLinked)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return r.toCase(), nil
}

// Create opens a case and records the `opened` event in one transaction.
func (s *Store) Create(ctx context.Context, in CreateInput) (*Case, error) {
	now := in.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if in.Source == "" {
		in.Source = SourceManual
	}
	due := SLADueAt(in.Source, in.DecisionClass, s.cfg.SLA, now)

	var out *Case
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var id int64
		err := tx.QueryRow(ctx, `
			INSERT INTO cases (decision_id, account_id, status, priority, assignee, opened_at,
				source, decision_class, primary_decision_id, risk_score, amount_total, sla_due_at,
				last_activity_at, repeat_account, second_review_of, version)
			VALUES ($1,$2,'OPEN',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,1)
			RETURNING id`,
			in.PrimaryDecision, in.AccountID,
			s.cfg.PriorityFor(in.RiskScore, in.AmountTotal, in.RepeatAccount, in.DecisionClass, 0),
			nullString(in.Assignee), now, string(in.Source), nullString(in.DecisionClass),
			nullString(in.PrimaryDecision), in.RiskScore, in.AmountTotal, due, now,
			in.RepeatAccount, in.SecondReviewOf).Scan(&id)
		if err != nil {
			return err
		}
		if err := s.appendEvent(ctx, tx, id, in.Actor, in.ActorRole, "opened", map[string]any{
			"source": string(in.Source), "decision_class": in.DecisionClass,
			"risk_score": in.RiskScore, "amount_total": in.AmountTotal,
		}, now); err != nil {
			return err
		}
		if err := s.enqueue(ctx, tx, "case.actions", fmt.Sprint(id), map[string]any{
			"case_id": id, "action": "opened", "status": "OPEN", "at": now,
		}); err != nil {
			return err
		}
		c, err := s.getTx(ctx, tx, id)
		if err != nil {
			return err
		}
		out = c
		return nil
	})
	return out, err
}

func (s *Store) getTx(ctx context.Context, tx pgx.Tx, id int64) (*Case, error) {
	return scanCase(tx.QueryRow(ctx, `SELECT `+caseColumns+` FROM cases WHERE id=$1`, id))
}

// Get returns one case.
func (s *Store) Get(ctx context.Context, id int64) (*Case, error) {
	return scanCase(s.pool.QueryRow(ctx, `SELECT `+caseColumns+` FROM cases WHERE id=$1`, id))
}

// List returns a page of cases in queue order (priority desc, oldest first).
func (s *Store) List(ctx context.Context, f Filter) ([]*Case, int, error) {
	limit := f.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var where []string
	var args []any
	add := func(clause string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(clause, len(args)))
	}
	if len(f.Statuses) > 0 {
		vals := make([]string, len(f.Statuses))
		for i, st := range f.Statuses {
			vals[i] = string(st)
		}
		add("status = ANY($%d)", vals)
	}
	if f.Assignee != "" {
		add("assignee = $%d", f.Assignee)
	}
	if f.Unassigned {
		where = append(where, "assignee IS NULL")
	}
	if f.Source != "" {
		add("source = $%d", string(f.Source))
	}
	if f.DecisionClass != "" {
		add("decision_class = $%d", f.DecisionClass)
	}
	if f.MinPriority > 0 {
		add("priority >= $%d", f.MinPriority)
	}
	if f.AccountHash != "" {
		add("account_id_hash = $%d", f.AccountHash)
	}
	clause := "TRUE"
	if len(where) > 0 {
		clause = strings.Join(where, " AND ")
	}
	var total int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM cases WHERE `+clause, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	args = append(args, limit, f.Offset)
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT %s FROM cases WHERE %s ORDER BY priority DESC, opened_at ASC LIMIT $%d OFFSET $%d`,
		caseColumns, clause, len(args)-1, len(args)), args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := make([]*Case, 0, limit)
	for rows.Next() {
		c, err := scanCase(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, c)
	}
	return out, total, rows.Err()
}

// Timeline returns the append-only case history, oldest first.
func (s *Store) Timeline(ctx context.Context, id int64) ([]CaseEvent, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, case_id, at, actor, actor_role, type, payload FROM case_events WHERE case_id=$1 ORDER BY at, id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CaseEvent
	for rows.Next() {
		var ev CaseEvent
		var payload []byte
		if err := rows.Scan(&ev.ID, &ev.CaseID, &ev.At, &ev.Actor, &ev.ActorRole, &ev.Type, &payload); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(payload, &ev.Payload)
		out = append(out, ev)
	}
	return out, rows.Err()
}

// mutate runs an optimistic-locked state change: it locks the row, checks the
// caller's version and the transition, then updates and records the event.
func (s *Store) mutate(ctx context.Context, id int64, version int, to Status, eventType,
	actor, role, setClause string, setArgs []any, payload map[string]any, now time.Time) (*Case, error) {
	var out *Case
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		cur, err := s.lockTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if version > 0 && cur.Version != version {
			return ErrVersionConflict
		}
		if to != "" {
			if err := ValidateTransition(cur.Status, to); err != nil {
				return fmt.Errorf("%w: %v", ErrIllegalTransition, err)
			}
		}
		args := append([]any{now, id}, setArgs...)
		query := fmt.Sprintf("UPDATE cases SET %s, last_activity_at=$1, version=version+1 WHERE id=$2", setClause)
		if _, err := tx.Exec(ctx, query, args...); err != nil {
			return err
		}
		if err := s.appendEvent(ctx, tx, id, actor, role, eventType, payload, now); err != nil {
			return err
		}
		if err := s.enqueue(ctx, tx, "case.actions", fmt.Sprint(id), map[string]any{
			"case_id": id, "action": eventType, "status": string(to), "at": now,
		}); err != nil {
			return err
		}
		c, err := s.getTx(ctx, tx, id)
		if err != nil {
			return err
		}
		out = c
		return nil
	})
	return out, err
}

func (s *Store) lockTx(ctx context.Context, tx pgx.Tx, id int64) (*Case, error) {
	return scanCase(tx.QueryRow(ctx, `SELECT `+caseColumns+` FROM cases WHERE id=$1 FOR UPDATE`, id))
}

// ClaimNext atomically claims the highest-priority available case for the
// caller. It uses SKIP LOCKED so concurrent analysts never block each other
// and never receive the same case.
func (s *Store) ClaimNext(ctx context.Context, actor, role string, f Filter) (*Case, error) {
	now := time.Now().UTC()
	var out *Case
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var where []string
		var args []any
		add := func(clause string, v any) {
			args = append(args, v)
			where = append(where, fmt.Sprintf(clause, len(args)))
		}
		statuses := f.Statuses
		if len(statuses) == 0 {
			statuses = []Status{StatusOpen, StatusEscalated, StatusReopened}
		}
		vals := make([]string, len(statuses))
		for i, st := range statuses {
			vals[i] = string(st)
		}
		add("status = ANY($%d)", vals)
		if f.Source != "" {
			add("source = $%d", string(f.Source))
		}
		if f.DecisionClass != "" {
			add("decision_class = $%d", f.DecisionClass)
		}
		if f.MinPriority > 0 {
			add("priority >= $%d", f.MinPriority)
		}
		if f.Unassigned {
			where = append(where, "assignee IS NULL")
		}
		clause := strings.Join(where, " AND ")
		if clause == "" {
			clause = "TRUE"
		}
		var id int64
		err := tx.QueryRow(ctx, fmt.Sprintf(
			`SELECT id FROM cases WHERE %s ORDER BY priority DESC, opened_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED`,
			clause), args...).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE cases SET status='IN_REVIEW', assignee=$2, claimed_at=$1,
				last_activity_at=$1, version=version+1 WHERE id=$3`,
			now, actor, id); err != nil {
			return err
		}
		if err := s.appendEvent(ctx, tx, id, actor, role, "claimed", map[string]any{"assignee": actor}, now); err != nil {
			return err
		}
		if err := s.enqueue(ctx, tx, "case.actions", fmt.Sprint(id), map[string]any{
			"case_id": id, "action": "claimed", "status": "IN_REVIEW", "at": now,
		}); err != nil {
			return err
		}
		c, err := s.getTx(ctx, tx, id)
		if err != nil {
			return err
		}
		out = c
		return nil
	})
	return out, err
}

// Claim moves an OPEN case to IN_REVIEW and assigns it to the caller.
func (s *Store) Claim(ctx context.Context, id int64, actor, role string, version int) (*Case, error) {
	now := time.Now().UTC()
	return s.mutate(ctx, id, version, StatusInReview, "claimed", actor, role,
		"status=$3, assignee=$4, claimed_at=$1", []any{string(StatusInReview), actor},
		map[string]any{"assignee": actor}, now)
}

// Assign hands the case to another analyst, leaving OPEN for IN_REVIEW.
func (s *Store) Assign(ctx context.Context, id int64, assignee, actor, role string, version int) (*Case, error) {
	now := time.Now().UTC()
	cur, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	to := StatusInReview
	if cur.Status == StatusInReview || cur.Status == StatusEscalated {
		to = ""
	}
	return s.mutate(ctx, id, version, to, "assigned", actor, role,
		"assignee=$3, status=COALESCE(NULLIF($4,''), status), claimed_at=COALESCE(claimed_at,$1)",
		[]any{assignee, string(to)}, map[string]any{"assignee": assignee}, now)
}

// Release returns a claimed case to the shared queue.
func (s *Store) Release(ctx context.Context, id int64, actor, role string, version int) (*Case, error) {
	now := time.Now().UTC()
	return s.mutate(ctx, id, version, StatusOpen, "released", actor, role,
		"status=$3, assignee=NULL", []any{string(StatusOpen)},
		map[string]any{"previous_assignee": actor}, now)
}

// Escalate raises the case for a lead.
func (s *Store) Escalate(ctx context.Context, id int64, actor, role, reason string, version int) (*Case, error) {
	if strings.TrimSpace(reason) == "" {
		return nil, ErrReasonRequired
	}
	now := time.Now().UTC()
	return s.mutate(ctx, id, version, StatusEscalated, "escalated", actor, role,
		"status=$3, escalated=TRUE, reason_code=$4", []any{string(StatusEscalated), NormalizeReason(reason)},
		map[string]any{"reason": reason}, now)
}

// Resolve records a verdict. Sensitive resolutions are parked in
// PENDING_APPROVAL until a different lead/admin approves them.
func (s *Store) Resolve(ctx context.Context, id int64, in Resolution, actor, role string, version int) (*Case, error) {
	if !ValidOutcome(in.Outcome) {
		return nil, ErrOutcomeRequired
	}
	if in.Outcome == OutcomeInconclusive && in.FollowUpAt == nil {
		return nil, ErrFollowUpRequired
	}
	if strings.TrimSpace(in.ReasonCode) == "" {
		return nil, ErrReasonRequired
	}
	now := in.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cur, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := ValidateTransition(cur.Status, StatusResolved); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrIllegalTransition, err)
	}
	needsApproval := s.cfg.NeedsFourEyes(cur.DecisionClass, in.Outcome, cur.AmountTotal)

	var out *Case
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		locked, err := s.lockTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if version > 0 && locked.Version != version {
			return ErrVersionConflict
		}
		resolvedAt := any(now)
		if needsApproval {
			resolvedAt = nil
		}
		// A verdict waiting for a second reviewer pauses the SLA clock: the
		// analyst's part is done and the wait is on a lead.
		var pausedAt any
		if needsApproval {
			pausedAt = now
		}
		if _, err := tx.Exec(ctx, `
			UPDATE cases SET status=$3, outcome=$4, reason_code=$5, outcome_confidence=$6,
				follow_up_at=$7, resolved_at=$8, pending_approval=$9, last_activity_at=$1,
				propagate_linked=$10, sla_paused_at=$11, version=version+1
			WHERE id=$2`,
			now, id, string(StatusResolved), string(in.Outcome), NormalizeReason(in.ReasonCode),
			nullString(in.Confidence), in.FollowUpAt, resolvedAt, needsApproval, in.PropagateLinked,
			pausedAt); err != nil {
			return err
		}
		locked.PropagateLinked = in.PropagateLinked
		payload := map[string]any{
			"outcome": string(in.Outcome), "reason_code": NormalizeReason(in.ReasonCode),
			"confidence": in.Confidence, "pending_approval": needsApproval,
			"propagate_linked": in.PropagateLinked,
		}
		if err := s.appendEvent(ctx, tx, id, actor, role, "resolved", payload, now); err != nil {
			return err
		}
		if needsApproval {
			// The label is written only after approval, so an unapproved verdict
			// never reaches the feedback loop.
			if err := s.enqueue(ctx, tx, "case.actions", fmt.Sprint(id), map[string]any{
				"case_id": id, "action": "resolution_pending_approval", "status": string(StatusResolved), "at": now,
			}); err != nil {
				return err
			}
		} else if err := s.writeLabel(ctx, tx, locked, in, actor, now); err != nil {
			return err
		}
		c, err := s.getTx(ctx, tx, id)
		if err != nil {
			return err
		}
		out = c
		return nil
	})
	return out, err
}

// Approve releases a pending four-eyes resolution. The approver must differ
// from the analyst who recorded the verdict.
func (s *Store) Approve(ctx context.Context, id int64, actor, role string, version int) (*Case, error) {
	now := time.Now().UTC()
	var out *Case
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		cur, err := s.lockTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if !cur.PendingApproval {
			return ErrNotPendingApproval
		}
		if version > 0 && cur.Version != version {
			return ErrVersionConflict
		}
		var resolver string
		if err := tx.QueryRow(ctx,
			`SELECT actor FROM case_events WHERE case_id=$1 AND type='resolved' ORDER BY at DESC, id DESC LIMIT 1`,
			id).Scan(&resolver); err != nil {
			return err
		}
		if resolver == actor {
			return ErrSelfApproval
		}
		if _, err := tx.Exec(ctx, `
			UPDATE cases SET pending_approval=FALSE, approved_by=$3, resolved_at=$1,
				last_activity_at=$1, sla_paused_at=NULL, version=version+1 WHERE id=$2`,
			now, id, actor); err != nil {
			return err
		}
		if err := s.appendEvent(ctx, tx, id, actor, role, "approved", map[string]any{"approved_by": actor}, now); err != nil {
			return err
		}
		if cur.Outcome != nil {
			in := Resolution{Outcome: *cur.Outcome}
			if cur.ReasonCode != nil {
				in.ReasonCode = *cur.ReasonCode
			}
			if cur.OutcomeConf != nil {
				in.Confidence = *cur.OutcomeConf
			}
			if err := s.writeLabel(ctx, tx, cur, in, actor, now); err != nil {
				return err
			}
		}
		c, err := s.getTx(ctx, tx, id)
		if err != nil {
			return err
		}
		out = c
		return nil
	})
	return out, err
}

// writeLabel appends the analyst label and supersedes any earlier label for
// the same event. INCONCLUSIVE writes no label by design.
func (s *Store) writeLabel(ctx context.Context, tx pgx.Tx, cur *Case, in Resolution, actor string, now time.Time) error {
	eventID := cur.PrimaryDecision
	if eventID == "" {
		eventID = fmt.Sprintf("case:%d", cur.ID)
	}
	label := string(in.Outcome)
	if in.Outcome != OutcomeInconclusive {
		mature := now.Add(s.cfg.LabelMature)
		var historyID int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO label_history (event_id, label, source, case_id, at, mature_at)
			VALUES ($1,$2,'ANALYST',$3,$4,$5) RETURNING id`,
			eventID, label, cur.ID, now, mature).Scan(&historyID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE label_history SET superseded_by=$1
			WHERE event_id=$2 AND id<>$1 AND superseded_by IS NULL`, historyID, eventID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO labels (event_id, label, source, scenario, created_at)
			VALUES ($1,$2,'ANALYST',$3,$4)
			ON CONFLICT (event_id) DO UPDATE SET label=EXCLUDED.label, source=EXCLUDED.source, created_at=EXCLUDED.created_at`,
			eventID, label, cur.DecisionClass, now); err != nil {
			return err
		}
		if err := s.enqueue(ctx, tx, "labels.feedback", eventID, map[string]any{
			"event_id": eventID, "label": label, "source": "ANALYST",
			"case_id": cur.ID, "scenario": cur.DecisionClass, "amount": cur.AmountTotal, "at": now,
		}); err != nil {
			return err
		}
	}
	if in.Outcome == OutcomeInconclusive || !cur.PropagateLinked {
		return nil
	}
	// Propagate to the linked decisions of the same account. A propagated label
	// never overwrites a human one: the analyst's verdict on that decision wins.
	for _, linkedID := range cur.LinkedDecisions {
		if err := s.writePropagatedLabel(ctx, tx, cur, linkedID, label, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) writePropagatedLabel(ctx context.Context, tx pgx.Tx, cur *Case, eventID, label string, now time.Time) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM labels WHERE event_id=$1)`, eventID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	mature := now.Add(s.cfg.LabelMature)
	if _, err := tx.Exec(ctx, `
		INSERT INTO label_history (event_id, label, source, case_id, at, mature_at)
		VALUES ($1,$2,'PROPAGATED',$3,$4,$5)`, eventID, label, cur.ID, now, mature); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO labels (event_id, label, source, scenario, created_at)
		VALUES ($1,$2,'PROPAGATED',$3,$4)
		ON CONFLICT (event_id) DO NOTHING`, eventID, label, cur.DecisionClass, now); err != nil {
		return err
	}
	return s.enqueue(ctx, tx, "labels.feedback", eventID, map[string]any{
		"event_id": eventID, "label": label, "source": "PROPAGATED",
		"case_id": cur.ID, "scenario": cur.DecisionClass,
		"amount": cur.AmountTotal, "at": now,
	})
}

// RejectApproval declines a pending four-eyes resolution and returns the case
// to review without writing a label.
func (s *Store) RejectApproval(ctx context.Context, id int64, actor, role, reason string, version int) (*Case, error) {
	if strings.TrimSpace(reason) == "" {
		return nil, ErrReasonRequired
	}
	now := time.Now().UTC()
	var out *Case
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		cur, err := s.lockTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if !cur.PendingApproval {
			return ErrNotPendingApproval
		}
		if version > 0 && cur.Version != version {
			return ErrVersionConflict
		}
		if _, err := tx.Exec(ctx, `
			UPDATE cases SET status='IN_REVIEW', pending_approval=FALSE, sla_paused_at=NULL,
				last_activity_at=$1, version=version+1 WHERE id=$2`, now, id); err != nil {
			return err
		}
		if err := s.appendEvent(ctx, tx, id, actor, role, "rejected_approval", map[string]any{"reason": reason}, now); err != nil {
			return err
		}
		c, err := s.getTx(ctx, tx, id)
		if err != nil {
			return err
		}
		out = c
		return nil
	})
	return out, err
}

// Reopen re-queues a resolved case within the reopen window.
func (s *Store) Reopen(ctx context.Context, id int64, actor, role, reason string, version int) (*Case, error) {
	if strings.TrimSpace(reason) == "" {
		return nil, ErrReasonRequired
	}
	now := time.Now().UTC()
	cur, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if cur.Status != StatusResolved {
		return nil, fmt.Errorf("%w: only resolved cases can be reopened", ErrIllegalTransition)
	}
	if cur.ResolvedAt != nil && now.Sub(*cur.ResolvedAt) > 7*24*time.Hour {
		return nil, fmt.Errorf("%w: the reopen window has closed", ErrIllegalTransition)
	}
	// A reopened case invalidates its label: the verdict is appended to, never
	// rewritten, so the history stays auditable.
	if _, err := s.pool.Exec(ctx, `
		UPDATE label_history SET superseded_at=$2
		WHERE case_id=$1 AND superseded_by IS NULL AND superseded_at IS NULL`, id, now); err != nil {
		return nil, err
	}
	// A reopened case gets a fresh SLA clock: the original one elapsed long ago
	// and would otherwise re-escalate on the next sweep.
	due := SLADueAt(cur.Source, cur.DecisionClass, s.cfg.SLA, now)
	return s.mutate(ctx, id, version, StatusReopened, "reopened", actor, role,
		"status=$3, outcome=NULL, resolved_at=NULL, pending_approval=FALSE, reason_code=$4, sla_due_at=$5, sla_paused_at=NULL",
		[]any{string(StatusReopened), NormalizeReason(reason), due},
		map[string]any{"reason": reason}, now)
}

// AddNote stores a redaction-checked analyst note on the timeline.
func (s *Store) AddNote(ctx context.Context, id int64, actor, role, text string) (*CaseEvent, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, fmt.Errorf("note must not be empty")
	}
	if len([]rune(text)) > s.cfg.NoteMaxChars {
		return nil, ErrNoteTooLong
	}
	if err := ScanNote(text); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	var ev CaseEvent
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := s.lockTx(ctx, tx, id); err != nil {
			return err
		}
		payload := map[string]any{"text": text}
		if err := s.appendEvent(ctx, tx, id, actor, role, "note", payload, now); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE cases SET last_activity_at=$1, version=version+1 WHERE id=$2`, now, id); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `
			SELECT id, case_id, at, actor, actor_role, type, payload
			FROM case_events WHERE case_id=$1 ORDER BY id DESC LIMIT 1`, id).
			Scan(&ev.ID, &ev.CaseID, &ev.At, &ev.Actor, &ev.ActorRole, &ev.Type, &ev.Payload)
	})
	if err != nil {
		return nil, err
	}
	return &ev, nil
}

// Link attaches another decision to the case and recomputes priority.
func (s *Store) Link(ctx context.Context, id int64, decisionID string, amount int64) (*Case, error) {
	now := time.Now().UTC()
	var out *Case
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		cur, err := s.lockTx(ctx, tx, id)
		if err != nil {
			return err
		}
		linked := append([]string{}, cur.LinkedDecisions...)
		for _, existing := range linked {
			if existing == decisionID {
				out = cur
				return nil
			}
		}
		linked = append(linked, decisionID)
		amountTotal := cur.AmountTotal + amount
		priority := s.cfg.PriorityFor(cur.RiskScore, amountTotal, cur.RepeatAccount, cur.DecisionClass, len(linked))
		if _, err := tx.Exec(ctx, `
			UPDATE cases SET linked_decision_ids=$3, amount_total=$4, priority=$5,
				last_activity_at=$1, version=version+1 WHERE id=$2`,
			now, id, linked, amountTotal, priority); err != nil {
			return err
		}
		if err := s.appendEvent(ctx, tx, id, "case-worker", "service", "linked", map[string]any{
			"decision_id": decisionID, "amount": amount,
		}, now); err != nil {
			return err
		}
		c, err := s.getTx(ctx, tx, id)
		if err != nil {
			return err
		}
		out = c
		return nil
	})
	return out, err
}

// FindGroupTarget returns an open case on the account created inside the
// grouping window, if one exists.
func (s *Store) FindGroupTarget(ctx context.Context, accountID string, now time.Time) (*Case, error) {
	since := now.Add(-time.Duration(s.cfg.Grouping.WindowMinutes) * time.Minute)
	c, err := scanCase(s.pool.QueryRow(ctx,
		`SELECT `+caseColumns+` FROM cases
		 WHERE account_id=$1 AND status IN ('OPEN','IN_REVIEW','ESCALATED') AND opened_at >= $2
		 ORDER BY opened_at DESC LIMIT 1`, accountID, since))
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	return c, err
}

// SLA breaches: cases past due that are still open.
func (s *Store) Overdue(ctx context.Context, now time.Time, limit int) ([]*Case, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+caseColumns+` FROM cases
		 WHERE status IN ('OPEN','IN_REVIEW','ESCALATED') AND escalated = FALSE
		   AND sla_due_at IS NOT NULL AND sla_due_at < $1
		 ORDER BY sla_due_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Case
	for rows.Next() {
		c, err := scanCase(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// AccountDecision is one row of an account timeline.
type AccountDecision struct {
	ID         string         `json:"id"`
	EventID    string         `json:"event_id"`
	Decision   string         `json:"decision"`
	RiskScore  int            `json:"risk_score"`
	Amount     int64          `json:"amount"`
	TxnType    string         `json:"txn_type"`
	DecidedAt  time.Time      `json:"decided_at"`
	NewDevice  bool           `json:"new_device"`
	SIMChanged bool           `json:"sim_changed_72h"`
	RecipNew   bool           `json:"recip_new_for_sender"`
	Features   map[string]any `json:"features,omitempty"`
}

// AccountSummary aggregates 30 days of activity for an account.
type AccountSummary struct {
	AccountID        string         `json:"account_id"`
	Decisions30d     int            `json:"decisions_30d"`
	ByDecision       map[string]int `json:"by_decision"`
	AmountTotal30d   int64          `json:"amount_total_30d"`
	DistinctRecip    int            `json:"distinct_recipients_30d"`
	DistinctDevices  int            `json:"distinct_devices_30d"`
	PriorCases       int            `json:"prior_cases"`
	PriorOutcomes    map[string]int `json:"prior_outcomes"`
	RepeatFraud      bool           `json:"repeat_fraud"`
}

// AccountTimeline returns decisions for an account within [from,to], keyset
// paginated by decided_at desc, id desc. The cursor is opaque to callers.
func (s *Store) AccountTimeline(ctx context.Context, accountID string, from, to time.Time, cursor string, limit int) ([]AccountDecision, string, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if from.IsZero() {
		from = time.Now().UTC().Add(-30 * 24 * time.Hour)
	}
	if to.IsZero() {
		to = time.Now().UTC()
	}
	var cursorTime time.Time
	var cursorID string
	if cursor != "" {
		parts := strings.SplitN(cursor, "|", 2)
		if len(parts) == 2 {
			if t, err := time.Parse(time.RFC3339Nano, parts[0]); err == nil {
				cursorTime = t
				cursorID = parts[1]
			}
		}
	}
	args := []any{accountID, from, to}
	clause := "account_id=$1 AND decided_at >= $2 AND decided_at <= $3"
	if !cursorTime.IsZero() {
		args = append(args, cursorTime, cursorID)
		clause += fmt.Sprintf(" AND (decided_at < $%d OR (decided_at = $%d AND id < $%d))", len(args)-1, len(args)-1, len(args))
	}
	args = append(args, limit+1)
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT id, event_id, decision, risk_score, amount, txn_type, decided_at, features
		FROM decisions WHERE %s ORDER BY decided_at DESC, id DESC LIMIT $%d`, clause, len(args)), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out := make([]AccountDecision, 0, limit)
	for rows.Next() {
		var d AccountDecision
		var featuresRaw []byte
		if err := rows.Scan(&d.ID, &d.EventID, &d.Decision, &d.RiskScore, &d.Amount, &d.TxnType, &d.DecidedAt, &featuresRaw); err != nil {
			return nil, "", err
		}
		var fv map[string]any
		if json.Unmarshal(featuresRaw, &fv) == nil {
			d.NewDevice, _ = fv["new_device"].(bool)
			d.SIMChanged, _ = fv["sim_changed_72h"].(bool)
			d.RecipNew, _ = fv["recip_new_for_sender"].(bool)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1]
		next = last.DecidedAt.UTC().Format(time.RFC3339Nano) + "|" + last.ID
	}
	return out, next, nil
}

// AccountSummary aggregates 30 days of account activity.
func (s *Store) AccountSummary(ctx context.Context, accountID string) (AccountSummary, error) {
	sum := AccountSummary{AccountID: accountID, ByDecision: map[string]int{}, PriorOutcomes: map[string]int{}}
	from := time.Now().UTC().Add(-30 * 24 * time.Hour)
	rows, err := s.pool.Query(ctx, `
		SELECT decision, count(*), coalesce(sum(amount),0) FROM decisions
		WHERE account_id=$1 AND decided_at >= $2 GROUP BY decision`, accountID, from)
	if err != nil {
		return sum, err
	}
	defer rows.Close()
	recips := map[string]bool{}
	devices := map[string]bool{}
	for rows.Next() {
		var decision string
		var n int
		var amt int64
		if err := rows.Scan(&decision, &n, &amt); err != nil {
			return sum, err
		}
		sum.Decisions30d += n
		sum.ByDecision[decision] = n
		sum.AmountTotal30d += amt
	}
	rows.Close()
	// distinct recipients and devices from features JSON.
	rows2, err := s.pool.Query(ctx, `SELECT features FROM decisions WHERE account_id=$1 AND decided_at >= $2`, accountID, from)
	if err == nil {
		defer rows2.Close()
		for rows2.Next() {
			var raw []byte
			if err := rows2.Scan(&raw); err != nil {
				continue
			}
			var fv map[string]any
			if json.Unmarshal(raw, &fv) != nil {
				continue
			}
			if r, ok := fv["recipient_id"].(string); ok {
				recips[r] = true
			}
			if d, ok := fv["device_id"].(string); ok {
				devices[d] = true
			}
		}
	}
	sum.DistinctRecip = len(recips)
	sum.DistinctDevices = len(devices)
	// prior cases
	var casesCount int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM cases WHERE account_id=$1`, accountID).Scan(&casesCount)
	sum.PriorCases = casesCount
	rows3, err := s.pool.Query(ctx, `SELECT outcome, count(*) FROM cases WHERE account_id=$1 AND outcome IS NOT NULL GROUP BY outcome`, accountID)
	if err == nil {
		defer rows3.Close()
		for rows3.Next() {
			var outcome string
			var n int
			if err := rows3.Scan(&outcome, &n); err == nil {
				sum.PriorOutcomes[outcome] = n
				if outcome == "FRAUD" {
					sum.RepeatFraud = true
				}
			}
		}
	}
	return sum, nil
}

// GraphNode is one node in an account relationship graph.
type GraphNode struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// GraphEdge connects two graph nodes.
type GraphEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Type   string `json:"type"`
	Count  int    `json:"count"`
	Total  int64  `json:"total"`
}

// AccountGraph is the related-entities view for an account.
type AccountGraph struct {
	Nodes      []GraphNode `json:"nodes"`
	Edges      []GraphEdge `json:"edges"`
	Truncated  bool        `json:"truncated"`
}

// AccountGraph builds a depth-1 recipient/cycle graph from stored decisions.
func (s *Store) AccountGraph(ctx context.Context, accountID string, p *Pseudonymizer) (AccountGraph, error) {
	g := AccountGraph{Nodes: []GraphNode{}, Edges: []GraphEdge{}}
	nodeSet := map[string]GraphNode{}
	addNode := func(id, typ string) bool {
		if _, ok := nodeSet[id]; ok {
			return true
		}
		if len(nodeSet) >= 60 {
			g.Truncated = true
			return false
		}
		nodeSet[id] = GraphNode{ID: id, Type: typ}
		return true
	}
	root := p.ID("acct", accountID)
	addNode(root, "account")
	// transfers from this account
	type edgeKey struct{ src, dst, typ string }
	edgeMap := map[edgeKey]*GraphEdge{}
	rows, err := s.pool.Query(ctx, `
		SELECT recipient_id, count(*), coalesce(sum(amount),0)
		FROM decisions WHERE account_id=$1 AND decided_at >= $2
		GROUP BY recipient_id`, accountID, time.Now().UTC().Add(-30*24*time.Hour))
	if err != nil {
		return g, err
	}
	defer rows.Close()
	for rows.Next() {
		var recip *string
		var count int
		var total int64
		if err := rows.Scan(&recip, &count, &total); err != nil {
			continue
		}
		if recip == nil || *recip == "" {
			continue
		}
		rn := p.ID("acct", *recip)
		if !addNode(rn, "recipient") {
			continue
		}
		k := edgeKey{root, rn, "transfer"}
		e := &GraphEdge{Source: root, Target: rn, Type: "transfer", Count: count, Total: total}
		edgeMap[k] = e
	}
	// cycles: recipient sent back to this account
	rows2, err := s.pool.Query(ctx, `
		SELECT account_id, count(*), coalesce(sum(amount),0)
		FROM decisions WHERE recipient_id=$1 AND account_id<>$1 AND decided_at >= $2
		GROUP BY account_id`, accountID, time.Now().UTC().Add(-30*24*time.Hour))
	if err == nil {
		defer rows2.Close()
		for rows2.Next() {
			var acct string
			var count int
			var total int64
			if err := rows2.Scan(&acct, &count, &total); err != nil {
				continue
			}
			an := p.ID("acct", acct)
			if !addNode(an, "account") {
				continue
			}
			k := edgeKey{an, root, "cycle"}
			edgeMap[k] = &GraphEdge{Source: an, Target: root, Type: "cycle", Count: count, Total: total}
		}
	}
	for _, n := range nodeSet {
		g.Nodes = append(g.Nodes, n)
	}
	for _, e := range edgeMap {
		g.Edges = append(g.Edges, *e)
	}
	return g, nil
}

// QASummary reports QA-sample review results.
func (s *Store) QASummary(ctx context.Context) (map[string]any, error) {
	var sampled, reviewed, fraud int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE status='RESOLVED'), count(*) FILTER (WHERE outcome='FRAUD')
		FROM cases WHERE source='QA_SAMPLE'`).Scan(&sampled, &reviewed, &fraud); err != nil {
		return nil, err
	}
	missRate := 0.0
	if reviewed > 0 {
		missRate = float64(fraud) / float64(reviewed)
	}
	// Wilson 95% CI for the miss rate.
	z := 1.96
	ciLow, ciHigh := wilsonCI(fraud, reviewed, z)
	return map[string]any{
		"sampled": sampled, "reviewed": reviewed, "fraud_found": fraud,
		"estimated_miss_rate": missRate, "wilson_ci_low": ciLow, "wilson_ci_high": ciHigh,
	}, nil
}

func wilsonCI(successes, n int, z float64) (float64, float64) {
	if n == 0 {
		return 0, 0
	}
	p := float64(successes) / float64(n)
	denom := 1 + z*z/float64(n)
	center := (p + z*z/(2*float64(n))) / denom
	spread := z * (math.Sqrt(p*(1-p)/float64(n) + z*z/(4*float64(n)*float64(n)))) / denom
	lo := center - spread
	hi := center + spread
	if lo < 0 {
		lo = 0
	}
	if hi > 1 {
		hi = 1
	}
	return lo, hi
}

// EventsAfter returns case_events with id greater than afterID.
func (s *Store) EventsAfter(ctx context.Context, afterID int64, limit int) ([]CaseEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, case_id, at, actor, actor_role, type, payload
		FROM case_events WHERE id > $1 ORDER BY id LIMIT $2`, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CaseEvent
	for rows.Next() {
		var ev CaseEvent
		var payload []byte
		if err := rows.Scan(&ev.ID, &ev.CaseID, &ev.At, &ev.Actor, &ev.ActorRole, &ev.Type, &payload); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(payload, &ev.Payload)
		out = append(out, ev)
	}
	return out, rows.Err()
}

// RecentDecision is a stored decision used for rule dry-runs.
type RecentDecision struct {
	ID       string
	EventID  string
	Decision string
	Amount   int64
	TxnType  string
	Features features.FeatureVector
	Label    *string
}

// RecentDecisions returns stored decisions from the last `hours` hours.
func (s *Store) RecentDecisions(ctx context.Context, hours int, sample int) ([]RecentDecision, error) {
	if hours <= 0 || hours > 168 {
		hours = 24
	}
	if sample <= 0 || sample > 100000 {
		sample = 5000
	}
	from := time.Now().UTC().Add(-time.Duration(hours) * time.Hour)
	rows, err := s.pool.Query(ctx, `
		SELECT d.id, d.event_id, d.decision, d.amount, d.txn_type, d.features,
			(SELECT l.label FROM labels l WHERE l.event_id = d.event_id LIMIT 1)
		FROM decisions d WHERE d.decided_at >= $1
		ORDER BY d.decided_at DESC LIMIT $2`, from, sample)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RecentDecision
	for rows.Next() {
		var rd RecentDecision
		var featuresRaw []byte
		var label *string
		if err := rows.Scan(&rd.ID, &rd.EventID, &rd.Decision, &rd.Amount, &rd.TxnType, &featuresRaw, &label); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(featuresRaw, &rd.Features)
		rd.Label = label
		out = append(out, rd)
	}
	return out, rows.Err()
}

// Metrics summarises queue health for the dashboard.
type Metrics struct {
	Open             int            `json:"open"`
	InReview         int            `json:"in_review"`
	Escalated        int            `json:"escalated"`
	PendingApproval  int            `json:"pending_approval"`
	Breached         int            `json:"breached"`
	ResolvedToday    int            `json:"resolved_today"`
	MedianResolution float64        `json:"median_resolution_minutes"`
	ByOutcome        map[string]int `json:"by_outcome"`
	ByReason         map[string]int `json:"by_reason"`
}

// Metrics reads queue counters.
func (s *Store) Metrics(ctx context.Context, now time.Time) (Metrics, error) {
	m := Metrics{ByOutcome: map[string]int{}, ByReason: map[string]int{}}
	if err := s.pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE status='OPEN'),
			count(*) FILTER (WHERE status='IN_REVIEW'),
			count(*) FILTER (WHERE status='ESCALATED'),
			count(*) FILTER (WHERE pending_approval),
			count(*) FILTER (WHERE status IN ('OPEN','IN_REVIEW','ESCALATED') AND sla_due_at < $1),
			count(*) FILTER (WHERE resolved_at >= $2)
		FROM cases`, now, now.Truncate(24*time.Hour)).Scan(
		&m.Open, &m.InReview, &m.Escalated, &m.PendingApproval, &m.Breached, &m.ResolvedToday); err != nil {
		return m, err
	}
	_ = s.pool.QueryRow(ctx, `
		SELECT coalesce(percentile_cont(0.5) WITHIN GROUP (
			ORDER BY EXTRACT(EPOCH FROM (resolved_at - opened_at))/60), 0)
		FROM cases WHERE resolved_at IS NOT NULL AND resolved_at >= $1`,
		now.Add(-7*24*time.Hour)).Scan(&m.MedianResolution)

	rows, err := s.pool.Query(ctx,
		`SELECT outcome, count(*) FROM cases WHERE outcome IS NOT NULL GROUP BY outcome`)
	if err != nil {
		return m, err
	}
	for rows.Next() {
		var outcome string
		var n int
		if err := rows.Scan(&outcome, &n); err != nil {
			rows.Close()
			return m, err
		}
		m.ByOutcome[outcome] = n
	}
	rows.Close()
	rows, err = s.pool.Query(ctx,
		`SELECT coalesce(reason_code,'unknown'), count(*) FROM cases
		 WHERE status='RESOLVED' GROUP BY 1 ORDER BY 2 DESC LIMIT 20`)
	if err != nil {
		return m, err
	}
	defer rows.Close()
	for rows.Next() {
		var reason string
		var n int
		if err := rows.Scan(&reason, &n); err != nil {
			return m, err
		}
		m.ByReason[reason] = n
	}
	return m, rows.Err()
}

// LabelAgreement returns Cohen's kappa inputs from blind second reviews.
func (s *Store) LabelAgreement(ctx context.Context) (map[string]any, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT l.label, sr.outcome, count(*)
		FROM second_reviews sr
		JOIN label_history l ON l.case_id = sr.case_id
			AND l.superseded_by IS NULL AND l.superseded_at IS NULL
		GROUP BY 1,2`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type pair struct{ a, b string }
	counts := map[pair]int{}
	total := 0
	for rows.Next() {
		var a, b string
		var n int
		if err := rows.Scan(&a, &b, &n); err != nil {
			return nil, err
		}
		counts[pair{a, b}] = n
		total += n
	}
	if total == 0 {
		return map[string]any{"samples": 0, "kappa": nil}, rows.Err()
	}
	labels := []string{"FRAUD", "LEGIT"}
	observed := 0
	for _, l := range labels {
		observed += counts[pair{l, l}]
	}
	po := float64(observed) / float64(total)
	var pe float64
	for _, l := range labels {
		var ra, rb int
		for _, other := range labels {
			ra += counts[pair{l, other}]
			rb += counts[pair{other, l}]
		}
		pe += (float64(ra) / float64(total)) * (float64(rb) / float64(total))
	}
	kappa := 0.0
	if pe < 1 {
		kappa = (po - pe) / (1 - pe)
	}
	return map[string]any{"samples": total, "observed_agreement": po, "kappa": kappa}, rows.Err()
}

// SecondReviewSampling records the blind review outcome for a share of cases.
func (s *Store) SecondReviewSampling(ctx context.Context, caseID int64, reviewer, outcome string, now time.Time) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO second_reviews (case_id, reviewer, outcome, at) VALUES ($1,$2,$3,$4)`,
		caseID, reviewer, outcome, now)
	return err
}

// HasDecision reports whether a decision is already attached to a case, as the
// primary decision or as a linked one. The triage worker uses it to make case
// creation at-least-once safe.
func (s *Store) HasDecision(ctx context.Context, decisionID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM cases
			WHERE primary_decision_id = $1 OR $1 = ANY(linked_decision_ids)
		)`, decisionID).Scan(&exists)
	return exists, err
}

// Breach escalates a case whose SLA elapsed. This is a system transition: the
// analyst state machine does not allow OPEN -> ESCALATED, but the SLA policy
// must not depend on an analyst having claimed the case first.
func (s *Store) Breach(ctx context.Context, id int64, now time.Time) (*Case, error) {
	var out *Case
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		cur, err := s.lockTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if cur.Status == StatusResolved {
			return ErrIllegalTransition
		}
		if cur.Escalated {
			out = cur
			return nil
		}
		if _, err := tx.Exec(ctx, `
			UPDATE cases SET status='ESCALATED', escalated=TRUE, last_activity_at=$1,
				version=version+1 WHERE id=$2`, now, id); err != nil {
			return err
		}
		if err := s.appendEvent(ctx, tx, id, "case-worker", "service", "sla_breached", map[string]any{
			"sla_due_at": cur.SLADueAt, "policy": s.cfg.BreachPolicy,
		}, now); err != nil {
			return err
		}
		if err := s.enqueue(ctx, tx, "case.actions", fmt.Sprint(id), map[string]any{
			"case_id": id, "action": "sla_breached", "status": "ESCALATED", "at": now,
		}); err != nil {
			return err
		}
		c, err := s.getTx(ctx, tx, id)
		if err != nil {
			return err
		}
		out = c
		return nil
	})
	return out, err
}

// LookupIdempotent returns a previously stored response for a key.
func (s *Store) LookupIdempotent(ctx context.Context, key string) ([]byte, bool, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx,
		`SELECT response FROM case_idempotency WHERE key=$1`, key).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

// SaveIdempotent records a response for a key. The first writer wins, so two
// concurrent retries return the same body.
func (s *Store) SaveIdempotent(ctx context.Context, key string, caseID int64, action string, payload []byte) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO case_idempotency (key, case_id, action, response)
		VALUES ($1,$2,$3,$4) ON CONFLICT (key) DO NOTHING`, key, caseID, action, payload)
	return err
}

func (s *Store) appendEvent(ctx context.Context, tx pgx.Tx, caseID int64, actor, role, typ string,
	payload map[string]any, at time.Time) error {
	if payload == nil {
		payload = map[string]any{}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO case_events (case_id, at, actor, actor_role, type, payload) VALUES ($1,$2,$3,$4,$5,$6)`,
		caseID, at, actor, role, typ, raw)
	return err
}

func (s *Store) enqueue(ctx context.Context, tx pgx.Tx, topic, key string, payload map[string]any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox (topic, key, payload) VALUES ($1,$2,$3)`, topic, key, raw)
	return err
}

func nullString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}
