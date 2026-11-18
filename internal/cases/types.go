package cases

import "time"

type Status string

const (
	StatusOpen      Status = "OPEN"
	StatusInReview  Status = "IN_REVIEW"
	StatusEscalated Status = "ESCALATED"
	StatusResolved  Status = "RESOLVED"
	StatusReopened  Status = "REOPENED"
)

type Outcome string

const (
	OutcomeFraud        Outcome = "FRAUD"
	OutcomeLegit        Outcome = "LEGIT"
	OutcomeInconclusive Outcome = "INCONCLUSIVE"
)

type Source string

const (
	SourceDecision     Source = "DECISION"
	SourceStepUpFailed Source = "STEP_UP_FAILED"
	SourceQASample     Source = "QA_SAMPLE"
	SourceManual       Source = "MANUAL"
)

type Case struct {
	ID              int64      `json:"id"`
	Source          Source     `json:"source"`
	DecisionClass   string     `json:"decision_class"`
	PrimaryDecision string     `json:"primary_decision_id"`
	LinkedDecisions []string   `json:"linked_decision_ids"`
	AccountID       string     `json:"account_id"`
	RiskScore       int        `json:"risk_score"`
	AmountTotal     int64      `json:"amount_total"`
	Priority        int        `json:"priority"`
	Status          Status     `json:"status"`
	Assignee        *string    `json:"assignee"`
	Outcome         *Outcome   `json:"outcome"`
	ReasonCode      *string    `json:"reason_code"`
	OutcomeConf     *string    `json:"outcome_confidence"`
	PendingApproval bool       `json:"pending_approval"`
	ApprovedBy      *string    `json:"approved_by"`
	FollowUpAt      *time.Time `json:"follow_up_at"`
	ClaimedAt       *time.Time `json:"claimed_at"`
	LastActivityAt  *time.Time `json:"last_activity_at"`
	RepeatAccount   bool       `json:"repeat_account"`
	Escalated       bool       `json:"escalated"`
	SLADueAt        *time.Time `json:"sla_due_at"`
	SLAPausedAt     *time.Time `json:"sla_paused_at"`
	Version         int        `json:"version"`
	OpenedAt        time.Time  `json:"opened_at"`
	ResolvedAt      *time.Time `json:"resolved_at"`
	SecondReviewOf  *int64     `json:"second_review_of"`
	// PropagateLinked records the analyst's choice to label the linked
	// decisions too. It is persisted so a four-eyes approval applies the same
	// scope the analyst asked for.
	PropagateLinked bool `json:"-"`
}

type CaseEvent struct {
	ID        int64          `json:"id"`
	CaseID    int64          `json:"case_id"`
	At        time.Time      `json:"at"`
	Actor     string         `json:"actor"`
	ActorRole string         `json:"actor_role"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
}

type Note struct {
	ID     int64     `json:"id"`
	CaseID int64     `json:"case_id"`
	At     time.Time `json:"at"`
	Actor  string    `json:"actor"`
	Text   string    `json:"text"`
}

// Transition describes an allowed state change.
type Transition struct {
	From   Status
	To     Status
	Action string
	Roles  []string
}
