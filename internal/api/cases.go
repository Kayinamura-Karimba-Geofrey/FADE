package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/cases"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// caseView is the analyst-facing projection. Raw account and decision
// identifiers never leave the API: they are replaced by keyed pseudonyms.
type caseView struct {
	*cases.Case
	AccountRef  string   `json:"account_ref"`
	AssigneeRef string   `json:"assignee_ref"`
	DecisionRef string   `json:"decision_ref"`
	LinkedRefs  []string `json:"linked_refs"`
	AgeMinutes  int      `json:"age_minutes"`
	SLAState    string   `json:"sla_state"`
}

func (s *Server) pseudonym() *cases.Pseudonymizer {
	return cases.NewPseudonymizer(s.deps.PseudonymKey)
}

func (s *Server) caseView(c *cases.Case) caseView {
	p := s.pseudonym()
	linked := make([]string, 0, len(c.LinkedDecisions))
	for _, d := range c.LinkedDecisions {
		linked = append(linked, p.ID("dec", d))
	}
	v := caseView{
		Case: c, AccountRef: p.ID("acct", c.AccountID),
		DecisionRef: p.ID("dec", c.PrimaryDecision), LinkedRefs: linked,
		AgeMinutes: int(time.Since(c.OpenedAt).Minutes()),
	}
	if c.Assignee != nil && *c.Assignee != "" {
		v.AssigneeRef = p.ID("usr", *c.Assignee)
	}
	// Internal user ids are never displayed; the dashboard shows the ref.
	v.AccountID = ""
	v.Assignee = nil
	v.PrimaryDecision = ""
	v.LinkedDecisions = nil
	switch {
	case c.PendingApproval:
		// Parked for a second reviewer; the clock is stopped, not met.
		v.SLAState = "paused"
	case c.Status == cases.StatusResolved:
		v.SLAState = "met"
	case c.SLADueAt == nil:
		v.SLAState = "none"
	case c.SLAPausedAt != nil:
		v.SLAState = "paused"
	case time.Now().After(*c.SLADueAt):
		v.SLAState = "breached"
	default:
		v.SLAState = "ok"
	}
	return v
}

func maskEvents(p *cases.Pseudonymizer, events []cases.CaseEvent) []cases.CaseEvent {
	for i := range events {
		events[i].Actor = p.ID("usr", events[i].Actor)
	}
	return events
}

func (s *Server) requireCases(w http.ResponseWriter, r *http.Request) *cases.Store {
	if s.deps.Cases == nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "CASES_UNAVAILABLE", "Case management unavailable",
			"the case store is not configured on this deployment")
		return nil
	}
	return s.deps.Cases
}

// idempotent runs fn at most once for a given Idempotency-Key. The key is
// mandatory on mutating endpoints so a retried request can never double-resolve
// a case or double-write a label.
func (s *Server) idempotent(w http.ResponseWriter, r *http.Request, caseID int64, action string,
	fn func() (any, error)) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || !idemKeyPattern.MatchString(key) {
		writeProblem(w, r, http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency key required",
			"mutating case actions require an Idempotency-Key header of 1-128 printable characters")
		return
	}
	ctx := r.Context()
	if raw, found, err := s.deps.Cases.LookupIdempotent(ctx, key); err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "IDEMPOTENCY_FAILED", "Idempotency check failed", err.Error())
		return
	} else if found {
		w.Header().Set("Idempotent-Replay", "true")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(raw)
		return
	}
	result, err := fn()
	if err != nil {
		s.writeCaseError(w, r, err)
		return
	}
	raw, err := json.Marshal(result)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "ENCODE_FAILED", "Encode failed", err.Error())
		return
	}
	if err := s.deps.Cases.SaveIdempotent(ctx, key, caseID, action, raw); err != nil {
		s.deps.Logger.Warn("idempotency record failed", "key", key, "error", err)
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) writeCaseError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, cases.ErrNotFound):
		writeProblem(w, r, http.StatusNotFound, "CASE_NOT_FOUND", "Case not found", "no case with that id")
	case errors.Is(err, cases.ErrVersionConflict):
		writeProblem(w, r, http.StatusConflict, "VERSION_CONFLICT", "Case changed",
			"another analyst modified this case; reload and try again")
	case errors.Is(err, cases.ErrIllegalTransition):
		writeProblem(w, r, http.StatusConflict, "ILLEGAL_TRANSITION", "Illegal transition", err.Error())
	case errors.Is(err, cases.ErrSelfApproval):
		writeProblem(w, r, http.StatusForbidden, "SELF_APPROVAL", "Self approval rejected",
			"a different reviewer must approve this resolution")
	case errors.Is(err, cases.ErrNotPendingApproval):
		writeProblem(w, r, http.StatusConflict, "NOT_PENDING_APPROVAL", "Not awaiting approval",
			"this case has no resolution awaiting approval")
	case errors.Is(err, cases.ErrRedactedContent):
		writeProblem(w, r, http.StatusUnprocessableEntity, "REDACTED_CONTENT", "Sensitive content",
			"notes must not contain phone numbers, emails or card numbers")
	case errors.Is(err, cases.ErrNoteTooLong):
		writeProblem(w, r, http.StatusUnprocessableEntity, "NOTE_TOO_LONG", "Note too long",
			"notes are limited to 2000 characters")
	case errors.Is(err, cases.ErrFollowUpRequired):
		writeProblem(w, r, http.StatusUnprocessableEntity, "FOLLOW_UP_REQUIRED", "Follow-up required",
			"inconclusive resolutions require follow_up_at")
	case errors.Is(err, cases.ErrReasonRequired):
		writeProblem(w, r, http.StatusBadRequest, "REASON_REQUIRED", "Reason required",
			"a reason code is required for this action")
	case errors.Is(err, cases.ErrOutcomeRequired):
		writeProblem(w, r, http.StatusBadRequest, "OUTCOME_REQUIRED", "Outcome required",
			"outcome must be FRAUD, LEGIT or INCONCLUSIVE")
	case errors.Is(err, cases.ErrApprovalRequired):
		writeProblem(w, r, http.StatusForbidden, "APPROVAL_REQUIRED", "Approval required",
			"this resolution needs a second reviewer")
	default:
		s.deps.Logger.Error("case action failed", "error", err)
		writeProblem(w, r, http.StatusInternalServerError, "CASE_ACTION_FAILED", "Case action failed",
			"the action could not be completed")
	}
}

// ------------------------------------------------------------------ read side

func (s *Server) listCases(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	q := r.URL.Query()
	f := cases.Filter{}
	for _, part := range strings.Split(q.Get("status"), ",") {
		if part = strings.TrimSpace(strings.ToUpper(part)); part != "" {
			f.Statuses = append(f.Statuses, cases.Status(part))
		}
	}
	if len(f.Statuses) == 0 {
		f.Statuses = []cases.Status{cases.StatusOpen, cases.StatusInReview, cases.StatusEscalated, cases.StatusReopened}
	}
	f.Assignee = q.Get("assignee")
	f.Unassigned = q.Get("unassigned") == "true"
	f.Source = cases.Source(q.Get("source"))
	f.DecisionClass = q.Get("decision_class")
	if v := q.Get("min_priority"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 100 {
			writeProblem(w, r, http.StatusBadRequest, "INVALID_PRIORITY", "Invalid priority",
				"min_priority must be 0-100")
			return
		}
		f.MinPriority = n
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			writeProblem(w, r, http.StatusBadRequest, "INVALID_LIMIT", "Invalid limit", "limit must be 1-200")
			return
		}
		f.Limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeProblem(w, r, http.StatusBadRequest, "INVALID_OFFSET", "Invalid offset", "offset must be >= 0")
			return
		}
		f.Offset = n
	}
	list, total, err := store.List(r.Context(), f)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "QUERY_FAILED", "Query failed", err.Error())
		return
	}
	views := make([]caseView, 0, len(list))
	for _, c := range list {
		views = append(views, s.caseView(c))
	}
	writeJSON(w, http.StatusOK, map[string]any{"cases": views, "total": total})
}

func (s *Server) getCase(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id, ok := caseID(w, r)
	if !ok {
		return
	}
	c, err := store.Get(r.Context(), id)
	if err != nil {
		s.writeCaseError(w, r, err)
		return
	}
	events, err := store.Timeline(r.Context(), id)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "QUERY_FAILED", "Query failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"case": s.caseView(c), "timeline": maskEvents(s.pseudonym(), events),
	})
}

func (s *Server) caseMetrics(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	m, err := store.Metrics(r.Context(), time.Now().UTC())
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "QUERY_FAILED", "Query failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func (s *Server) accountTimeline(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id := chi.URLParam(r, "id")
	from, _ := time.Parse(time.RFC3339, r.URL.Query().Get("from"))
	to, _ := time.Parse(time.RFC3339, r.URL.Query().Get("to"))
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	rows, next, err := store.AccountTimeline(r.Context(), id, from, to, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "QUERY_FAILED", "Query failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"decisions": rows, "next_cursor": next})
}

func (s *Server) caseGraph(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id, ok := caseID(w, r)
	if !ok {
		return
	}
	c, err := store.Get(r.Context(), id)
	if err != nil {
		s.writeCaseError(w, r, err)
		return
	}
	g, err := store.AccountGraph(r.Context(), c.AccountID, s.pseudonym())
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "QUERY_FAILED", "Query failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, g)
}

func (s *Server) accountSummary(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id := chi.URLParam(r, "id")
	sum, err := store.AccountSummary(r.Context(), id)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "QUERY_FAILED", "Query failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, sum)
}

func (s *Server) qaSummary(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	res, err := store.QASummary(r.Context())
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "QUERY_FAILED", "Query failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) dryRunRules(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	body, ok := readBody(w, r, MaxRuleBody, "dry-run JSON")
	if !ok {
		return
	}
	var in struct {
		RuleJSON    json.RawMessage `json:"rule_json"`
		RuleSetJSON json.RawMessage `json:"rule_set_json"`
		Hours       int             `json:"hours"`
		Sample      int             `json:"sample"`
	}
	if !decodeStrict(w, r, body, &in) {
		return
	}
	if in.Hours < 1 || in.Hours > 168 {
		in.Hours = 24
	}
	// compile candidate
	var f rules.RuleSetFile
	if len(in.RuleSetJSON) > 0 {
		if err := json.Unmarshal(in.RuleSetJSON, &f); err != nil {
			writeProblem(w, r, http.StatusBadRequest, "INVALID_RULE_SET", "Invalid rule set", err.Error())
			return
		}
	} else if len(in.RuleJSON) > 0 {
		var rule rules.Rule
		if err := json.Unmarshal(in.RuleJSON, &rule); err != nil {
			writeProblem(w, r, http.StatusBadRequest, "INVALID_RULE", "Invalid rule", err.Error())
			return
		}
		f = rules.RuleSetFile{
			RuleSet: "dryrun", Version: 1, Author: "dry-run",
			Config: rules.Config{Combiner: rules.CombinerConfig{}},
			Rules:  []rules.Rule{rule},
		}
	} else {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_INPUT", "Invalid input", "rule_json or rule_set_json is required")
		return
	}
	if f.RuleSet == "" {
		f.RuleSet = "dryrun"
	}
	if f.Version == 0 {
		f.Version = 1
	}
	if f.Author == "" {
		f.Author = "dry-run"
	}
	rs, err := rules.Compile(f)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "COMPILE_FAILED", "Compile failed", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	rows, err := store.RecentDecisions(ctx, in.Hours, in.Sample)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "QUERY_FAILED", "Query failed", err.Error())
		return
	}
	fires := 0
	perRule := map[string]int{}
	overlap := 0
	var tp, fp, fn, labeled int
	for _, rd := range rows {
		evalCtx := rules.EvalContextFor(&rd.Features, rd.Amount, 0, rd.TxnType, "", 0, 0)
		res := rs.EvaluateNew(&rd.Features, evalCtx)
		fired := res.FiredCount > 0
		if fired {
			fires++
			for i := 0; i < res.FiredCount; i++ {
				perRule[res.Fired[i].Rule.ID]++
			}
			if rd.Decision == "HOLD" || rd.Decision == "BLOCK" {
				overlap++
			}
		}
		if rd.Label != nil {
			labeled++
			isFraud := *rd.Label == "FRAUD"
			if fired && isFraud {
				tp++
			}
			if fired && !isFraud {
				fp++
			}
			if !fired && isFraud {
				fn++
			}
		}
	}
	precision := 0.0
	if fires > 0 {
		precision = float64(tp) / float64(fires)
	}
	recall := 0.0
	if tp+fn > 0 {
		recall = float64(tp) / float64(tp+fn)
	}
	projectedPerDay := 0.0
	if in.Hours > 0 {
		projectedPerDay = float64(fires) * 24.0 / float64(in.Hours)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"hours": in.Hours, "evaluated": len(rows), "fires": fires,
		"per_rule_fires": perRule, "overlap_with_current_hold_block": overlap,
		"projected_fires_per_day": projectedPerDay,
		"labeled": labeled, "true_positives": tp, "false_positives": fp,
		"false_negatives": fn, "precision": precision, "recall": recall,
		"caveats": []string{
			"evaluated against stored feature snapshots; ML not included",
			"labels joined where available; unlabeled rows excluded from precision/recall",
		},
	})
}

func (s *Server) caseLabelAgreement(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	res, err := store.LabelAgreement(r.Context())
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "QUERY_FAILED", "Query failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// ----------------------------------------------------------------- write side

type versionRequest struct {
	Version int `json:"version"`
}

func readVersion(r *http.Request) int {
	if v := strings.TrimSpace(r.Header.Get("If-Match")); v != "" {
		v = strings.Trim(v, `"`)
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 0
}

type assignRequest struct {
	versionRequest
	Assignee string `json:"assignee"`
}

func (s *Server) claimNextCase(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	p := PrincipalFrom(r.Context())
	f := cases.Filter{}
	if v := r.URL.Query().Get("status"); v != "" {
		for _, part := range strings.Split(v, ",") {
			if part = strings.TrimSpace(strings.ToUpper(part)); part != "" {
				f.Statuses = append(f.Statuses, cases.Status(part))
			}
		}
	}
	if v := r.URL.Query().Get("min_priority"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.MinPriority = n
		}
	}
	if r.URL.Query().Get("unassigned") == "true" {
		f.Unassigned = true
	}
	c, err := store.ClaimNext(r.Context(), p.Subject, p.Role, f)
	if err != nil {
		s.writeCaseError(w, r, err)
		return
	}
	s.audit(r, "case.claim_next", "case", strconv.FormatInt(c.ID, 10), nil, nil)
	writeJSON(w, http.StatusOK, map[string]any{"case": s.caseView(c)})
}

func (s *Server) claimCase(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id, ok := caseID(w, r)
	if !ok {
		return
	}
	p := PrincipalFrom(r.Context())
	s.idempotent(w, r, id, "claim", func() (any, error) {
		c, err := store.Claim(r.Context(), id, p.Subject, p.Role, readVersion(r))
		if err != nil {
			return nil, err
		}
		s.audit(r, "case.claim", "case", strconv.FormatInt(id, 10), nil, nil)
		return map[string]any{"case": s.caseView(c)}, nil
	})
}

func (s *Server) assignCase(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id, ok := caseID(w, r)
	if !ok {
		return
	}
	body, ok := readBody(w, r, MaxDecideBody, "assign JSON")
	if !ok {
		return
	}
	var in assignRequest
	if !decodeStrict(w, r, body, &in) {
		return
	}
	if strings.TrimSpace(in.Assignee) == "" {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_ASSIGNEE", "Invalid assignee", "assignee is required")
		return
	}
	p := PrincipalFrom(r.Context())
	version := in.Version
	if version == 0 {
		version = readVersion(r)
	}
	s.idempotent(w, r, id, "assign", func() (any, error) {
		c, err := store.Assign(r.Context(), id, in.Assignee, p.Subject, p.Role, version)
		if err != nil {
			return nil, err
		}
		s.audit(r, "case.assign", "case", strconv.FormatInt(id, 10), nil,
			map[string]any{"assignee": in.Assignee})
		return map[string]any{"case": s.caseView(c)}, nil
	})
}

func (s *Server) releaseCase(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id, ok := caseID(w, r)
	if !ok {
		return
	}
	p := PrincipalFrom(r.Context())
	s.idempotent(w, r, id, "release", func() (any, error) {
		c, err := store.Release(r.Context(), id, p.Subject, p.Role, readVersion(r))
		if err != nil {
			return nil, err
		}
		s.audit(r, "case.release", "case", strconv.FormatInt(id, 10), nil, nil)
		return map[string]any{"case": s.caseView(c)}, nil
	})
}

type escalateRequest struct {
	versionRequest
	Reason string `json:"reason"`
}

func (s *Server) escalateCase(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id, ok := caseID(w, r)
	if !ok {
		return
	}
	body, ok := readBody(w, r, MaxDecideBody, "escalate JSON")
	if !ok {
		return
	}
	var in escalateRequest
	if !decodeStrict(w, r, body, &in) {
		return
	}
	p := PrincipalFrom(r.Context())
	s.idempotent(w, r, id, "escalate", func() (any, error) {
		c, err := store.Escalate(r.Context(), id, p.Subject, p.Role, in.Reason, in.Version)
		if err != nil {
			return nil, err
		}
		s.audit(r, "case.escalate", "case", strconv.FormatInt(id, 10), nil,
			map[string]any{"reason": in.Reason})
		return map[string]any{"case": s.caseView(c)}, nil
	})
}

type resolveRequest struct {
	versionRequest
	Outcome         cases.Outcome `json:"outcome"`
	ReasonCode      string        `json:"reason_code"`
	Confidence      string        `json:"confidence"`
	FollowUpAt      *time.Time    `json:"follow_up_at"`
	PropagateLinked bool          `json:"propagate_linked"`
}

func (s *Server) resolveCase(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id, ok := caseID(w, r)
	if !ok {
		return
	}
	body, ok := readBody(w, r, MaxDecideBody, "resolution JSON")
	if !ok {
		return
	}
	var in resolveRequest
	if !decodeStrict(w, r, body, &in) {
		return
	}
	p := PrincipalFrom(r.Context())
	s.idempotent(w, r, id, "resolve", func() (any, error) {
		c, err := store.Resolve(r.Context(), id, cases.Resolution{
			Outcome: in.Outcome, ReasonCode: in.ReasonCode, Confidence: in.Confidence,
			FollowUpAt: in.FollowUpAt, PropagateLinked: in.PropagateLinked,
		}, p.Subject, p.Role, in.Version)
		if err != nil {
			return nil, err
		}
		s.audit(r, "case.resolve", "case", strconv.FormatInt(id, 10), nil, map[string]any{
			"outcome": in.Outcome, "reason_code": in.ReasonCode,
			"pending_approval": c.PendingApproval,
		})
		return map[string]any{"case": s.caseView(c)}, nil
	})
}

func (s *Server) rejectApprovalCase(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id, ok := caseID(w, r)
	if !ok {
		return
	}
	body, ok := readBody(w, r, MaxDecideBody, "reject JSON")
	if !ok {
		return
	}
	var in struct {
		Reason  string `json:"reason"`
		Version int    `json:"version"`
	}
	if !decodeStrict(w, r, body, &in) {
		return
	}
	if strings.TrimSpace(in.Reason) == "" {
		writeProblem(w, r, http.StatusBadRequest, "REASON_REQUIRED", "Reason required", "reason is required")
		return
	}
	p := PrincipalFrom(r.Context())
	version := in.Version
	if version == 0 {
		version = readVersion(r)
	}
	s.idempotent(w, r, id, "reject_approval", func() (any, error) {
		c, err := store.RejectApproval(r.Context(), id, p.Subject, p.Role, in.Reason, version)
		if err != nil {
			return nil, err
		}
		s.audit(r, "case.reject_approval", "case", strconv.FormatInt(id, 10), nil, map[string]any{"reason": in.Reason})
		return map[string]any{"case": s.caseView(c)}, nil
	})
}

func (s *Server) approveCase(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id, ok := caseID(w, r)
	if !ok {
		return
	}
	p := PrincipalFrom(r.Context())
	s.idempotent(w, r, id, "approve", func() (any, error) {
		c, err := store.Approve(r.Context(), id, p.Subject, p.Role, readVersion(r))
		if err != nil {
			return nil, err
		}
		s.audit(r, "case.approve", "case", strconv.FormatInt(id, 10), nil,
			map[string]any{"approved_by": p.Subject})
		return map[string]any{"case": s.caseView(c)}, nil
	})
}

type reopenRequest struct {
	versionRequest
	Reason string `json:"reason"`
}

func (s *Server) reopenCase(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id, ok := caseID(w, r)
	if !ok {
		return
	}
	body, ok := readBody(w, r, MaxDecideBody, "reopen JSON")
	if !ok {
		return
	}
	var in reopenRequest
	if !decodeStrict(w, r, body, &in) {
		return
	}
	p := PrincipalFrom(r.Context())
	s.idempotent(w, r, id, "reopen", func() (any, error) {
		c, err := store.Reopen(r.Context(), id, p.Subject, p.Role, in.Reason, in.Version)
		if err != nil {
			return nil, err
		}
		s.audit(r, "case.reopen", "case", strconv.FormatInt(id, 10), nil,
			map[string]any{"reason": in.Reason})
		return map[string]any{"case": s.caseView(c)}, nil
	})
}

type noteRequest struct {
	Text string `json:"text"`
}

func (s *Server) addCaseNote(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id, ok := caseID(w, r)
	if !ok {
		return
	}
	body, ok := readBody(w, r, MaxDecideBody, "note JSON")
	if !ok {
		return
	}
	var in noteRequest
	if !decodeStrict(w, r, body, &in) {
		return
	}
	p := PrincipalFrom(r.Context())
	s.idempotent(w, r, id, "note", func() (any, error) {
		ev, err := store.AddNote(r.Context(), id, p.Subject, p.Role, in.Text)
		if err != nil {
			return nil, err
		}
		masked := maskEvents(s.pseudonym(), []cases.CaseEvent{*ev})
		s.audit(r, "case.note", "case", strconv.FormatInt(id, 10), nil, nil)
		return map[string]any{"event": masked[0]}, nil
	})
}

type manualCaseRequest struct {
	AccountID     string   `json:"account_id"`
	DecisionRef   string   `json:"decision_ref"`
	DecisionClass string   `json:"decision_class"`
	RiskScore     int      `json:"risk_score"`
	AmountTotal   int64    `json:"amount_total"`
	Reasons       []string `json:"reasons"`
}

// createManualCase opens an analyst-initiated case. account_id is accepted here
// because the caller is a trusted service/analyst tool, and is hashed before it
// is stored.
func (s *Server) createManualCase(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	body, ok := readBody(w, r, MaxDecideBody, "case JSON")
	if !ok {
		return
	}
	var in manualCaseRequest
	if !decodeStrict(w, r, body, &in) {
		return
	}
	if strings.TrimSpace(in.AccountID) == "" {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_ACCOUNT", "Invalid account", "account_id is required")
		return
	}
	if in.DecisionClass != "" && in.DecisionClass != "HOLD" && in.DecisionClass != "BLOCK" && in.DecisionClass != "STEP_UP" {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_DECISION_CLASS", "Invalid decision class",
			"decision_class must be HOLD, BLOCK or STEP_UP")
		return
	}
	p := PrincipalFrom(r.Context())
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || !idemKeyPattern.MatchString(key) {
		writeProblem(w, r, http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency key required",
			"mutating case actions require an Idempotency-Key header")
		return
	}
	if raw, found, err := store.LookupIdempotent(r.Context(), key); err == nil && found {
		w.Header().Set("Idempotent-Replay", "true")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
		return
	}
	c, err := store.Create(r.Context(), cases.CreateInput{
		Source: cases.SourceManual, DecisionClass: in.DecisionClass,
		PrimaryDecision: in.DecisionRef, AccountID: in.AccountID,
		RiskScore: in.RiskScore, AmountTotal: in.AmountTotal,
		Reasons: in.Reasons, Actor: p.Subject, ActorRole: p.Role,
	})
	if err != nil {
		s.writeCaseError(w, r, err)
		return
	}
	s.audit(r, "case.create", "case", strconv.FormatInt(c.ID, 10), nil,
		map[string]any{"source": "MANUAL", "decision_class": in.DecisionClass})
	result := map[string]any{"case": s.caseView(c)}
	if raw, err := json.Marshal(result); err == nil {
		_ = store.SaveIdempotent(r.Context(), key, c.ID, "create", raw)
	}
	writeJSON(w, http.StatusCreated, result)
}

// secondReview submits a blind second opinion used for label-quality metrics.
type secondReviewRequest struct {
	versionRequest
	Outcome string `json:"outcome"`
}

func (s *Server) secondReview(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id, ok := caseID(w, r)
	if !ok {
		return
	}
	body, ok := readBody(w, r, MaxDecideBody, "review JSON")
	if !ok {
		return
	}
	var in secondReviewRequest
	if !decodeStrict(w, r, body, &in) {
		return
	}
	outcome := cases.Outcome(strings.ToUpper(strings.TrimSpace(in.Outcome)))
	if !cases.ValidOutcome(outcome) {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_OUTCOME", "Invalid outcome",
			"outcome must be FRAUD, LEGIT or INCONCLUSIVE")
		return
	}
	p := PrincipalFrom(r.Context())
	s.idempotent(w, r, id, "second_review", func() (any, error) {
		if err := store.SecondReviewSampling(r.Context(), id, p.Subject, string(outcome), time.Now().UTC()); err != nil {
			return nil, err
		}
		return map[string]any{"recorded": true}, nil
	})
}

// stepUpRequest reports the outcome of a step-up challenge.
type stepUpRequest struct {
	Passed bool `json:"passed"`
}

// stepUpResult opens a case when a step-up challenge failed. A passed challenge
// opens nothing, which keeps the analyst queue free of resolved challenges.
func (s *Server) stepUpResult(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	if id == "" {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_ID", "Invalid id", "a decision id is required")
		return
	}
	body, ok := readBody(w, r, MaxDecideBody, "step-up JSON")
	if !ok {
		return
	}
	var in stepUpRequest
	if !decodeStrict(w, r, body, &in) {
		return
	}
	if in.Passed {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	p := PrincipalFrom(r.Context())
	d, err := store.Decision(r.Context(), id)
	if err != nil {
		s.writeCaseError(w, r, err)
		return
	}
	c, created, err := store.OpenFromDecision(r.Context(), d, cases.SourceStepUpFailed, time.Now().UTC())
	if err != nil {
		s.writeCaseError(w, r, err)
		return
	}
	if created {
		s.audit(r, "case.create", "case", strconv.FormatInt(c.ID, 10), nil,
			map[string]any{"source": "STEP_UP_FAILED", "decision_ref": s.pseudonym().ID("dec", d.ID)})
		writeJSON(w, http.StatusCreated, map[string]any{"case": s.caseView(c)})
		return
	}
	if c == nil {
		writeJSON(w, http.StatusOK, map[string]any{"case": nil, "created": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"case": s.caseView(c), "created": false, "actor": p.Subject})
}

func caseID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_ID", "Invalid id", "id must be a positive integer")
		return 0, false
	}
	return id, true
}

func shortHash(v string) string {
	sum := sha256.Sum256([]byte(v))
	return hex.EncodeToString(sum[:])[:16]
}

var (
	sseConns   = map[string]int{}
	sseConnsMu sync.Mutex
)

func (s *Server) eventStream(w http.ResponseWriter, r *http.Request) {
	store := s.requireCases(w, r)
	if store == nil {
		return
	}
	p := PrincipalFrom(r.Context())
	if p == nil {
		writeProblem(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "Unauthenticated", "login required")
		return
	}
	sseConnsMu.Lock()
	if sseConns[p.Subject] >= 3 {
		sseConnsMu.Unlock()
		writeProblem(w, r, http.StatusTooManyRequests, "SSE_CAP", "Too many connections", "at most 3 SSE connections per user")
		return
	}
	sseConns[p.Subject]++
	sseConnsMu.Unlock()
	defer func() {
		sseConnsMu.Lock()
		sseConns[p.Subject]--
		if sseConns[p.Subject] <= 0 {
			delete(sseConns, p.Subject)
		}
		sseConnsMu.Unlock()
	}()

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeProblem(w, r, http.StatusInternalServerError, "NO_FLUSH", "Streaming unavailable", "response writer does not support flushing")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	lastID := int64(0)
	if v := strings.TrimSpace(r.Header.Get("Last-Event-ID")); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			lastID = n
		}
	}

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	stats := time.NewTicker(5 * time.Second)
	defer stats.Stop()
	poll := time.NewTicker(1 * time.Second)
	defer poll.Stop()

	writeEvent := func(ev cases.CaseEvent) {
		c, _ := store.Get(r.Context(), ev.CaseID)
		status, assignee, version := "", "", 0
		if c != nil {
			status = string(c.Status)
			if c.Assignee != nil {
				assignee = s.pseudonym().ID("usr", *c.Assignee)
			}
			version = c.Version
		}
		name := "case_updated"
		if ev.Type == "opened" || ev.Type == "created" {
			name = "case_created"
		}
		data, _ := json.Marshal(map[string]any{
			"id": ev.CaseID, "status": status, "assignee": assignee,
			"version": version, "type": ev.Type,
		})
		fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.ID, name, data)
		flusher.Flush()
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			fmt.Fprint(w, ": heartbeat\n\n")
			flusher.Flush()
		case <-stats.C:
			m, _ := store.Metrics(r.Context(), time.Now().UTC())
			data, _ := json.Marshal(m)
			fmt.Fprintf(w, "event: stats_tick\ndata: %s\n\n", data)
			flusher.Flush()
		case <-poll.C:
			events, err := store.EventsAfter(r.Context(), lastID, 100)
			if err != nil {
				continue
			}
			for _, ev := range events {
				writeEvent(ev)
				if ev.ID > lastID {
					lastID = ev.ID
				}
			}
		}
	}
}

// caseContext keeps the context import meaningful for future async work.
var _ = context.Background
