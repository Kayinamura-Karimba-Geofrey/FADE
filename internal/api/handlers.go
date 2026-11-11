package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/api/oapi"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// ------------------------------------------------------------------ decide

var idemKeyPattern = regexp.MustCompile(`^[\x20-\x7e]{1,128}$`)

func (s *Server) decide(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r, MaxDecideBody, "event JSON")
	if !ok {
		return
	}
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key != "" && !idemKeyPattern.MatchString(key) {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY", "Invalid idempotency key",
			"Idempotency-Key must be 1-128 printable characters")
		return
	}
	ev, verr := events.Parse(body, time.Now())
	if verr != nil {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_EVENT", "Invalid event", verr.Error(),
			oapi.FieldError{Field: "event", Detail: verr.Detail})
		return
	}
	includeFeatures, includeExplain := parseInclude(r)
	if len(body) == 0 {
		writeProblem(w, r, http.StatusBadRequest, "EMPTY_BODY", "Empty body", "an event object is required")
		return
	}
	dec, err := s.deps.Pipeline.Decide(r.Context(), ev, pipeline.Options{
		IdempotencyKey:  key,
		IncludeFeatures: includeFeatures,
		IncludeExplain:  includeExplain,
		FailPolicyApply: r.Header.Get("X-Fail-Policy") == "apply",
	})
	if err != nil {
		s.writePipelineError(w, r, err)
		return
	}
	w.Header().Set("X-Decision-ID", dec.DecisionID)
	w.Header().Set("Cache-Control", "no-store")
	if dec.Replay {
		w.Header().Set("Idempotent-Replay", "true")
	}
	writeJSON(w, http.StatusOK, dec)
}

func parseInclude(r *http.Request) (features, explain bool) {
	for _, part := range strings.Split(r.URL.Query().Get("include"), ",") {
		switch strings.TrimSpace(strings.ToLower(part)) {
		case "features":
			features = true
		case "explain":
			explain = true
		}
	}
	return
}

func readBody(w http.ResponseWriter, r *http.Request, limit int64, what string) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeProblem(w, r, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", "Body too large",
				fmt.Sprintf("%s must not exceed %d bytes", what, limit))
			return nil, false
		}
		writeProblem(w, r, http.StatusBadRequest, "INVALID_BODY", "Invalid body", err.Error())
		return nil, false
	}
	if len(body) == 0 {
		writeProblem(w, r, http.StatusBadRequest, "EMPTY_BODY", "Empty body", what+" is required")
		return nil, false
	}
	return body, true
}

func (s *Server) writePipelineError(w http.ResponseWriter, r *http.Request, err error) {
	var pe *pipeline.Error
	if !errors.As(err, &pe) {
		s.deps.Logger.Error("decision failed", "error", err)
		writeProblem(w, r, http.StatusInternalServerError, "INTERNAL", "Internal error",
			"the decision could not be produced")
		return
	}
	switch pe.Kind {
	case pipeline.KindShed:
		w.Header().Set("Retry-After", "1")
	case pipeline.KindIdemConflict:
		writeProblem(w, r, http.StatusUnprocessableEntity, "IDEMPOTENCY_CONFLICT",
			"Idempotency conflict", pe.Message)
		return
	case pipeline.KindIdempotencyInFlight:
		w.Header().Set("Retry-After", "1")
	}
	if pe.Err != nil {
		s.deps.Logger.Error("decision failed", "kind", pe.Kind, "error", pe.Err)
	}
	writeProblem(w, r, pe.Status, pe.Kind, strings.ReplaceAll(pe.Kind, "_", " "), pe.Message)
}

// ------------------------------------------------------------------- batch

type batchRequest struct {
	Events []json.RawMessage `json:"events"`
}

type batchItem struct {
	Index    int                `json:"index"`
	EventID  string             `json:"event_id"`
	Decision *pipeline.Decision `json:"decision,omitempty"`
	Error    *oapi.Problem      `json:"error,omitempty"`
}

type batchResponse struct {
	Results   []batchItem `json:"results"`
	Succeeded int         `json:"succeeded"`
	Failed    int         `json:"failed"`
}

func (s *Server) decideBatch(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r, MaxBatchBody, "batch JSON")
	if !ok {
		return
	}
	var req batchRequest
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_JSON", "Invalid JSON", err.Error())
		return
	}
	if len(req.Events) == 0 || len(req.Events) > 500 {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_BATCH", "Invalid batch",
			"events must contain between 1 and 500 items")
		return
	}

	type parsed struct {
		index int
		ev    *events.Event
		err   *oapi.Problem
	}
	items := make([]parsed, len(req.Events))
	groups := map[string][]int{}
	order := make([]string, 0, len(req.Events))
	for i, raw := range req.Events {
		ev, verr := events.Parse(raw, time.Now())
		if verr != nil {
			items[i] = parsed{index: i, err: problemValue(r, http.StatusBadRequest, "INVALID_EVENT", "Invalid event", verr.Detail)}
			continue
		}
		items[i] = parsed{index: i, ev: ev}
		if _, seen := groups[ev.Sender.AccountID]; !seen {
			order = append(order, ev.Sender.AccountID)
		}
		groups[ev.Sender.AccountID] = append(groups[ev.Sender.AccountID], i)
	}

	results := make([]batchItem, len(req.Events))
	for i, item := range items {
		if item.err != nil {
			results[i] = batchItem{Index: i, EventID: eventIDFromRaw(req.Events[i]), Error: item.err}
		}
	}
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	for _, account := range order {
		indices := groups[account]
		wg.Add(1)
		go func(indices []int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			for _, idx := range indices {
				ev := items[idx].ev
				d, err := s.deps.Pipeline.Decide(r.Context(), ev, pipeline.Options{IdempotencyKey: ev.EventID})
				if err != nil {
					var pe *pipeline.Error
					status := http.StatusInternalServerError
					code := "INTERNAL"
					detail := err.Error()
					if errors.As(err, &pe) {
						status, code = pe.Status, pe.Kind
						detail = pe.Message
					}
					results[idx] = batchItem{Index: idx, EventID: ev.EventID,
						Error: problemValue(r, status, code, strings.ReplaceAll(code, "_", " "), detail)}
					continue
				}
				results[idx] = batchItem{Index: idx, EventID: ev.EventID, Decision: d}
			}
		}(indices)
	}
	wg.Wait()
	resp := batchResponse{Results: results}
	for _, item := range results {
		if item.Error != nil {
			resp.Failed++
		} else {
			resp.Succeeded++
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func eventIDFromRaw(raw json.RawMessage) string {
	var probe struct {
		EventID string `json:"event_id"`
	}
	_ = json.Unmarshal(raw, &probe)
	return probe.EventID
}

// --------------------------------------------------------------- decisions

func (s *Server) getDecision(w http.ResponseWriter, r *http.Request) {
	if s.deps.Pipeline.Store() == nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "STORE_UNAVAILABLE", "Store unavailable",
			"decision history is not configured")
		return
	}
	id := chi.URLParam(r, "id")
	rec, err := s.deps.Pipeline.Store().GetDecision(r.Context(), id)
	if err != nil {
		writeProblem(w, r, http.StatusNotFound, "DECISION_NOT_FOUND", "Decision not found", "no decision with that id")
		return
	}
	d := rec.Decision.Clone()
	if !strings.Contains(r.URL.Query().Get("include"), "features") {
		d.Features = nil
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) listDecisions(w http.ResponseWriter, r *http.Request) {
	if s.deps.Pipeline.Store() == nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "STORE_UNAVAILABLE", "Store unavailable",
			"decision history is not configured")
		return
	}
	q := r.URL.Query()
	f := pipeline.DecisionFilter{
		AccountID: q.Get("account_id"), Decision: q.Get("decision"),
		RuleID: q.Get("rule_id"), Cursor: q.Get("cursor"),
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			writeProblem(w, r, http.StatusBadRequest, "INVALID_LIMIT", "Invalid limit", "limit must be 1-200")
			return
		}
		f.Limit = n
	}
	if f.Decision != "" && !rules.Decision(f.Decision).Valid() {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_DECISION", "Invalid decision", "unknown decision filter")
		return
	}
	var err error
	if f.From, err = parseOptionalTime(q.Get("from")); err != nil {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_TIME", "Invalid time", "from must be RFC3339")
		return
	}
	if f.To, err = parseOptionalTime(q.Get("to")); err != nil {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_TIME", "Invalid time", "to must be RFC3339")
		return
	}
	recs, next, err := s.deps.Pipeline.Store().ListDecisions(r.Context(), f)
	if err != nil {
		s.deps.Logger.Error("list decisions failed", "error", err)
		writeProblem(w, r, http.StatusInternalServerError, "QUERY_FAILED", "Query failed", "could not list decisions")
		return
	}
	out := make([]*pipeline.Decision, 0, len(recs))
	for _, rec := range recs {
		d := rec.Decision.Clone()
		d.Features = nil
		out = append(out, d)
	}
	writeJSON(w, http.StatusOK, map[string]any{"decisions": out, "next_cursor": next})
}

func parseOptionalTime(v string) (time.Time, error) {
	if v == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, v)
}

// ------------------------------------------------------------------- rules

func (s *Server) reloadRules(w http.ResponseWriter, r *http.Request) {
	previous := s.deps.Rules.Current().Version
	start := time.Now()
	rs, err := s.deps.Rules.Reload(r.Context(), actorFrom(r))
	if err != nil {
		s.writeCompileError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"active": true, "version": rs.Version, "hash": rs.Hash,
		"rules": len(rs.Rules()), "previous_version": previous,
		"duration_ms": float64(time.Since(start).Microseconds()) / 1000,
	})
}

func (s *Server) activeRules(w http.ResponseWriter, r *http.Request) {
	rs := s.deps.Rules.Current()
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot": rs.Snapshot(true, s.deps.Rules.Source()),
		"rule_set": rs.File(),
	})
}

func (s *Server) ruleVersions(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"versions": s.deps.Rules.Versions(r.Context())})
}

func (s *Server) putRule(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r, MaxRuleBody, "rule JSON")
	if !ok {
		return
	}
	var in oapi.Rule
	if !decodeStrict(w, r, body, &in) {
		return
	}
	id := chi.URLParam(r, "id")
	rs, err := s.deps.Rules.PutRule(r.Context(), id, toRule(in), actorFrom(r))
	if err != nil {
		s.writeCompileError(w, r, err)
		return
	}
	s.audit(r, "rules.put", "rule", id, nil, in)
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot": rs.Snapshot(true, s.deps.Rules.Source()),
		"rule_set": rs.File(),
	})
}

func (s *Server) validateRules(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r, MaxRuleBody, "rule set JSON")
	if !ok {
		return
	}
	rs, err := rules.Load(body)
	if err != nil {
		s.writeCompileError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"valid": true, "snapshot": rs.Snapshot(false, "file"),
	})
}

func (s *Server) writeCompileError(w http.ResponseWriter, r *http.Request, err error) {
	var ce *rules.CompileError
	if !errors.As(err, &ce) {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_RULESET", "Invalid rule set", err.Error())
		return
	}
	fields := make([]oapi.FieldError, 0, len(ce.Errors))
	for _, e := range ce.Errors {
		fields = append(fields, oapi.FieldError{Field: e.Path, Detail: e.Message})
	}
	writeProblem(w, r, http.StatusUnprocessableEntity, "INVALID_RULESET", "Invalid rule set",
		ce.Error(), fields...)
}

func decodeStrict(w http.ResponseWriter, r *http.Request, body []byte, v any) bool {
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_JSON", "Invalid JSON", err.Error())
		return false
	}
	return true
}

func actorFrom(r *http.Request) string {
	if p := PrincipalFrom(r.Context()); p != nil {
		return p.Subject
	}
	return "unknown"
}

// ------------------------------------------------------------------- lists

func (s *Server) listLists(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	entries, err := s.deps.Pipeline.Lists().Entries(r.Context(), q.Get("list_type"), q.Get("subject"), q.Get("value"), limit)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "QUERY_FAILED", "Query failed", "could not list entries")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries, "count": len(entries)})
}

func (s *Server) addList(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r, MaxDecideBody, "list entry JSON")
	if !ok {
		return
	}
	var in oapi.ListEntryInput
	if !decodeStrict(w, r, body, &in) {
		return
	}
	if strings.TrimSpace(in.Value) == "" || len(in.Value) > 128 {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_LIST_ENTRY", "Invalid list entry",
			"value must be 1-128 characters")
		return
	}
	if in.ExpiresAt != nil && !in.ExpiresAt.After(time.Now()) {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_LIST_ENTRY", "Invalid list entry",
			"expires_at must be in the future")
		return
	}
	entry, err := s.deps.Pipeline.Lists().Add(r.Context(), toListEntry(in, actorFrom(r)))
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "LIST_WRITE_FAILED", "List write failed", err.Error())
		return
	}
	s.audit(r, "list.add", "list_entry", strconv.FormatInt(entry.ID, 10), nil, entry)
	writeJSON(w, http.StatusCreated, entry)
}

func (s *Server) deleteList(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_ID", "Invalid id", "id must be an integer")
		return
	}
	ok, err := s.deps.Pipeline.Lists().Delete(r.Context(), id)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "LIST_WRITE_FAILED", "List write failed", err.Error())
		return
	}
	if !ok {
		writeProblem(w, r, http.StatusNotFound, "LIST_ENTRY_NOT_FOUND", "List entry not found", "no entry with that id")
		return
	}
	s.audit(r, "list.delete", "list_entry", strconv.FormatInt(id, 10), map[string]any{"id": id}, nil)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) ruleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.deps.Pipeline.Store() == nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "STORE_UNAVAILABLE", "Store unavailable",
			"decision history is not configured")
		return
	}
	window, err := parseWindow(r.URL.Query().Get("window"))
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_WINDOW", "Invalid window",
			"window must look like 30m, 24h or 7d")
		return
	}
	res, err := s.deps.Pipeline.Store().RuleMetrics(r.Context(), window)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "QUERY_FAILED", "Query failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

var windowPattern = regexp.MustCompile(`^([0-9]{1,5})(m|h|d)$`)

func parseWindow(v string) (time.Duration, error) {
	if v == "" {
		return 24 * time.Hour, nil
	}
	m := windowPattern.FindStringSubmatch(v)
	if m == nil {
		return 0, fmt.Errorf("invalid window")
	}
	n, _ := strconv.Atoi(m[1])
	unit := time.Minute
	switch m[2] {
	case "h":
		unit = time.Hour
	case "d":
		unit = 24 * time.Hour
	}
	return time.Duration(n) * unit, nil
}

func (s *Server) audit(r *http.Request, action, objectType, objectID string, before, after any) {
	if s.deps.Auditor == nil {
		return
	}
	if err := s.deps.Auditor.Record(r.Context(), actorFrom(r), action, objectType, objectID, before, after); err != nil {
		s.deps.Logger.Error("audit write failed", "action", action, "error", err)
	}
}

// keep context import used by tests and future middleware.
var _ = context.Background
