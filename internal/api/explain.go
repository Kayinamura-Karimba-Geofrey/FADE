package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/audit"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/explain"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/redaction"
)

// ---------------------------------------------------------------- explain

// Explain returns the structured explanation for one decision. Rendering is off
// the decision path; the request only reads the stored decision.
func (s *Server) Explain(w http.ResponseWriter, r *http.Request) {
	store := s.deps.Pipeline.Store()
	if store == nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "STORE_UNAVAILABLE", "Store unavailable",
			"decision history is not configured")
		return
	}
	audience, err := explain.ParseAudience(r.URL.Query().Get("audience"))
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_AUDIENCE", "Invalid audience",
			"audience must be analyst, customer or regulator")
		return
	}
	lang := explain.NormalizeLanguage(r.URL.Query().Get("lang"))
	if raw := r.URL.Query().Get("lang"); raw != "" && lang != raw && lang != "en" {
		// Unsupported language falls back to the default; the response says which.
		s.deps.Logger.Debug("unsupported explain language", "requested", raw, "used", lang)
	}

	id := chi.URLParam(r, "id")
	rec, err := store.GetDecision(r.Context(), id)
	if err != nil || rec == nil || rec.Decision == nil {
		writeProblem(w, r, http.StatusNotFound, "DECISION_NOT_FOUND", "Decision not found",
			"no decision with that id")
		return
	}
	started := time.Now()
	view, err := s.explainBuilder().Build(rec.Decision, explain.Options{Audience: audience, Language: lang})
	if s.deps.Metrics != nil {
		s.deps.Metrics.ExplainLatency.WithLabelValues(string(audience)).Observe(time.Since(started).Seconds())
	}
	if err != nil {
		s.deps.Logger.Error("explain failed", "decision_id", id, "error", err)
		writeProblem(w, r, http.StatusInternalServerError, "EXPLAIN_FAILED", "Explain failed",
			"the explanation could not be built")
		return
	}
	// Privacy guard: an explanation must never echo a raw identifier.
	if err := redaction.CheckJSON(view); err != nil {
		s.deps.Logger.Error("explain privacy guard tripped", "decision_id", id, "error", err)
		if s.deps.Metrics != nil {
			s.deps.Metrics.Explains.WithLabelValues(string(audience), "privacy_blocked").Inc()
		}
		writeProblem(w, r, http.StatusInternalServerError, "PRIVACY_GUARD", "Blocked",
			"the explanation contained a raw personal identifier and was withheld")
		return
	}
	if s.deps.Metrics != nil {
		s.deps.Metrics.Explains.WithLabelValues(string(audience), "ok").Inc()
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) explainBuilder() *explain.Builder { return explain.NewBuilder() }

// ------------------------------------------------------------------ audit

// ListAudit returns a page of the hash-chained audit log.
func (s *Server) ListAudit(w http.ResponseWriter, r *http.Request) {
	store := s.auditStore()
	if store == nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "AUDIT_UNAVAILABLE", "Audit unavailable",
			"the audit log is not configured")
		return
	}
	from, err := parseSeq(r.URL.Query().Get("from"))
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_SEQ", "Invalid sequence", "from must be a positive integer")
		return
	}
	to, err := parseSeq(r.URL.Query().Get("to"))
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_SEQ", "Invalid sequence", "to must be a positive integer")
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			writeProblem(w, r, http.StatusBadRequest, "INVALID_LIMIT", "Invalid limit", "limit must be 1-1000")
			return
		}
		limit = n
	}
	entries := make([]audit.Entry, 0, limit)
	stop := errors.New("page full")
	err = store.Range(r.Context(), from, to, func(e audit.Entry) error {
		if len(entries) == limit {
			return stop
		}
		entries = append(entries, e)
		return nil
	})
	if err != nil && !errors.Is(err, stop) {
		s.deps.Logger.Error("audit list failed", "error", err)
		writeProblem(w, r, http.StatusInternalServerError, "QUERY_FAILED", "Query failed", "could not read the audit log")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries, "count": len(entries)})
}

// VerifyAudit recomputes the chain over a range and reports the first break.
func (s *Server) VerifyAudit(w http.ResponseWriter, r *http.Request) {
	store := s.auditStore()
	if store == nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "AUDIT_UNAVAILABLE", "Audit unavailable",
			"the audit log is not configured")
		return
	}
	from, err := parseSeq(r.URL.Query().Get("from"))
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_SEQ", "Invalid sequence", "from must be a positive integer")
		return
	}
	to, err := parseSeq(r.URL.Query().Get("to"))
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_SEQ", "Invalid sequence", "to must be a positive integer")
		return
	}
	res, err := audit.VerifyChain(r.Context(), store, audit.VerifyOptions{
		From: from, To: to, CheckpointDir: s.deps.AuditCheckpointDir, HMACKey: s.deps.AuditHMACKey,
	})
	if err != nil {
		s.deps.Logger.Error("audit verify failed", "error", err)
		writeProblem(w, r, http.StatusInternalServerError, "VERIFY_FAILED", "Verify failed", "could not verify the audit log")
		return
	}
	if s.deps.Metrics != nil {
		result := "ok"
		if !res.OK {
			result = "broken"
		}
		s.deps.Metrics.AuditVerify.WithLabelValues(result).Inc()
	}
	code := http.StatusOK
	if !res.OK {
		code = http.StatusConflict
	}
	if err := redaction.CheckJSON(res); err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "PRIVACY_GUARD", "Blocked", "the result was withheld")
		return
	}
	writeJSON(w, code, res)
}

// CheckpointAudit writes a verification checkpoint at the newest entry.
func (s *Server) CheckpointAudit(w http.ResponseWriter, r *http.Request) {
	store := s.auditStore()
	if store == nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "AUDIT_UNAVAILABLE", "Audit unavailable",
			"the audit log is not configured")
		return
	}
	svc := audit.NewCheckpointService(store, s.deps.AuditHMACKey, s.deps.AuditCheckpointDir, audit.Hooks{})
	cp, err := svc.Run(r.Context())
	if errors.Is(err, audit.ErrNoHMACKey) {
		writeProblem(w, r, http.StatusServiceUnavailable, "AUDIT_KEY_MISSING", "Checkpoint unavailable",
			"AUDIT_HMAC_KEY is not configured; checkpoints are never written unsigned")
		return
	}
	if err != nil {
		s.deps.Logger.Error("audit checkpoint failed", "error", err)
		writeProblem(w, r, http.StatusInternalServerError, "CHECKPOINT_FAILED", "Checkpoint failed",
			"could not write an audit checkpoint")
		return
	}
	if s.deps.Metrics != nil {
		s.deps.Metrics.AuditCheckpointTimestamp.Set(float64(time.Now().UTC().Unix()))
	}
	// CheckpointService appends the audit.checkpoint row itself; writing it
	// again here would create a duplicate governance entry.
	writeJSON(w, http.StatusCreated, cp)
}

func (s *Server) auditStore() audit.Store {
	if s.deps.Audit != nil {
		return s.deps.Audit
	}
	if pa, ok := s.deps.Auditor.(*PostgresAuditor); ok {
		return pa.Store()
	}
	return nil
}

func parseSeq(v string) (int64, error) {
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid sequence")
	}
	return n, nil
}
