package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/parquet-go/parquet-go"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/gate"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/report"
)

const maxReplayBody = 1 << 20

func writeJSONError(w http.ResponseWriter, status int, code, detail string) {
	writeJSON(w, status, map[string]any{"code": code, "detail": detail})
}

type replayCandidate struct {
	Label    string          `json:"label"`
	Rules    string          `json:"rules,omitempty"`
	Model    string          `json:"model,omitempty"`
	Combiner json.RawMessage `json:"combiner,omitempty"`
}

type replayCreate struct {
	Name        string            `json:"name"`
	Dataset     string            `json:"dataset"`
	Mode        string            `json:"mode"`
	From        string            `json:"from,omitempty"`
	To          string            `json:"to,omitempty"`
	WarmupDays  int               `json:"warmup_days"`
	StatePolicy string            `json:"state_policy"`
	Seed        int64             `json:"seed"`
	Baseline    replayCandidate   `json:"baseline"`
	Candidates  []replayCandidate `json:"candidates"`
	Options     struct {
		Ablate          bool    `json:"ablate"`
		WithExplain     bool    `json:"with_explain"`
		AllowMLDegraded bool    `json:"allow_ml_degraded"`
		Sample          float64 `json:"sample"`
	} `json:"options"`
}

func (s *Server) replayDB(w http.ResponseWriter) bool {
	if s.deps.DB == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "DB_UNAVAILABLE", "replay storage is not configured")
		return false
	}
	return true
}

func (s *Server) createReplay(w http.ResponseWriter, r *http.Request) {
	if !s.replayDB(w) {
		return
	}
	var req replayCreate
	body := http.MaxBytesReader(w, r.Body, maxReplayBody)
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_JSON", "invalid replay request")
		return
	}
	if req.Dataset == "" || req.Baseline.Rules == "" || len(req.Candidates) == 0 || len(req.Candidates) > 5 {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "dataset, baseline.rules and 1-5 candidates are required")
		return
	}
	if req.Mode == "" {
		req.Mode = "full"
	}
	if req.Mode != "full" && req.Mode != "snapshot" && req.Mode != "stored-ml" {
		writeJSONError(w, http.StatusBadRequest, "INVALID_MODE", "mode must be full, snapshot or stored-ml")
		return
	}
	if req.StatePolicy == "" {
		req.StatePolicy = "ignore"
	}
	if req.StatePolicy != "ignore" {
		writeJSONError(w, http.StatusBadRequest, "UNSUPPORTED_STATE_POLICY", "state policy apply-block is not supported by this engine")
		return
	}
	if req.Seed == 0 {
		req.Seed = 11
	}
	dir := filepath.Join("data", req.Dataset)
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		writeJSONError(w, http.StatusBadRequest, "UNKNOWN_DATASET", "dataset not found")
		return
	}
	raw, err := json.Marshal(req)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "encode request")
		return
	}
	actor := principalSubject(r)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var id int64
	err = s.deps.DB.QueryRow(ctx,
		`INSERT INTO replay_jobs (dataset, baseline, candidate, status, name, created_by, mode,
		   state_policy, seed, engine_version, options)
		 VALUES ($1,$2,$3,'QUEUED',$4,$5,$6,$7,$8,$9,$10) RETURNING id`,
		req.Dataset, mustJSON(req.Baseline), mustJSON(req.Candidates), req.Name, actor,
		req.Mode, req.StatePolicy, req.Seed, s.deps.Version, raw).Scan(&id)
	if err != nil {
		s.deps.Logger.Error("replay enqueue", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL", "could not enqueue job")
		return
	}
	_ = s.deps.Auditor.Record(r.Context(), actor, "REPLAY_START", "replay_job",
		strconv.FormatInt(id, 10), nil, map[string]any{
			"job_id": id, "dataset": req.Dataset, "mode": req.Mode,
			"seed": req.Seed, "status": "QUEUED",
		})
	writeJSON(w, http.StatusAccepted, map[string]any{"job_id": id, "status": "QUEUED"})
}

func (s *Server) listReplay(w http.ResponseWriter, r *http.Request) {
	if !s.replayDB(w) {
		return
	}
	status := r.URL.Query().Get("status")
	dataset := r.URL.Query().Get("dataset")
	limit := parseLimit(r, 50, 200)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rows, err := s.deps.DB.Query(ctx,
		`SELECT id, name, dataset, status, mode, created_by, seed, progress, created_at,
		        COALESCE(started_at, created_at), COALESCE(finished_at, created_at)
		   FROM replay_jobs
		  WHERE ($1 = '' OR status = $1) AND ($2 = '' OR dataset = $2)
		  ORDER BY id DESC LIMIT $3`, status, dataset, limit)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL", "list jobs")
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, seed int64
		var name, ds, st, mode, by string
		var progress float64
		var created, started, finished time.Time
		if err := rows.Scan(&id, &name, &ds, &st, &mode, &by, &seed, &progress, &created, &started, &finished); err != nil {
			writeJSONError(w, http.StatusInternalServerError, "INTERNAL", "scan jobs")
			return
		}
		out = append(out, map[string]any{"job_id": id, "name": name, "dataset": ds,
			"status": st, "mode": mode, "created_by": by, "seed": seed,
			"progress": progress, "created_at": created, "started_at": started, "finished_at": finished})
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

func (s *Server) getReplay(w http.ResponseWriter, r *http.Request) {
	if !s.replayDB(w) {
		return
	}
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_ID", "invalid job id")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var name, ds, st, mode string
	var progress float64
	var errText *string
	var reportPath *string
	err = s.deps.DB.QueryRow(ctx,
		`SELECT name, dataset, status, mode, progress, error, report_path FROM replay_jobs WHERE id=$1`, id).
		Scan(&name, &ds, &st, &mode, &progress, &errText, &reportPath)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
		return
	}
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL", "load job")
		return
	}
	out := map[string]any{"job_id": id, "name": name, "dataset": ds, "status": st,
		"mode": mode, "progress": progress}
	if errText != nil {
		out["error"] = *errText
	}
	rows, err := s.deps.DB.Query(ctx,
		`SELECT config_label, metrics, per_scenario, per_rule, daily, segments, sweep
		   FROM replay_results WHERE job_id=$1 ORDER BY config_label`, id)
	if err == nil {
		defer rows.Close()
		results := []map[string]any{}
		for rows.Next() {
			var label string
			var metricsRaw, scen, rules, daily, seg, sweep []byte
			if rows.Scan(&label, &metricsRaw, &scen, &rules, &daily, &seg, &sweep) == nil {
				results = append(results, map[string]any{"config_label": label,
					"metrics": rawJSON(metricsRaw), "per_scenario": rawJSON(scen),
					"per_rule": rawJSON(rules), "daily": rawJSON(daily),
					"segments": rawJSON(seg), "sweep": rawJSON(sweep)})
			}
		}
		out["results"] = results
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) replayReport(w http.ResponseWriter, r *http.Request) {
	s.serveReplayFile(w, r, "report.json", "application/json")
}

func (s *Server) replayReportMD(w http.ResponseWriter, r *http.Request) {
	s.serveReplayFile(w, r, "report.md", "text/markdown; charset=utf-8")
}

func (s *Server) serveReplayFile(w http.ResponseWriter, r *http.Request, name, contentType string) {
	if !s.replayDB(w) {
		return
	}
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_ID", "invalid job id")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var reportPath *string
	if err := s.deps.DB.QueryRow(ctx, `SELECT report_path FROM replay_jobs WHERE id=$1`, id).Scan(&reportPath); err != nil {
		writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
		return
	}
	path := filepath.Join("replay", "jobs", strconv.FormatInt(id, 10), name)
	if reportPath != nil && *reportPath != "" {
		path = filepath.Join(*reportPath, name)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "NOT_READY", "artifact is not available")
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

type replayDeltaRow struct {
	CandidateLabel  string `parquet:"candidate_label"`
	EventID         string `parquet:"event_id"`
	Label           string `parquet:"label"`
	Scenario        string `parquet:"scenario"`
	Amount          int64  `parquet:"amount"`
	From            string `parquet:"from"`
	To              string `parquet:"to"`
	BaselineReason  string `parquet:"baseline_reason"`
	CandidateReason string `parquet:"candidate_reason"`
}

func (s *Server) replayDeltas(w http.ResponseWriter, r *http.Request) {
	if !s.replayDB(w) {
		return
	}
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_ID", "invalid job id")
		return
	}
	candidate := r.URL.Query().Get("candidate")
	from := r.URL.Query().Get("class_from")
	to := r.URL.Query().Get("class_to")
	cursor := r.URL.Query().Get("cursor")
	limit := parseLimit(r, 100, 500)
	path := filepath.Join("replay", "jobs", strconv.FormatInt(id, 10), "deltas.parquet")
	f, err := os.Open(path)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "NOT_READY", "deltas are not available")
		return
	}
	defer f.Close()
	st, _ := f.Stat()
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL", "open deltas")
		return
	}
	reader := parquet.NewGenericReader[replayDeltaRow](pf)
	defer reader.Close()
	out := []replayDeltaRow{}
	buf := make([]replayDeltaRow, 256)
	afterCursor := cursor == ""
	for {
		n, err := reader.Read(buf)
		for i := 0; i < n; i++ {
			row := buf[i]
			if !afterCursor {
				if row.EventID == cursor {
					afterCursor = true
				}
				continue
			}
			if candidate != "" && row.CandidateLabel != candidate {
				continue
			}
			if from != "" && row.From != from {
				continue
			}
			if to != "" && row.To != to {
				continue
			}
			out = append(out, row)
			if len(out) >= limit {
				writeJSON(w, http.StatusOK, map[string]any{"deltas": out, "next_cursor": row.EventID})
				return
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "INTERNAL", "read deltas")
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"deltas": out, "next_cursor": nil})
}

func (s *Server) replayRules(w http.ResponseWriter, r *http.Request) {
	s.replayJSONColumn(w, r, "per_rule")
}

func (s *Server) replaySweep(w http.ResponseWriter, r *http.Request) {
	s.replayJSONColumn(w, r, "sweep")
}

func (s *Server) replayJSONColumn(w http.ResponseWriter, r *http.Request, column string) {
	if !s.replayDB(w) {
		return
	}
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_ID", "invalid job id")
		return
	}
	candidate := r.URL.Query().Get("candidate")
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var raw []byte
	q := fmt.Sprintf(`SELECT %s FROM replay_results WHERE job_id=$1 AND ($2='' OR config_label=$2) ORDER BY config_label LIMIT 1`, column)
	if err := s.deps.DB.QueryRow(ctx, q, id, candidate).Scan(&raw); err != nil {
		writeJSONError(w, http.StatusNotFound, "NOT_READY", "result is not available")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job_id": id, "candidate": candidate, column: rawJSON(raw)})
}

func (s *Server) cancelReplay(w http.ResponseWriter, r *http.Request) {
	if !s.replayDB(w) {
		return
	}
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_ID", "invalid job id")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	cmd, err := s.deps.DB.Exec(ctx,
		`UPDATE replay_jobs SET status='CANCELLED', cancelled_at=now()
		  WHERE id=$1 AND status IN ('QUEUED','RUNNING')`, id)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL", "cancel job")
		return
	}
	if cmd.RowsAffected() == 0 {
		writeJSONError(w, http.StatusConflict, "NOT_CANCELLABLE", "job is not queued or running")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job_id": id, "status": "CANCELLED"})
}

func (s *Server) gateReplay(w http.ResponseWriter, r *http.Request) {
	if !s.replayDB(w) {
		return
	}
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_ID", "invalid job id")
		return
	}
	var body struct {
		Candidate string `json:"candidate"`
	}
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body)
	if body.Candidate == "" {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "candidate is required")
		return
	}
	path := filepath.Join("replay", "jobs", strconv.FormatInt(id, 10), "report.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "NOT_READY", "report is not available")
		return
	}
	var rep report.Report
	if err := json.Unmarshal(raw, &rep); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL", "decode report")
		return
	}
	policy, err := gate.LoadPolicy(filepath.Join("config", "gate.yaml"))
	if err != nil {
		policy = gate.DefaultPolicy()
	}
	res, err := gate.Evaluate(&rep, body.Candidate, policy)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_CANDIDATE", err.Error())
		return
	}
	if m := s.deps.Metrics; m != nil {
		result := "fail"
		if res.Passed {
			result = "pass"
		}
		m.GateTotal.WithLabelValues(result).Inc()
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	checks, _ := json.Marshal(res.Checks)
	_, err = s.deps.DB.Exec(ctx,
		`INSERT INTO replay_gates (job_id, candidate_label, passed, config_hash, rules_hash,
		    model_version, checks, created_by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		id, body.Candidate, res.Passed, res.ConfigHash, res.RulesHash, res.Model, checks, principalSubject(r))
	if err != nil {
		s.deps.Logger.Error("gate insert", "error", err)
	}
	_ = s.deps.Auditor.Record(r.Context(), principalSubject(r), "REPLAY_PROMOTION_GATE", "replay_gate",
		fmt.Sprintf("%d:%s", id, body.Candidate), nil, res)
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) listDatasets(w http.ResponseWriter, r *http.Request) {
	raw, err := os.ReadFile(filepath.Join("replay", "datasets.json"))
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"datasets": []any{}})
		return
	}
	var doc map[string]any
	if json.Unmarshal(raw, &doc) != nil {
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL", "decode dataset registry")
		return
	}
	writeJSON(w, http.StatusOK, doc)
}

func principalSubject(r *http.Request) string {
	if p := PrincipalFrom(r.Context()); p != nil && p.Subject != "" {
		return p.Subject
	}
	return "anonymous"
}

func mustJSON(v any) json.RawMessage {
	raw, _ := json.Marshal(v)
	return raw
}

func rawJSON(raw []byte) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("null")
	}
	return raw
}

func parseLimit(r *http.Request, def, max int) int {
	n, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

var _ = strings.TrimSpace
