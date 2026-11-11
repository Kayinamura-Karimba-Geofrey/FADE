package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

type fakeStore struct {
	byID    map[string]*pipeline.StoredDecision
	list    []*pipeline.StoredDecision
	metrics *pipeline.RuleMetricResult
	err     error
}

func (f *fakeStore) WriteDecisions(context.Context, []*pipeline.StoredDecision) error { return f.err }

func (f *fakeStore) GetDecision(_ context.Context, id string) (*pipeline.StoredDecision, error) {
	if f.err != nil {
		return nil, f.err
	}
	if rec, ok := f.byID[id]; ok {
		return rec, nil
	}
	return nil, pipeline.ErrNotFound
}

func (f *fakeStore) ListDecisions(_ context.Context, filter pipeline.DecisionFilter) ([]*pipeline.StoredDecision, string, error) {
	if f.err != nil {
		return nil, "", f.err
	}
	out := f.list
	if filter.AccountID != "" {
		out = nil
		for _, rec := range f.list {
			if rec.AccountID == filter.AccountID {
				out = append(out, rec)
			}
		}
	}
	if filter.Limit > 0 && len(out) > filter.Limit {
		return out[:filter.Limit], "next", nil
	}
	return out, "", nil
}

func (f *fakeStore) DecisionByEventID(context.Context, string) (*pipeline.StoredDecision, error) {
	return nil, pipeline.ErrNotFound
}

func (f *fakeStore) RuleMetrics(context.Context, time.Duration) (*pipeline.RuleMetricResult, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.metrics, nil
}

type failingPinger struct{}

func (failingPinger) Ping(context.Context) error { return errors.New("redis down") }

func stored(id, account string, at time.Time) *pipeline.StoredDecision {
	d := &pipeline.Decision{
		DecisionID: id, EventID: strings.TrimPrefix(id, "dec_"), Decision: rules.Allow,
		RiskScore: 10, RuleScore: 0, MLStatus: rules.MLNotConfigured, RuleSetVersion: 1,
		Reasons: []pipeline.Reason{}, ShadowHits: []string{}, DecidedAt: at,
		Features:  &features.FeatureVector{EventID: "evt"},
		AccountID: account, RecipientID: "acct_000456", Amount: 1000,
		TxnType: "P2P_SEND", EventTime: at,
	}
	return d.ToStored()
}

func TestDecisionHistoryHandlers(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeStore{
		byID: map[string]*pipeline.StoredDecision{
			"dec_one": stored("dec_one", "acct_000123", now),
		},
		list: []*pipeline.StoredDecision{
			stored("dec_one", "acct_000123", now),
			stored("dec_two", "acct_000456", now.Add(-time.Minute)),
		},
		metrics: &pipeline.RuleMetricResult{WindowSeconds: 3600, Decisions: 2,
			Rules: []pipeline.RuleMetric{{RuleID: "R-API-001", Mode: "enforce", Fires: 2, Share: 1}}},
	}
	h, auth := newTestRouterWith(t, nil, store, nil)
	tok := token(t, auth, "service", time.Hour)

	res := do(t, h, http.MethodGet, "/v1/decisions/dec_one", tok, nil, nil)
	if res.Code != http.StatusOK {
		t.Fatalf("get decision status=%d body=%s", res.Code, res.Body)
	}
	var one map[string]any
	if err := json.Unmarshal(res.Body.Bytes(), &one); err != nil {
		t.Fatal(err)
	}
	if one["decision_id"] != "dec_one" || one["features_snapshot"] != nil {
		t.Fatalf("decision=%v", one)
	}

	res = do(t, h, http.MethodGet, "/v1/decisions/dec_one?include=features", tok, nil, nil)
	var withFeatures map[string]any
	if err := json.Unmarshal(res.Body.Bytes(), &withFeatures); err != nil {
		t.Fatal(err)
	}
	if withFeatures["features_snapshot"] == nil {
		t.Fatal("include=features must return the snapshot")
	}

	if res = do(t, h, http.MethodGet, "/v1/decisions/missing", tok, nil, nil); res.Code != http.StatusNotFound {
		t.Fatalf("missing decision status=%d", res.Code)
	}

	res = do(t, h, http.MethodGet, "/v1/decisions?limit=1", tok, nil, nil)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), `"next_cursor":"next"`) {
		t.Fatalf("list status=%d body=%s", res.Code, res.Body)
	}
	for _, query := range []string{"limit=0", "limit=abc", "decision=BOGUS", "from=not-a-time", "to=bad"} {
		if res = do(t, h, http.MethodGet, "/v1/decisions?"+query, tok, nil, nil); res.Code != http.StatusBadRequest {
			t.Fatalf("%s status=%d", query, res.Code)
		}
	}
	res = do(t, h, http.MethodGet, "/v1/metrics/rules?window=2h", tok, nil, nil)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), "R-API-001") {
		t.Fatalf("metrics status=%d body=%s", res.Code, res.Body)
	}
	if res = do(t, h, http.MethodGet, "/v1/metrics/rules?window=forever", tok, nil, nil); res.Code != http.StatusBadRequest {
		t.Fatalf("bad window status=%d", res.Code)
	}

	store.err = errors.New("database down")
	if res = do(t, h, http.MethodGet, "/v1/decisions", tok, nil, nil); res.Code != http.StatusInternalServerError {
		t.Fatalf("list failure status=%d", res.Code)
	}
	if res = do(t, h, http.MethodGet, "/v1/metrics/rules", tok, nil, nil); res.Code != http.StatusInternalServerError {
		t.Fatalf("metrics failure status=%d", res.Code)
	}
}

func TestRuleHandlers(t *testing.T) {
	h, auth := newTestRouter(t, nil)
	admin := token(t, auth, "admin", time.Hour)

	if res := do(t, h, http.MethodGet, "/v1/rules/active", admin, nil, nil); res.Code != http.StatusOK ||
		!strings.Contains(res.Body.String(), "R-API-001") {
		t.Fatalf("active status=%d body=%s", res.Code, res.Body)
	}
	if res := do(t, h, http.MethodGet, "/v1/rules/versions", admin, nil, nil); res.Code != http.StatusOK {
		t.Fatalf("versions status=%d", res.Code)
	}
	if res := do(t, h, http.MethodPost, "/v1/rules/reload", admin, nil, nil); res.Code != http.StatusOK ||
		!strings.Contains(res.Body.String(), `"active":true`) {
		t.Fatalf("reload status=%d body=%s", res.Code, res.Body)
	}

	if res := do(t, h, http.MethodPost, "/v1/rules/validate", admin, []byte(testRules), nil); res.Code != http.StatusOK {
		t.Fatalf("validate status=%d body=%s", res.Code, res.Body)
	}
	if res := do(t, h, http.MethodPost, "/v1/rules/validate", admin, []byte(`{"rule_set":"bad"}`), nil); res.Code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid validate status=%d body=%s", res.Code, res.Body)
	}
	if res := do(t, h, http.MethodPost, "/v1/rules/validate", admin, []byte(`{`), nil); res.Code != http.StatusUnprocessableEntity {
		t.Fatalf("malformed validate status=%d", res.Code)
	}

	rule := `{"id":"R-NEW-001","name":"new","enabled":true,"mode":"enforce","weight":10,
	 "action":"STEP_UP","when":{"f":"amount","op":">","v":10},"reason":"amount {amount}"}`
	if res := do(t, h, http.MethodPut, "/v1/rules/R-NEW-001", admin, []byte(rule), nil); res.Code != http.StatusOK ||
		!strings.Contains(res.Body.String(), "R-NEW-001") {
		t.Fatalf("put rule status=%d body=%s", res.Code, res.Body)
	}
	if res := do(t, h, http.MethodPut, "/v1/rules/R-NEW-001", admin, []byte(`{"id":`), nil); res.Code != http.StatusBadRequest {
		t.Fatalf("bad put status=%d", res.Code)
	}
	if res := do(t, h, http.MethodPut, "/v1/rules/R-NEW-001", admin, []byte(`{"id":"R-NEW-001"}`), nil); res.Code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid rule status=%d body=%s", res.Code, res.Body)
	}
}

func TestListHandlers(t *testing.T) {
	h, auth := newTestRouter(t, nil)
	admin := token(t, auth, "admin", time.Hour)

	if res := do(t, h, http.MethodGet, "/v1/lists", admin, nil, nil); res.Code != http.StatusOK {
		t.Fatalf("empty lists status=%d", res.Code)
	}
	entry := `{"list_type":"BLOCK","subject":"ACCOUNT","value":"acct_000999","reason":"fraud"}`
	res := do(t, h, http.MethodPost, "/v1/lists", admin, []byte(entry), nil)
	if res.Code != http.StatusCreated {
		t.Fatalf("add list status=%d body=%s", res.Code, res.Body)
	}
	var added pipeline.ListEntry
	if err := json.Unmarshal(res.Body.Bytes(), &added); err != nil {
		t.Fatal(err)
	}
	if added.ID == 0 || added.CreatedBy != "tester" {
		t.Fatalf("entry=%+v", added)
	}

	res = do(t, h, http.MethodGet, "/v1/lists?list_type=BLOCK&value=acct_000999", admin, nil, nil)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), "acct_000999") {
		t.Fatalf("filtered lists status=%d body=%s", res.Code, res.Body)
	}

	for _, body := range []string{
		`{"list_type":"BLOCK","subject":"ACCOUNT","value":""}`,
		`{"list_type":"BLOCK","subject":"ACCOUNT","value":"x","expires_at":"2000-01-01T00:00:00Z"}`,
		`{"list_type":"BLOCK","subject":"ACCOUNT","value":"x","bogus":true}`,
	} {
		if res = do(t, h, http.MethodPost, "/v1/lists", admin, []byte(body), nil); res.Code != http.StatusBadRequest {
			t.Fatalf("bad list body %s status=%d", body, res.Code)
		}
	}

	if res = do(t, h, http.MethodDelete, "/v1/lists/"+strconv.FormatInt(added.ID, 10), admin, nil, nil); res.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%s", res.Code, res.Body)
	}
	if res = do(t, h, http.MethodDelete, "/v1/lists/"+strconv.FormatInt(added.ID, 10), admin, nil, nil); res.Code != http.StatusNotFound {
		t.Fatalf("delete twice status=%d", res.Code)
	}
	if res = do(t, h, http.MethodDelete, "/v1/lists/nope", admin, nil, nil); res.Code != http.StatusBadRequest {
		t.Fatalf("bad delete id status=%d", res.Code)
	}
}

func TestOperationalAndBatchErrors(t *testing.T) {
	h, auth := newTestRouterWith(t, nil, nil, []Check{{Name: "redis", Pinger: failingPinger{}}})
	admin := token(t, auth, "admin", time.Hour)

	res := do(t, h, http.MethodGet, "/readyz", "", nil, nil)
	if res.Code != http.StatusServiceUnavailable || !strings.Contains(res.Body.String(), `"failed":"redis"`) {
		t.Fatalf("readyz status=%d body=%s", res.Code, res.Body)
	}

	res = do(t, h, http.MethodOptions, "/v1/decide", "", nil, map[string]string{"Origin": "http://localhost:3000"})
	if res.Code != http.StatusNoContent || res.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Fatalf("preflight status=%d headers=%v", res.Code, res.Header())
	}
	if res = do(t, h, http.MethodPut, "/healthz", "", nil, nil); res.Code != http.StatusMethodNotAllowed {
		t.Fatalf("method not allowed status=%d", res.Code)
	}

	for _, body := range []string{`{}`, `{"events":[]}`, `{"events":[],"bogus":1}`, `{"events":`} {
		if res = do(t, h, http.MethodPost, "/v1/decide/batch", admin, []byte(body), nil); res.Code != http.StatusBadRequest {
			t.Fatalf("batch %s status=%d body=%s", body, res.Code, res.Body)
		}
	}
}
