package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/events"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

const testRules = `{
 "rule_set":"apitest","version":1,"author":"test",
 "config":{"combiner":{"thresholds":{"step_up":40,"hold":65,"block":85}},
 "fail_mode":{"P2P_SEND":"step_up"}},
 "rules":[
  {"id":"R-API-001","name":"large send","enabled":true,"mode":"enforce",
   "weight":30,"action":"STEP_UP","when":{"f":"amount","op":">","v":100},
   "reason":"amount {amount}"}
 ]}`

type stubFeatures struct{}

func (stubFeatures) Compute(_ context.Context, ev *events.Event) (features.FeatureVector, features.Status, error) {
	return features.FeatureVector{EventID: ev.EventID, FeatureSchemaVersion: "1"}, features.StatusOK, nil
}

func newTestRouter(t *testing.T, limits map[string]RateLimit) (http.Handler, *Auth) {
	return newTestRouterWith(t, limits, nil, nil)
}

func newTestRouterWith(t *testing.T, limits map[string]RateLimit, store pipeline.Store, checks []Check) (http.Handler, *Auth) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "prod.json"), []byte(testRules), 0o644); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	manager := rules.NewManager(dir, logger, nil, rules.Hooks{})
	if _, err := manager.Reload(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	p := pipeline.New(pipeline.Deps{
		Rules: manager, Features: stubFeatures{},
		Lists: pipeline.NewListCache(pipeline.NewMemoryListSource(), logger),
		Idem:  pipeline.NewMemoryIdempotency(), Store: store, Logger: logger,
	})
	auth := NewAuth(AuthConfig{Secret: "test-secret", Issuer: "fade-test"})
	return NewRouter(Deps{
		Logger: logger, Metrics: obs.NewMetrics("api-test"), Pipeline: p,
		Rules: manager, Auth: auth, Limiter: NewRateLimiter(limits), Version: "test",
		Checks: checks, CORSOrigin: "http://localhost:3000",
	}), auth
}

func token(t *testing.T, auth *Auth, role string, ttl time.Duration) string {
	t.Helper()
	tok, err := auth.SignToken("tester", role, ttl)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func ulidAt(ms int64) string {
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	var out [26]byte
	for i := range out {
		out[i] = '0'
	}
	v := uint64(ms)
	for i := 9; i >= 0; i-- {
		out[i] = alphabet[v&31]
		v >>= 5
	}
	return string(out[:])
}

func eventBody(t *testing.T, at time.Time, amount int64) []byte {
	t.Helper()
	at = at.UTC().Truncate(time.Millisecond)
	body := map[string]any{
		"event_id": ulidAt(at.UnixMilli()), "event_time": at.Format("2006-01-02T15:04:05.000Z"),
		"txn_type": "P2P_SEND", "amount": amount, "currency": "RWF",
		"fee": events.FeeForAmount(amount),
		"sender": map[string]any{
			"account_id": "acct_000123", "msisdn_hash": "h_0123456789abcdef0123456789abcdef",
			"device_id": "dev_000123_1", "sim_serial_hash": "sim_0123456789abcdef0123456789abcdef",
			"geo":              map[string]any{"lat": -1.95, "lon": 30.06, "cell_id": "KGL-01"},
			"account_age_days": 120, "kyc_tier": 2, "balance_before": 100000,
		},
		"recipient": map[string]any{
			"account_id": "acct_000456", "msisdn_hash": "h_fedcba9876543210fedcba9876543210",
			"is_agent": false, "first_seen_by_sender": false,
		},
		"channel": "APP", "session_id": "p2p-1234ABCD",
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func do(t *testing.T, h http.Handler, method, path, tok string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestDecideContractAndAuthz(t *testing.T) {
	h, auth := newTestRouter(t, nil)
	now := time.Now()
	body := eventBody(t, now, 70000)

	res := do(t, h, http.MethodPost, "/v1/decide", "", body, nil)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status=%d body=%s", res.Code, res.Body)
	}
	if ct := res.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("problem content type=%q", ct)
	}

	res = do(t, h, http.MethodPost, "/v1/decide", token(t, auth, "analyst", time.Hour), body, nil)
	if res.Code != http.StatusForbidden {
		t.Fatalf("analyst decide status=%d", res.Code)
	}

	res = do(t, h, http.MethodPost, "/v1/decide", token(t, auth, "service", time.Hour)+"x", body, nil)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("tampered token status=%d", res.Code)
	}

	res = do(t, h, http.MethodPost, "/v1/decide", token(t, auth, "service", -time.Minute), body, nil)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expired token status=%d", res.Code)
	}

	res = do(t, h, http.MethodPost, "/v1/decide", token(t, auth, "service", time.Hour), body,
		map[string]string{"Idempotency-Key": "test-key-1"})
	if res.Code != http.StatusOK {
		t.Fatalf("decide status=%d body=%s", res.Code, res.Body)
	}
	var dec pipeline.Decision
	if err := json.Unmarshal(res.Body.Bytes(), &dec); err != nil {
		t.Fatal(err)
	}
	if dec.Decision != rules.StepUp || dec.RiskScore < 40 {
		t.Fatalf("decision=%s risk=%d", dec.Decision, dec.RiskScore)
	}
	if dec.MLStatus != rules.MLNotConfigured || dec.MLScore != nil {
		t.Fatalf("ml=%s/%v", dec.MLStatus, dec.MLScore)
	}
	if !strings.HasPrefix(dec.DecisionID, "dec_") || len(dec.DecisionID) != 30 {
		t.Fatalf("decision id=%q", dec.DecisionID)
	}
	if dec.ShadowHits == nil || dec.Reasons == nil {
		t.Fatal("arrays must be present")
	}
	if got := res.Header().Get("X-Decision-ID"); got != dec.DecisionID {
		t.Fatalf("X-Decision-ID=%q want %q", got, dec.DecisionID)
	}

	// Same key, new payload: RFC 7807 422.
	conflict := eventBody(t, now.Add(time.Second), 80000)
	res = do(t, h, http.MethodPost, "/v1/decide", token(t, auth, "service", time.Hour), conflict,
		map[string]string{"Idempotency-Key": "test-key-1"})
	if res.Code != http.StatusUnprocessableEntity {
		t.Fatalf("conflict status=%d body=%s", res.Code, res.Body)
	}
}

func TestDecideRejectsBadInput(t *testing.T) {
	h, auth := newTestRouter(t, nil)
	tok := token(t, auth, "service", time.Hour)

	cases := []struct {
		name string
		body []byte
		code int
	}{
		{"empty", nil, http.StatusBadRequest},
		{"malformed", []byte(`{"event_id":`), http.StatusBadRequest},
		{"unknown field", []byte(`{"bogus":true}`), http.StatusBadRequest},
		{"oversized", bytes.Repeat([]byte("x"), MaxDecideBody+1), http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := do(t, h, http.MethodPost, "/v1/decide", tok, tc.body, nil)
			if res.Code != tc.code {
				t.Fatalf("status=%d want=%d body=%s", res.Code, tc.code, res.Body)
			}
			var p map[string]any
			if err := json.Unmarshal(res.Body.Bytes(), &p); err != nil {
				t.Fatalf("not JSON: %v", err)
			}
			if p["type"] == nil || p["status"] == nil {
				t.Fatalf("not an RFC 7807 problem: %v", p)
			}
		})
	}

	res := do(t, h, http.MethodPost, "/v1/decide", tok, eventBody(t, time.Now(), 70000),
		map[string]string{"Idempotency-Key": strings.Repeat("x", 129)})
	if res.Code != http.StatusBadRequest {
		t.Fatalf("long idempotency key status=%d", res.Code)
	}
}

func TestRoleMatrixAndRateLimit(t *testing.T) {
	h, auth := newTestRouter(t, nil)
	body := eventBody(t, time.Now(), 500)

	if res := do(t, h, http.MethodGet, "/v1/rules/active", token(t, auth, "service", time.Hour), nil, nil); res.Code != http.StatusOK {
		t.Fatalf("service rules:read status=%d", res.Code)
	}
	if res := do(t, h, http.MethodPost, "/v1/rules/reload", token(t, auth, "service", time.Hour), nil, nil); res.Code != http.StatusForbidden {
		t.Fatalf("service rules:write status=%d", res.Code)
	}
	if res := do(t, h, http.MethodGet, "/v1/decisions", token(t, auth, "analyst", time.Hour), nil, nil); res.Code != http.StatusServiceUnavailable {
		t.Fatalf("analyst decisions:read status=%d (store is intentionally nil)", res.Code)
	}
	if res := do(t, h, http.MethodPost, "/v1/lists", token(t, auth, "analyst", time.Hour), []byte(`{}`), nil); res.Code != http.StatusForbidden {
		t.Fatalf("analyst lists:write status=%d", res.Code)
	}

	limited, limitedAuth := newTestRouter(t, map[string]RateLimit{"service": {PerSecond: 0.001, Burst: 1}})
	first := do(t, limited, http.MethodPost, "/v1/decide", token(t, limitedAuth, "service", time.Hour), body, nil)
	second := do(t, limited, http.MethodPost, "/v1/decide", token(t, limitedAuth, "service", time.Hour), eventBody(t, time.Now().Add(time.Second), 500), nil)
	if first.Code != http.StatusOK || second.Code != http.StatusTooManyRequests {
		t.Fatalf("rate limit first=%d second=%d", first.Code, second.Code)
	}
	if second.Header().Get("Retry-After") != "1" {
		t.Fatalf("missing Retry-After")
	}
}

func TestHealthVersionAndNotFound(t *testing.T) {
	h, _ := newTestRouter(t, nil)
	for _, path := range []string{"/healthz", "/readyz", "/version", "/metrics"} {
		res := do(t, h, http.MethodGet, path, "", nil, nil)
		if res.Code != http.StatusOK {
			t.Fatalf("%s status=%d", path, res.Code)
		}
	}
	var version map[string]any
	if err := json.Unmarshal(do(t, h, http.MethodGet, "/version", "", nil, nil).Body.Bytes(), &version); err != nil {
		t.Fatal(err)
	}
	if version["service"] != "decision-api" || version["rule_set_version"].(float64) != 1 {
		t.Fatalf("version=%v", version)
	}
	res := do(t, h, http.MethodGet, "/nope", "", nil, nil)
	if res.Code != http.StatusNotFound || !strings.Contains(res.Body.String(), "NOT_FOUND") {
		t.Fatalf("not found status=%d body=%s", res.Code, res.Body)
	}
}

func TestBatchValidatesAndDecides(t *testing.T) {
	h, auth := newTestRouter(t, nil)
	tok := token(t, auth, "service", time.Hour)
	now := time.Now()
	batch := map[string]any{"events": []json.RawMessage{
		eventBody(t, now, 70000),
		json.RawMessage(`{"bad":true}`),
	}}
	raw, _ := json.Marshal(batch)
	res := do(t, h, http.MethodPost, "/v1/decide/batch", tok, raw, nil)
	if res.Code != http.StatusOK {
		t.Fatalf("batch status=%d body=%s", res.Code, res.Body)
	}
	var out batchResponse
	if err := json.Unmarshal(res.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Succeeded != 1 || out.Failed != 1 || out.Results[0].Decision == nil || out.Results[1].Error == nil {
		t.Fatalf("batch=%+v", out)
	}
	for _, item := range out.Results {
		if item.Error != nil && item.Error.Status != http.StatusBadRequest {
			t.Fatalf("batch error=$%+v", item.Error)
		}
	}
}
