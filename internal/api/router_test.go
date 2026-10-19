package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
)

// fakePinger is a controllable Pinger for readiness tests.
type fakePinger struct {
	err   error
	calls int
}

func (f *fakePinger) Ping(context.Context) error {
	f.calls++
	return f.err
}

func newTestRouter(checks ...Check) http.Handler {
	logger := obs.NewLogger("error")
	metrics := obs.NewMetrics("test")
	return NewRouter(logger, metrics, checks...)
}

func TestHealthzAlwaysOK(t *testing.T) {
	router := newTestRouter()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want %d", rec.Code, http.StatusOK)
	}

	var body statusResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "ok" {
		t.Fatalf("healthz body = %q, want ok", body.Status)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}

func TestReadyzAllChecksPass(t *testing.T) {
	pg := &fakePinger{}
	rd := &fakePinger{}
	router := newTestRouter(
		Check{Name: "postgres", Pinger: pg},
		Check{Name: "redis", Pinger: rd},
	)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("readyz status = %d, want %d", rec.Code, http.StatusOK)
	}
	if pg.calls != 1 || rd.calls != 1 {
		t.Fatalf("checks called pg=%d rd=%d, want 1 each", pg.calls, rd.calls)
	}
}

func TestReadyzFailsOnDownDependency(t *testing.T) {
	ok := &fakePinger{}
	bad := &fakePinger{err: errors.New("connection refused")}
	router := newTestRouter(
		Check{Name: "postgres", Pinger: ok},
		Check{Name: "redis", Pinger: bad},
	)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	var body failureResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Failed != "redis" {
		t.Fatalf("failed check = %q, want redis", body.Failed)
	}
}

func TestReadyzNoChecksIsReady(t *testing.T) {
	router := newTestRouter()

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("readyz status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestMetricsEndpointExposed(t *testing.T) {
	router := newTestRouter()

	// Issue one request first so the HTTP counters have data.
	probe := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	probeRec := httptest.NewRecorder()
	router.ServeHTTP(probeRec, probe)
	if probeRec.Code != http.StatusOK {
		t.Fatalf("probe status = %d, want %d", probeRec.Code, http.StatusOK)
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"fade_http_requests_total",
		"fade_http_request_duration_seconds",
		"fade_service_up",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q", want)
		}
	}
}
