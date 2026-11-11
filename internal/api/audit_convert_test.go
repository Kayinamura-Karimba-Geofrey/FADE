package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/api/oapi"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
)

func TestDisabledAuthAndConstantTimeEqual(t *testing.T) {
	auth := NewAuth(AuthConfig{})
	if auth.Enabled() {
		t.Fatal("empty secret must disable auth")
	}
	var principal *Principal
	h := auth.Authenticate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal = PrincipalFrom(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusNoContent || principal == nil || principal.Role != "admin" || !principal.HasScope("rules:write") {
		t.Fatalf("disabled auth principal=%+v code=%d", principal, rec.Code)
	}
	if !ConstantTimeEqual("secret", "secret") || ConstantTimeEqual("secret", "other") {
		t.Fatal("ConstantTimeEqual mismatch")
	}
}

func TestNoopAndNilAuditors(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	if err := (NoopAuditor{Logger: logger}).Record(context.Background(), "a", "b", "c", "d", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := (NoopAuditor{}).Record(context.Background(), "a", "b", "c", "d", 1, 2); err != nil {
		t.Fatal(err)
	}
	var nilAuditor *PostgresAuditor
	if err := nilAuditor.Record(context.Background(), "a", "b", "c", "d", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := (&PostgresAuditor{}).Record(context.Background(), "a", "b", "c", "d", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := marshalOrNil(make(chan int)); err == nil {
		t.Fatal("unsupported JSON value must fail")
	}
}

func TestRuleConversionRoundTrip(t *testing.T) {
	f := "amount"
	op := oapi.ConditionOpGreaterThan
	var v any = 100.0
	inner := oapi.Condition{F: &f, Op: &op, V: &v}
	not := oapi.Condition{Not: &inner}
	all := []oapi.Condition{{F: &f, Op: &op, V: &v}, not}
	tags := []string{"velocity"}
	applies := []oapi.RuleAppliesTo{"P2P_SEND"}
	stateless := true
	in := oapi.Rule{
		Id: "R-CNV-001", Name: "convert", Enabled: true, Mode: oapi.RuleModeEnforce,
		Weight: 20, Action: oapi.RuleAction("STEP_UP"), When: oapi.Condition{All: &all},
		Reason: "amount {amount}", Tags: &tags, AppliesTo: &applies, Stateless: &stateless,
	}
	out := toRule(in)
	if out.ID != in.Id || !*out.Enabled || *out.Weight != 20 || out.Reason != in.Reason ||
		len(out.Tags) != 1 || out.AppliesTo[0] != "P2P_SEND" || !out.Stateless {
		t.Fatalf("rule=%+v", out)
	}
	if out.When.All == nil || len(out.When.All) != 2 || out.When.All[1].Not == nil {
		t.Fatalf("condition=%+v", out.When)
	}
	if string(out.When.All[0].V) != "100" {
		t.Fatalf("value=%s", out.When.All[0].V)
	}

	reason := "review"
	entry := toListEntry(oapi.ListEntryInput{
		ListType: oapi.ListEntryInputListType("BLOCK"), Subject: oapi.ListEntryInputSubject("ACCOUNT"),
		Value: "acct_000001", Reason: &reason,
	}, "actor")
	if entry.ListType != "BLOCK" || entry.Subject != "ACCOUNT" || entry.CreatedBy != "actor" || entry.Reason != "review" {
		t.Fatalf("entry=%+v", entry)
	}

	req := httptest.NewRequest("POST", "/v1/decide", nil)
	p := problemValue(req, 400, "BAD", "Bad", "detail")
	if p.Type == "" || p.Instance == nil || *p.Code != "BAD" || *p.Detail != "detail" {
		t.Fatalf("problem=%+v", p)
	}
	if p := problemValue(nil, 500, "ERR", "Err", ""); p.Instance != nil || p.Detail != nil {
		t.Fatalf("nil request problem=%+v", p)
	}
}

type errorAuditor struct{ err error }

func (a errorAuditor) Record(context.Context, string, string, string, string, any, any) error {
	return a.err
}

func TestPipelineErrorMappingAndAuditFailures(t *testing.T) {
	s := &Server{deps: Deps{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{errors.New("plain"), http.StatusInternalServerError, "INTERNAL"},
		{&pipeline.Error{Kind: pipeline.KindShed, Status: http.StatusServiceUnavailable, Message: "shedding", Err: errors.New("full")}, http.StatusServiceUnavailable, pipeline.KindShed},
		{&pipeline.Error{Kind: pipeline.KindIdemConflict, Status: http.StatusUnprocessableEntity, Message: "conflict"}, http.StatusUnprocessableEntity, "IDEMPOTENCY_CONFLICT"},
		{&pipeline.Error{Kind: pipeline.KindIdempotencyInFlight, Status: http.StatusConflict, Message: "in flight"}, http.StatusConflict, pipeline.KindIdempotencyInFlight},
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		s.writePipelineError(rec, httptest.NewRequest(http.MethodPost, "/v1/decide", nil), tc.err)
		if rec.Code != tc.status {
			t.Fatalf("err=%v status=%d want=%d", tc.err, rec.Code, tc.status)
		}
		if rec.Body.Len() == 0 || rec.Header().Get("Content-Type") != "application/problem+json" {
			t.Fatalf("err=%v headers=%v body=%s", tc.err, rec.Header(), rec.Body.String())
		}
	}

	rec := httptest.NewRecorder()
	retry := &pipeline.Error{Kind: pipeline.KindShed, Status: http.StatusServiceUnavailable, Message: "shedding"}
	s.writePipelineError(rec, httptest.NewRequest(http.MethodPost, "/v1/decide", nil), retry)
	if rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After=%q", rec.Header().Get("Retry-After"))
	}

	s.deps.Auditor = errorAuditor{err: errors.New("audit down")}
	s.audit(httptest.NewRequest(http.MethodPost, "/v1/rules", nil), "rules.put", "rule_set", "1", nil, nil)
	s.deps.Auditor = nil
	s.audit(httptest.NewRequest(http.MethodPost, "/v1/rules", nil), "rules.put", "rule_set", "1", nil, nil)
}
