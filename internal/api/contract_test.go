package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"
)

var (
	specOnce sync.Once
	specDoc  *openapi3.T
	specErr  error
)

func loadSpec(t *testing.T) (*openapi3.T, routers.Router) {
	t.Helper()
	specOnce.Do(func() {
		loader := openapi3.NewLoader()
		loader.IsExternalRefsAllowed = true
		specDoc, specErr = loader.LoadFromFile("../../api/openapi.yaml")
	})
	if specErr != nil {
		t.Fatal(specErr)
	}
	router, err := gorillamux.NewRouter(specDoc)
	if err != nil {
		t.Fatal(err)
	}
	return specDoc, router
}

func validateExchange(t *testing.T, route routers.Router, method, path string, body []byte,
	headers map[string]string, res *httptest.ResponseRecorder) {
	t.Helper()
	req := httptest.NewRequest(method, "http://localhost:8080"+path, bytes.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	found, params, err := route.FindRoute(req)
	if err != nil {
		t.Fatalf("contract route %s %s: %v", method, path, err)
	}
	input := &openapi3filter.RequestValidationInput{
		Request: req, PathParams: params, Route: found,
		Options: &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc},
	}
	if err := openapi3filter.ValidateRequest(context.Background(), input); err != nil {
		t.Fatalf("request violates contract: %v", err)
	}
	resp := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: input, Status: res.Code, Header: res.Header(),
	}
	resp.SetBodyBytes(res.Body.Bytes())
	if err := openapi3filter.ValidateResponse(context.Background(), resp); err != nil {
		t.Fatalf("response %d violates contract: %v\nbody=%s", res.Code, err, res.Body)
	}
}

func TestOpenAPIContractDecide(t *testing.T) {
	doc, router := loadSpec(t)
	if doc.Info.Version != "0.4.0" {
		t.Fatalf("spec version=%q", doc.Info.Version)
	}
	for _, path := range []string{"/v1/decide", "/v1/decide/batch", "/v1/decisions", "/v1/decisions/{id}",
		"/v1/rules/reload", "/v1/rules/active", "/v1/rules/versions", "/v1/rules/{id}", "/v1/rules/validate",
		"/v1/lists", "/v1/lists/{id}", "/v1/metrics/rules", "/healthz", "/readyz", "/version", "/metrics"} {
		if doc.Paths.Find(path) == nil {
			t.Fatalf("spec is missing %s", path)
		}
	}

	h, auth := newTestRouter(t, nil)
	tok := token(t, auth, "service", time.Hour)
	body := eventBody(t, time.Now(), 70000)
	res := do(t, h, http.MethodPost, "/v1/decide", tok, body,
		map[string]string{"Idempotency-Key": "contract-1"})
	if res.Code != http.StatusOK {
		t.Fatalf("decide status=%d", res.Code)
	}
	validateExchange(t, router, http.MethodPost, "/v1/decide", body,
		map[string]string{"Authorization": "Bearer " + tok, "Idempotency-Key": "contract-1",
			"Content-Type": "application/json"}, res)

	unauth := do(t, h, http.MethodPost, "/v1/decide", "", body, nil)
	validateExchange(t, router, http.MethodPost, "/v1/decide", body,
		map[string]string{"Content-Type": "application/json"}, unauth)
}

func TestOpenAPIContractVersionAndProblem(t *testing.T) {
	_, router := loadSpec(t)
	h, _ := newTestRouter(t, nil)
	version := do(t, h, http.MethodGet, "/version", "", nil, nil)
	validateExchange(t, router, http.MethodGet, "/version", nil, nil, version)

	body := eventBody(t, time.Now(), 70000)
	problem := do(t, h, http.MethodPost, "/v1/decide", "", body,
		map[string]string{"Content-Type": "application/json"})
	validateExchange(t, router, http.MethodPost, "/v1/decide", body,
		map[string]string{"Content-Type": "application/json"}, problem)
}
