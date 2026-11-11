// Package api implements the FADE decision-api HTTP surface. Request and
// response shapes are defined by api/openapi.yaml; Go types are generated into
// internal/api/oapi.
package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/api/oapi"
)

// ProblemTypeBase is the stable URI namespace for RFC 7807 problems.
const ProblemTypeBase = "https://fade.local/problems/"

func writeProblem(w http.ResponseWriter, r *http.Request, status int, code, title, detail string, fields ...oapi.FieldError) {
	p := oapi.Problem{
		Type:   ProblemTypeBase + code,
		Title:  title,
		Status: status,
		Code:   &code,
	}
	if detail != "" {
		p.Detail = &detail
	}
	instance := r.URL.Path
	p.Instance = &instance
	if id := middleware.GetReqID(r.Context()); id != "" {
		p.RequestId = &id
	}
	if len(fields) > 0 {
		p.Errors = &fields
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(p)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
