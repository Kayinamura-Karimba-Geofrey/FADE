package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/api/oapi"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// toRule converts the generated OpenAPI rule DTO into the engine's authoring
// form. Required booleans/numbers arrive as values, so they become pointers.
func toRule(in oapi.Rule) rules.Rule {
	enabled := in.Enabled
	weight := in.Weight
	out := rules.Rule{
		ID: in.Id, Name: in.Name, Enabled: &enabled, Mode: rules.Mode(in.Mode),
		Weight: &weight, Action: rules.Decision(in.Action),
		When: toCondition(in.When), Reason: in.Reason,
	}
	if in.Tags != nil {
		out.Tags = append([]string(nil), (*in.Tags)...)
	}
	if in.AppliesTo != nil {
		out.AppliesTo = make([]string, 0, len(*in.AppliesTo))
		for _, t := range *in.AppliesTo {
			out.AppliesTo = append(out.AppliesTo, string(t))
		}
	}
	if in.Stateless != nil {
		out.Stateless = *in.Stateless
	}
	return out
}

func toCondition(in oapi.Condition) rules.Condition {
	out := rules.Condition{}
	if in.F != nil {
		out.F = *in.F
	}
	if in.Op != nil {
		out.Op = string(*in.Op)
	}
	if in.V != nil {
		if raw, err := json.Marshal(*in.V); err == nil {
			out.V = raw
		}
	}
	if in.All != nil {
		out.All = make([]rules.Condition, 0, len(*in.All))
		for _, c := range *in.All {
			out.All = append(out.All, toCondition(c))
		}
	}
	if in.Any != nil {
		out.Any = make([]rules.Condition, 0, len(*in.Any))
		for _, c := range *in.Any {
			out.Any = append(out.Any, toCondition(c))
		}
	}
	if in.Not != nil {
		child := toCondition(*in.Not)
		out.Not = &child
	}
	return out
}

// toListEntry converts the generated list input.
func toListEntry(in oapi.ListEntryInput, actor string) pipeline.ListEntry {
	e := pipeline.ListEntry{
		ListType: string(in.ListType), Subject: string(in.Subject),
		Value: in.Value, CreatedBy: actor, ExpiresAt: in.ExpiresAt,
	}
	if in.Reason != nil {
		e.Reason = *in.Reason
	}
	return e
}

// problemValue builds an RFC 7807 body without writing it (batch items).
func problemValue(r *http.Request, status int, code, title, detail string) *oapi.Problem {
	p := &oapi.Problem{Type: ProblemTypeBase + code, Title: title, Status: status, Code: &code}
	if detail != "" {
		p.Detail = &detail
	}
	if r != nil {
		inst := r.URL.Path
		p.Instance = &inst
		if id := middleware.GetReqID(r.Context()); id != "" {
			p.RequestId = &id
		}
	}
	return p
}
