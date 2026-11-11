package rules

import (
	"strconv"
	"strings"
	"time"
)

// reasonPart is one literal or one identifier substitution.
type reasonPart struct {
	literal string
	field   *Field
}

// ReasonTemplate is a precompiled reason with positional substitutions.
// Rendering happens lazily, only for explainable decisions.
type ReasonTemplate struct {
	parts []reasonPart
}

// compileReason validates placeholders and precompiles the template.
func compileReason(path, text string) (*ReasonTemplate, []ValidationError) {
	if strings.TrimSpace(text) == "" {
		return nil, nil // emptiness is reported by the caller
	}
	var errs []ValidationError
	tmpl := &ReasonTemplate{}
	rest := text
	for {
		loc := reasonToken.FindStringSubmatchIndex(rest)
		if loc == nil {
			break
		}
		if loc[0] > 0 {
			tmpl.parts = append(tmpl.parts, reasonPart{literal: rest[:loc[0]]})
		}
		name := rest[loc[2]:loc[3]]
		field, ok := Fields[name]
		if !ok {
			errs = append(errs, ValidationError{
				Path:    path,
				Message: "placeholder {" + name + "} does not name a known identifier",
			})
		} else {
			f := field
			tmpl.parts = append(tmpl.parts, reasonPart{field: &f})
		}
		rest = rest[loc[1]:]
	}
	if rest != "" {
		tmpl.parts = append(tmpl.parts, reasonPart{literal: rest})
	}
	// Any remaining brace is an unmatched placeholder and is a compile error.
	for _, p := range tmpl.parts {
		if strings.ContainsAny(p.literal, "{}") {
			errs = append(errs, ValidationError{Path: path, Message: "unmatched brace in reason template"})
			break
		}
	}
	return tmpl, errs
}

// Render substitutes the current values. It allocates and must never be called
// on the hot path unless the decision is explainable.
func (t *ReasonTemplate) Render(ctx *EvalContext) string {
	if t == nil {
		return ""
	}
	var b strings.Builder
	b.Grow(64)
	for _, part := range t.parts {
		if part.field == nil {
			b.WriteString(part.literal)
			continue
		}
		switch part.field.Kind {
		case KindInt:
			b.WriteString(strconv.FormatInt(part.field.Int(ctx), 10))
		case KindFloat:
			b.WriteString(strconv.FormatFloat(part.field.Float(ctx), 'f', 2, 64))
		case KindBool:
			if part.field.Bool(ctx) {
				b.WriteString("true")
			} else {
				b.WriteString("false")
			}
		case KindString:
			b.WriteString(part.field.Str(ctx))
		}
	}
	return b.String()
}

// Lit is the raw template text, used by the admin preview.
func (t *ReasonTemplate) Lit() string {
	if t == nil {
		return ""
	}
	var b strings.Builder
	for _, p := range t.parts {
		if p.field == nil {
			b.WriteString(p.literal)
			continue
		}
		b.WriteByte('{')
		b.WriteString(p.field.Name)
		b.WriteByte('}')
	}
	return b.String()
}

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339Nano) }
