package audit

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/canonical"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/redaction"
)

// payloadAllowlist names the fields that may be persisted in audit before/after
// documents for each object type. Everything else is dropped before hashing, so
// a field added to a rule, model or case by a future phase can never leak into
// the audit log by accident.
var payloadAllowlist = map[string][]string{
	"rule_set": {
		"rule_set", "version", "hash", "previous_version", "author", "changelog",
		"active", "rules", "config", "fail_mode", "source", "compiled_at", "reason",
	},
	"rule": {
		"id", "name", "enabled", "mode", "weight", "action", "tags", "applies_to",
		"stateless", "reason", "when", "changed_fields",
	},
	"combiner": {
		"rule_weight", "ml_weight", "step_up_threshold", "hold_threshold",
		"block_threshold", "step_up_floor", "hold_floor", "block_floor",
		"allow_ml_only_block",
	},
	"model": {
		"version", "artifact_hash", "stage", "previous_stage", "promoted_by",
		"retired_by", "metrics", "reason", "model_version",
	},
	"list_entry": {
		"id", "list_type", "subject", "value", "reason", "created_by",
		"expires_at", "count",
	},
	"case": {
		"id", "decision_id", "status", "priority", "assignee", "previous_assignee",
		"outcome", "notes", "opened_at", "resolved_at", "reason",
	},
	"label": {
		"event_id", "label", "source", "scenario", "previous_label", "count",
	},
	"user": {
		"subject", "role", "previous_role", "reason",
	},
	"decision": {
		"id", "decision", "previous_decision", "risk_score", "rule_score",
		"primary_reason", "reason", "override_id", "case_id",
	},
	"retention": {
		"partition", "table", "data_class", "rows", "from", "to", "export_path",
		"export_sha256", "dry_run", "reason", "months", "partitions",
	},
	"audit_log": {
		"seq", "hash", "prev_hash", "rows", "row_count", "from_seq", "to_seq",
		"checkpoint", "checkpoint_path", "digest", "ok", "first_break", "reason",
	},
	"auth": {
		"source_ip_hash", "failures", "window_seconds", "route", "reason", "threshold",
	},
	"config": {
		"key", "previous_value", "value", "scope", "reason",
	},
	"partition": {
		"partition", "table", "from", "to", "rows", "reason",
	},
}

// KnownObjectType reports whether an object type has a redaction allowlist.
func KnownObjectType(objectType string) bool {
	_, ok := payloadAllowlist[objectType]
	return ok
}

// ObjectTypes returns the allowlisted object types in sorted order.
func ObjectTypes() []string {
	out := make([]string, 0, len(payloadAllowlist))
	for k := range payloadAllowlist {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// AllowedFields returns the persisted fields for an object type.
func AllowedFields(objectType string) []string {
	fields, ok := payloadAllowlist[objectType]
	if !ok {
		return nil
	}
	out := make([]string, len(fields))
	copy(out, fields)
	return out
}

// RedactPayload converts an arbitrary before/after value into the allowlisted
// JSON document that is persisted and hashed. Values of allowlisted fields are
// themselves scanned: raw personal identifiers are masked rather than persisted.
func RedactPayload(objectType string, v any) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	allowed, ok := payloadAllowlist[objectType]
	if !ok {
		return nil, fmt.Errorf("audit: object_type %q has no redaction allowlist", objectType)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("audit: marshal payload: %w", err)
	}
	generic, err := canonicalValue(raw)
	if err != nil {
		return nil, err
	}
	obj, ok := generic.(map[string]any)
	if !ok {
		// A scalar payload is not a governance document; refuse rather than
		// guess which part is safe.
		return nil, fmt.Errorf("audit: %s payload must be a JSON object", objectType)
	}
	set := make(map[string]bool, len(allowed))
	for _, f := range allowed {
		set[f] = true
	}
	out := make(map[string]any, len(allowed))
	for k, val := range obj {
		if !set[k] {
			continue
		}
		out[k] = redactStrings(val)
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

// redactStrings walks a generic JSON value and masks raw identifiers in every
// string, so an allowlisted free-text field cannot carry a phone number into the
// permanent log.
func redactStrings(v any) any {
	switch n := v.(type) {
	case string:
		if redaction.Clean(n) {
			return n
		}
		return redaction.Redact(n)
	case []any:
		out := make([]any, len(n))
		for i, item := range n {
			out[i] = redactStrings(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(n))
		for k, item := range n {
			out[k] = redactStrings(item)
		}
		return out
	default:
		return v
	}
}

func canonicalValue(raw []byte) (any, error) {
	v, err := canonical.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("audit: decode payload: %w", err)
	}
	return v, nil
}
