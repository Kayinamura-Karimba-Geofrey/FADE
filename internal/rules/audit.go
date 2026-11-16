package rules

import (
	"encoding/json"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/audit"
)

// AuditRow is the rule-set audit row shape. It is an alias of audit.Row so the
// rule persister and the API write one indistinguishable hash chain.
type AuditRow = audit.Row

// ChainHash computes sha256(prev_hash || canonical_json(row)). Kept for
// compatibility; new code should call audit.ChainHash.
func ChainHash(prev string, row AuditRow) (string, error) { return audit.ChainHash(prev, row) }

// NewAuditRow builds a row stamped at t.
func NewAuditRow(actor, action, objectType, objectID string, before, after []byte, t time.Time) AuditRow {
	return audit.NewRow(actor, action, objectType, objectID, before, after, t)
}

// compile-time assertion that a row marshals with the contract field names.
var _ = json.Marshal
