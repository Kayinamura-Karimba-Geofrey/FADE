package rules

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

// AuditRow is one hash-chained audit entry. Hash is computed over the canonical
// JSON of the row excluding Hash itself, prefixed with PrevHash.
type AuditRow struct {
	Actor      string          `json:"actor"`
	Action     string          `json:"action"`
	ObjectType string          `json:"object_type"`
	ObjectID   string          `json:"object_id"`
	Before     json.RawMessage `json:"before,omitempty"`
	After      json.RawMessage `json:"after,omitempty"`
	PrevHash   string          `json:"prev_hash"`
	At         string          `json:"at"`
}

// ChainHash computes sha256(prev_hash || canonical_json(row without hash)).
func ChainHash(prev string, row AuditRow) (string, error) {
	row.PrevHash = prev
	raw, err := json.Marshal(row)
	if err != nil {
		return "", err
	}
	canon, err := CanonicalJSON(raw)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(prev))
	h.Write(canon)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// NewAuditRow builds a row stamped at t.
func NewAuditRow(actor, action, objectType, objectID string, before, after []byte, t time.Time) AuditRow {
	return AuditRow{
		Actor: actor, Action: action, ObjectType: objectType, ObjectID: objectID,
		Before: before, After: after, At: t.UTC().Format(time.RFC3339Nano),
	}
}
