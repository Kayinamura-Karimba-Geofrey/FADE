package rules

import (
	"encoding/json"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/canonical"
)

// CanonicalJSON converts a rule-set document into deterministic JSON with
// sorted object keys, no insignificant whitespace and normalized numbers. The
// same document always produces the same content hash, regardless of the
// authoring tool's key order.
func CanonicalJSON(data []byte) ([]byte, error) { return canonical.JSON(data) }

// HashContent returns the hex SHA-256 of canonical JSON.
func HashContent(data []byte) (string, error) { return canonical.SHA256(data) }

// HashRuleSet hashes the validated authoring form.
func HashRuleSet(f RuleSetFile) (string, error) {
	raw, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	return HashContent(raw)
}
