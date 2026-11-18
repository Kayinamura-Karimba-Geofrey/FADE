package cases

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// Pseudonymizer renders identifiers for analyst display. Analysts never see a
// raw account, decision or event identifier: the dashboard only receives the
// HMAC of the identifier under a deployment key, truncated for readability.
type Pseudonymizer struct {
	key []byte
}

// NewPseudonymizer builds a pseudonymizer. An empty key is accepted for local
// development but then the pseudonym is a plain digest and reversible only by
// brute force over the small identifier space; production sets FADE_PSEUDONYM_KEY.
func NewPseudonymizer(key string) *Pseudonymizer {
	return &Pseudonymizer{key: []byte(key)}
}

// ID returns a stable, prefixed pseudonym such as "acct_9f2c81a4b7".
func (p *Pseudonymizer) ID(prefix, value string) string {
	if value == "" {
		return ""
	}
	mac := hmac.New(sha256.New, p.key)
	mac.Write([]byte(prefix))
	mac.Write([]byte{0})
	mac.Write([]byte(value))
	sum := hex.EncodeToString(mac.Sum(nil))
	return prefix + "_" + sum[:10]
}

// Truncate shortens an opaque identifier for display without hashing it. Only
// use it for identifiers that are already non-sensitive (case ids, run ids).
func Truncate(value string, keep int) string {
	if keep <= 0 || len(value) <= keep {
		return value
	}
	return value[:keep] + "…"
}

// NormalizeReason maps free text to a stable reason code for label analytics.
func NormalizeReason(s string) string {
	s = strings.TrimSpace(strings.ToUpper(s))
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	if len(s) > 64 {
		s = s[:64]
	}
	return s
}
