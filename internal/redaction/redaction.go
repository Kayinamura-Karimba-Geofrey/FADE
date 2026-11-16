// Package redaction detects and masks raw personal identifiers. FADE stores
// only hashed identifiers, so any raw identifier that reaches a payload, a log
// line or a decision reason is a defect. The scanner is used by the API privacy
// guard, the audit exporter and the privscan CLI.
package redaction

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// Kind classifies a finding.
type Kind string

// Identifier kinds.
const (
	KindEmail      Kind = "EMAIL"
	KindPhone      Kind = "PHONE"
	KindCard       Kind = "CARD"
	KindNationalID Kind = "NATIONAL_ID"
	KindIBAN       Kind = "IBAN"
)

// Mask is the replacement token.
const Mask = "[REDACTED]"

// Finding is one detected identifier.
type Finding struct {
	Kind   Kind   `json:"kind"`
	Offset int    `json:"offset"`
	Length int    `json:"length"`
	Masked string `json:"masked"`
}

// String renders a finding without leaking more than the last two characters.
func (f Finding) String() string {
	return fmt.Sprintf("%s at offset %d (%s)", f.Kind, f.Offset, f.Masked)
}

var (
	emailRe = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)
	// International MSISDN, bare local mobile numbers and grouped landlines.
	phoneRe = regexp.MustCompile(`\+\d{1,3}[\s\-]?\d{6,12}|\b0\d{9}\b|\b\d{3,4}[\s\-]\d{3}[\s\-]?\d{3,4}\b`)
	// IBAN: country + check digits + grouped or contiguous account digits.
	ibanRe = regexp.MustCompile(`\b[A-Z]{2}\d{2}(?:\s?[A-Z0-9]{4}){2,7}(?:\s?[A-Z0-9]{1,4})?\b`)
	// 13-19 digit runs, optionally space/dash separated, validated by Luhn.
	cardRe = regexp.MustCompile(`\b(?:\d[ \-]?){12,18}\d\b`)
	// 16-digit national identifiers (Rwanda NID and similar).
	nidRe = regexp.MustCompile(`\b\d{16}\b`)
)

// Scan returns every raw identifier found in text, ordered by offset and
// without overlapping ranges.
func Scan(text string) []Finding {
	var out []Finding
	add := func(kind Kind, loc []int) {
		out = append(out, Finding{Kind: kind, Offset: loc[0], Length: loc[1] - loc[0], Masked: mask(text[loc[0]:loc[1]])})
	}
	for _, loc := range emailRe.FindAllStringIndex(text, -1) {
		add(KindEmail, loc)
	}
	for _, loc := range ibanRe.FindAllStringIndex(text, -1) {
		add(KindIBAN, loc)
	}
	for _, loc := range nidRe.FindAllStringIndex(text, -1) {
		add(KindNationalID, loc)
	}
	for _, loc := range cardRe.FindAllStringIndex(text, -1) {
		if luhn(text[loc[0]:loc[1]]) {
			add(KindCard, loc)
		}
	}
	for _, loc := range phoneRe.FindAllStringIndex(text, -1) {
		n := digits(text[loc[0]:loc[1]])
		if n < 9 || n > 15 {
			continue
		}
		add(KindPhone, loc)
	}
	return dedupe(out)
}

// priority breaks ties when two detectors match the same range: the more
// specific kind wins.
func priority(k Kind) int {
	switch k {
	case KindCard:
		return 5
	case KindIBAN:
		return 4
	case KindEmail:
		return 3
	case KindNationalID:
		return 2
	default:
		return 1
	}
}

// dedupe sorts findings by offset and drops ranges that overlap an earlier,
// longer or higher-priority match.
func dedupe(in []Finding) []Finding {
	sortSlice(in, func(a, b Finding) bool {
		if a.Offset != b.Offset {
			return a.Offset < b.Offset
		}
		if a.Length != b.Length {
			return a.Length > b.Length
		}
		return priority(a.Kind) > priority(b.Kind)
	})
	out := make([]Finding, 0, len(in))
	end := -1
	for _, f := range in {
		if f.Offset < end {
			continue
		}
		out = append(out, f)
		end = f.Offset + f.Length
	}
	return out
}

// Clean reports whether text contains no raw identifiers.
func Clean(text string) bool { return len(Scan(text)) == 0 }

// Redact replaces every detected identifier with Mask.
func Redact(text string) string {
	findings := Scan(text)
	if len(findings) == 0 {
		return text
	}
	var b strings.Builder
	last := 0
	for _, f := range findings {
		if f.Offset < last {
			continue // overlapping match already masked
		}
		b.WriteString(text[last:f.Offset])
		b.WriteString(Mask)
		last = f.Offset + f.Length
	}
	b.WriteString(text[last:])
	return b.String()
}

// ScanJSON scans the string leaves of an arbitrary JSON value and returns the
// JSON path of each finding.
func ScanJSON(v any) ([]string, error) {
	var paths []string
	var walk func(prefix string, node any) error
	walk = func(prefix string, node any) error {
		switch n := node.(type) {
		case string:
			if !Clean(n) {
				paths = append(paths, prefix)
			}
		case []any:
			for i, item := range n {
				if err := walk(fmt.Sprintf("%s[%d]", prefix, i), item); err != nil {
					return err
				}
			}
		case map[string]any:
			keys := make([]string, 0, len(n))
			for k := range n {
				keys = append(keys, k)
			}
			// Deterministic order keeps error messages stable.
			sortStrings(keys)
			for _, k := range keys {
				next := k
				if prefix != "" {
					next = prefix + "." + k
				}
				if err := walk(next, n[k]); err != nil {
					return err
				}
			}
		case json.Number, float64, bool, nil:
		default:
			return fmt.Errorf("unsupported JSON node %T", node)
		}
		return nil
	}
	if err := walk("", v); err != nil {
		return nil, err
	}
	return paths, nil
}

// CheckJSON returns an error naming the first JSON path that leaks an
// identifier.
func CheckJSON(v any) error {
	paths, err := ScanJSON(v)
	if err != nil {
		return err
	}
	if len(paths) > 0 {
		return fmt.Errorf("raw personal identifier in %s", paths[0])
	}
	return nil
}

func mask(raw string) string {
	digitsOnly := make([]byte, 0, 4)
	for i := len(raw) - 1; i >= 0 && len(digitsOnly) < 2; i-- {
		if raw[i] >= '0' && raw[i] <= '9' || raw[i] >= 'a' && raw[i] <= 'z' {
			digitsOnly = append(digitsOnly, raw[i])
		}
	}
	for i, j := 0, len(digitsOnly)-1; i < j; i, j = i+1, j-1 {
		digitsOnly[i], digitsOnly[j] = digitsOnly[j], digitsOnly[i]
	}
	return Mask + string(digitsOnly)
}

func digits(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			n++
		}
	}
	return n
}

// luhn reports whether a digit run satisfies the Luhn checksum.
func luhn(s string) bool {
	sum, alt := 0, false
	valid := false
	for i := len(s) - 1; i >= 0; i-- {
		c := s[i]
		if c < '0' || c > '9' {
			continue
		}
		valid = true
		d := int(c - '0')
		if alt {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		alt = !alt
	}
	return valid && sum%10 == 0
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func sortSlice[T any](s []T, less func(a, b T) bool) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && less(s[j], s[j-1]); j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
