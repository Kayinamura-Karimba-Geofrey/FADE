// Package canonical implements RFC 8785 JSON Canonicalization Scheme (JCS).
//
// Canonical JSON is the byte-exact pre-image behind every hash in FADE: rule
// set content hashes, audit hash-chain rows and explanation digests. The rules
// are:
//
//   - object keys are sorted by UTF-16 code units (the ordering used by
//     JavaScript's Array.prototype.sort and therefore by JCS);
//   - no insignificant whitespace is emitted;
//   - strings are UTF-8 with the shortest legal escape for control characters,
//     quote and backslash, and every other code point emitted literally;
//   - numbers are serialized with the ECMAScript Number::toString algorithm
//     (integers without an exponent where ECMAScript uses one, and the
//     shortest representation that round-trips through an IEEE-754 double);
//   - duplicate object keys are rejected rather than silently overwritten;
//   - the document must be valid UTF-8.
//
// This package deliberately does not depend on encoding/json's map ordering or
// number formatting; both differ from JCS in observable ways.
package canonical

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// SchemaVersion is the canonicalization contract version. It is recorded in
// audit checkpoints so a future verifier can tell which algorithm produced a
// chain.
const SchemaVersion = 1

// JSON returns the JCS encoding of a JSON document.
func JSON(data []byte) ([]byte, error) {
	v, err := Parse(data)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Grow(len(data))
	if err := write(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Marshal canonicalizes v by encoding it with encoding/json first. Struct field
// order and map iteration order are irrelevant to the result.
func Marshal(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return JSON(raw)
}

// Parse decodes one JSON document into generic values, preserving number text
// and rejecting duplicate object keys. The returned value uses only nil, bool,
// string, json.Number, []any and map[string]any.
func Parse(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := parseValue(dec, "$")
	if err != nil {
		return nil, err
	}
	// Reject trailing content: a canonicalization input is exactly one value.
	if _, err := dec.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("canonicalize: trailing JSON value")
		}
		return nil, fmt.Errorf("canonicalize: %w", err)
	}
	return v, nil
}

func parseValue(dec *json.Decoder, path string) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("canonicalize: %w", err)
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			obj := make(map[string]any)
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, fmt.Errorf("canonicalize: %w", err)
				}
				key, ok := keyTok.(string)
				if !ok {
					return nil, fmt.Errorf("canonicalize: object key at %s is not a string", path)
				}
				if _, dup := obj[key]; dup {
					return nil, fmt.Errorf("canonicalize: duplicate object key %q at %s", key, path)
				}
				val, err := parseValue(dec, path+"."+key)
				if err != nil {
					return nil, err
				}
				obj[key] = val
			}
			if _, err := dec.Token(); err != nil { // closing '}'
				return nil, fmt.Errorf("canonicalize: %w", err)
			}
			return obj, nil
		case '[':
			arr := make([]any, 0, 8)
			for i := 0; dec.More(); i++ {
				val, err := parseValue(dec, fmt.Sprintf("%s[%d]", path, i))
				if err != nil {
					return nil, err
				}
				arr = append(arr, val)
			}
			if _, err := dec.Token(); err != nil { // closing ']'
				return nil, fmt.Errorf("canonicalize: %w", err)
			}
			return arr, nil
		default:
			return nil, fmt.Errorf("canonicalize: unexpected delimiter %q at %s", t, path)
		}
	case string:
		if !utf8.ValidString(t) {
			return nil, fmt.Errorf("canonicalize: invalid UTF-8 string at %s", path)
		}
		return t, nil
	case json.Number:
		return t, nil
	case bool:
		return t, nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("canonicalize: unsupported token %T at %s", tok, path)
	}
}

// SHA256 returns the lowercase hex SHA-256 of the canonical encoding.
func SHA256(data []byte) (string, error) {
	canon, err := JSON(data)
	if err != nil {
		return "", err
	}
	return SHA256Canonical(canon), nil
}

// SHA256Canonical hashes bytes that are already canonical.
func SHA256Canonical(canon []byte) string {
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:])
}

// SHA256Value canonicalizes and hashes v.
func SHA256Value(v any) (string, error) {
	canon, err := Marshal(v)
	if err != nil {
		return "", err
	}
	return SHA256Canonical(canon), nil
}

func write(buf *bytes.Buffer, v any) error {
	switch n := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if n {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case string:
		return writeString(buf, n)
	case json.Number:
		s, err := Number(n.String())
		if err != nil {
			return err
		}
		buf.WriteString(s)
	case float64:
		s, err := Number(strconv.FormatFloat(n, 'g', -1, 64))
		if err != nil {
			return err
		}
		buf.WriteString(s)
	case float32:
		s, err := Number(strconv.FormatFloat(float64(n), 'g', -1, 32))
		if err != nil {
			return err
		}
		buf.WriteString(s)
	case int:
		buf.WriteString(strconv.Itoa(n))
	case int64:
		buf.WriteString(strconv.FormatInt(n, 10))
	case uint64:
		buf.WriteString(strconv.FormatUint(n, 10))
	case []any:
		buf.WriteByte('[')
		for i, item := range n {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := write(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(n))
		for k := range n {
			if !utf8.ValidString(k) {
				return fmt.Errorf("canonicalize: invalid UTF-8 object key")
			}
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return lessUTF16(keys[i], keys[j]) })
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeString(buf, k); err != nil {
				return err
			}
			buf.WriteByte(':')
			if err := write(buf, n[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return fmt.Errorf("canonicalize: unsupported type %T", v)
	}
	return nil
}

// writeString emits a JCS string: shortest escapes for the five named control
// characters, \u00xx for other controls, and literal UTF-8 for everything else.
func writeString(buf *bytes.Buffer, s string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("canonicalize: invalid UTF-8 string")
	}
	buf.WriteByte('"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			i++
			continue
		}
		buf.WriteString(s[start:i])
		switch c {
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		case '\b':
			buf.WriteString(`\b`)
		case '\f':
			buf.WriteString(`\f`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		default:
			const hexDigits = "0123456789abcdef"
			buf.WriteString(`\u00`)
			buf.WriteByte(hexDigits[c>>4])
			buf.WriteByte(hexDigits[c&0x0f])
		}
		i++
		start = i
	}
	buf.WriteString(s[start:])
	buf.WriteByte('"')
	return nil
}

// lessUTF16 compares strings by their UTF-16 code units, the JCS key order.
func lessUTF16(a, b string) bool {
	ar := utf16.Encode([]rune(a))
	br := utf16.Encode([]rune(b))
	for i := 0; i < len(ar) && i < len(br); i++ {
		if ar[i] != br[i] {
			return ar[i] < br[i]
		}
	}
	return len(ar) < len(br)
}

// Number serializes a JSON number literal with the ECMAScript Number::toString
// algorithm. The literal is first parsed as an IEEE-754 double, as required by
// JCS; NaN and infinities are rejected.
func Number(lit string) (string, error) {
	s := strings.TrimSpace(lit)
	if s == "" {
		return "", fmt.Errorf("canonicalize: empty number")
	}
	// Fast path: integers within the exactly-representable range serialize
	// identically under ECMAScript, avoiding a float parse for hot paths.
	if !strings.ContainsAny(s, ".eE") {
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			if i == 0 {
				return "0", nil
			}
			if i > -(1<<53) && i < (1<<53) {
				return strconv.FormatInt(i, 10), nil
			}
		}
		if u, err := strconv.ParseUint(s, 10, 64); err == nil && u < (1<<53) {
			return strconv.FormatUint(u, 10), nil
		}
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return "", fmt.Errorf("canonicalize: invalid number %q", lit)
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", fmt.Errorf("canonicalize: non-finite number %q", lit)
	}
	return formatECMA(f), nil
}

// formatECMA implements ECMAScript Number::toString for a finite double.
func formatECMA(f float64) string {
	if f == 0 {
		return "0"
	}
	if f < 0 {
		return "-" + formatECMA(-f)
	}
	// Shortest round-tripping scientific form: d[.ddd]e±xx.
	sci := strconv.FormatFloat(f, 'e', -1, 64)
	mant, expPart, ok := strings.Cut(sci, "e")
	if !ok {
		// No exponent: already plain digits.
		return sci
	}
	exp, err := strconv.Atoi(expPart)
	if err != nil {
		return sci
	}
	digits := strings.Replace(mant, ".", "", 1)
	digits = strings.TrimRight(digits, "0")
	if digits == "" {
		return "0"
	}
	n := len(digits)
	// k is the position of the decimal point relative to the digit string.
	k := exp + 1

	switch {
	case 0 < k && k <= 21:
		if n <= k {
			return digits + strings.Repeat("0", k-n)
		}
		return digits[:k] + "." + digits[k:]
	case k <= 0 && k > -6:
		return "0." + strings.Repeat("0", -k) + digits
	default:
		// Exponential form.
		e := k - 1
		sign := "+"
		if e < 0 {
			sign = "-"
			e = -e
		}
		if n == 1 {
			return digits + "e" + sign + strconv.Itoa(e)
		}
		return digits[:1] + "." + digits[1:] + "e" + sign + strconv.Itoa(e)
	}
}
