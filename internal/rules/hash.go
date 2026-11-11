package rules

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// CanonicalJSON converts a rule-set document into deterministic JSON with
// sorted object keys, no insignificant whitespace and normalized numbers. The
// same document always produces the same content hash, regardless of the
// authoring tool's key order.
func CanonicalJSON(data []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("canonicalize: %w", err)
	}
	var buf bytes.Buffer
	if err := writeCanonical(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// HashContent returns the hex SHA-256 of canonical JSON.
func HashContent(data []byte) (string, error) {
	canon, err := CanonicalJSON(data)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), nil
}

// HashRuleSet hashes the validated authoring form.
func HashRuleSet(f RuleSetFile) (string, error) {
	raw, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	return HashContent(raw)
}

func writeCanonical(buf *bytes.Buffer, v any) error {
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
		raw, _ := json.Marshal(n)
		buf.Write(raw)
	case json.Number:
		writeNumber(buf, n)
	case float64:
		writeNumber(buf, json.Number(strconv.FormatFloat(n, 'f', -1, 64)))
	case int:
		buf.WriteString(strconv.Itoa(n))
	case int64:
		buf.WriteString(strconv.FormatInt(n, 10))
	case []any:
		buf.WriteByte('[')
		for i, item := range n {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(n))
		for k := range n {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			raw, _ := json.Marshal(k)
			buf.Write(raw)
			buf.WriteByte(':')
			if err := writeCanonical(buf, n[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return fmt.Errorf("canonicalize: unsupported type %T", v)
	}
	return nil
}

func writeNumber(buf *bytes.Buffer, n json.Number) {
	if i, err := n.Int64(); err == nil {
		buf.WriteString(strconv.FormatInt(i, 10))
		return
	}
	f, err := n.Float64()
	if err != nil {
		buf.WriteString("0")
		return
	}
	buf.WriteString(strconv.FormatFloat(f, 'f', -1, 64))
}
