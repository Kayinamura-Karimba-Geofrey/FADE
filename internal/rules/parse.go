package rules

import (
	"bytes"
	"encoding/json"
)

// ParseFile decodes a rule-set document without compiling it. Unknown fields
// are rejected and numbers are preserved as json.Number for exact validation.
func ParseFile(data []byte) (RuleSetFile, error) {
	var f RuleSetFile
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(&f); err != nil {
		return f, &CompileError{Errors: []ValidationError{{Path: "$", Message: "invalid JSON: " + err.Error()}}}
	}
	if err := ensureEOF(dec); err != nil {
		return f, &CompileError{Errors: []ValidationError{{Path: "$", Message: err.Error()}}}
	}
	return f, nil
}
