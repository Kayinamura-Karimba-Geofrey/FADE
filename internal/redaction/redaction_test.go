package redaction

import (
	"strings"
	"testing"
)

func TestScanDetectsIdentifiers(t *testing.T) {
	cases := []struct {
		text string
		kind Kind
	}{
		{"contact geofrey.kayinamura+fade@example.co.rw for details", KindEmail},
		{"call +250 788 123 456 now", KindPhone},
		{"card 4242 4242 4242 4242 charged", KindCard},
		{"nid 1199570012345678 verified", KindNationalID},
		{"iban RW12 3456 7890 1234 5678 90", KindIBAN},
	}
	for _, c := range cases {
		findings := Scan(c.text)
		if len(findings) == 0 {
			t.Fatalf("%s: no finding in %q", c.kind, c.text)
		}
		found := false
		for _, f := range findings {
			if f.Kind == c.kind {
				found = true
			}
			if strings.Contains(f.Masked, c.text[f.Offset:f.Offset+f.Length]) {
				t.Fatalf("%s: mask leaked the raw value", c.kind)
			}
		}
		if !found {
			t.Fatalf("%s: got %+v", c.kind, findings)
		}
	}
}

func TestScanAllowsHashedIdentifiersAndNumbers(t *testing.T) {
	clean := []string{
		"acct_000123",
		"sha256:2e2216e518fd2d51b46e982da90c7b2fa8db797cf77f0009c274b740d2d1f25b",
		"risk score 72 above threshold 65",
		"amount 70000 RWF at 2026-11-16T09:00:00Z",
		"decision HOLD rule R-VEL-001 weight 40",
		"feature velocity_1m=12 amount_sum_10m=70000",
		"",
	}
	for _, s := range clean {
		if f := Scan(s); len(f) != 0 {
			t.Fatalf("false positive in %q: %+v", s, f)
		}
	}
}

func TestRedactMasksEveryFinding(t *testing.T) {
	in := "email a@b.co, phone +250788123456, card 4242424242424242"
	out := Redact(in)
	if strings.Contains(out, "a@b.co") || strings.Contains(out, "250788123456") || strings.Contains(out, "4242424242424242") {
		t.Fatalf("redaction leaked: %q", out)
	}
	if strings.Count(out, Mask) < 3 {
		t.Fatalf("expected three masks: %q", out)
	}
	if Clean(out) == false {
		t.Fatalf("redacted text must be clean: %q", out)
	}
}

func TestCheckJSONFindsLeakingPath(t *testing.T) {
	var payload map[string]any
	payload = map[string]any{
		"decision_id": "dec_1",
		"reasons":     []any{map[string]any{"text": "transfer from 0788123456"}},
	}
	paths, err := ScanJSON(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0] != "reasons[0].text" {
		t.Fatalf("paths=%v", paths)
	}
	if err := CheckJSON(payload); err == nil {
		t.Fatal("CheckJSON must fail")
	}
	if err := CheckJSON(map[string]any{"decision_id": "dec_1", "account": "acct_000123"}); err != nil {
		t.Fatalf("clean payload rejected: %v", err)
	}
}

func TestLuhn(t *testing.T) {
	if !luhn("4242424242424242") {
		t.Fatal("valid card rejected")
	}
	if luhn("4242424242424241") {
		t.Fatal("invalid card accepted")
	}
}
