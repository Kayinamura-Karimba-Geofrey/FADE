package canonical

import (
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
)

type fixture struct {
	Reference string `json:"reference"`
	Cases     []struct {
		Input    string `json:"input"`
		Expected string `json:"expected"`
	} `json:"cases"`
}

// TestJCSReferenceVectors checks the implementation against 1,000 documents
// canonicalized by the Python rfc8785 package (the reference implementation of
// RFC 8785). Regenerate the fixture with `make canonical-parity`.
func TestJCSReferenceVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/jcs_cases.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var f fixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if len(f.Cases) < 1000 {
		t.Fatalf("fixture has %d cases, want >= 1000", len(f.Cases))
	}
	for i, c := range f.Cases {
		got, err := JSON([]byte(c.Input))
		if err != nil {
			t.Fatalf("case %d input %s: %v", i, c.Input, err)
		}
		if string(got) != c.Expected {
			t.Fatalf("case %d input %s:\n got %s\nwant %s", i, c.Input, got, c.Expected)
		}
	}
}

func TestJCSErrors(t *testing.T) {
	cases := []struct{ name, in string }{
		{"duplicate key", `{"a":1,"a":2}`},
		{"trailing value", `{"a":1}{"b":2}`},
		{"unknown top-level", ``},
		{"nan", `{"n":NaN}`},
	}
	for _, tc := range cases {
		if _, err := JSON([]byte(tc.in)); err == nil {
			t.Fatalf("%s: expected an error", tc.name)
		}
	}
}

func TestNumberECMAScript(t *testing.T) {
	cases := []struct{ in, want string }{
		{"0", "0"},
		{"-0", "0"},
		{"0.0", "0"},
		{"1", "1"},
		{"-1", "-1"},
		{"100000", "100000"},
		{"1000000", "1000000"},
		{"1e6", "1000000"},
		{"1e21", "1e+21"},
		{"1e20", "100000000000000000000"},
		{"1e-6", "0.000001"},
		{"1e-7", "1e-7"},
		{"1.5e-7", "1.5e-7"},
		{"123.456", "123.456"},
		{"9.4", "9.4"},
		{"0.1", "0.1"},
		{"1.0", "1"},
		{"-2.25", "-2.25"},
		{"9007199254740991", "9007199254740991"},
	}
	for _, tc := range cases {
		got, err := Number(tc.in)
		if err != nil {
			t.Fatalf("Number(%s): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("Number(%s)=%s want %s", tc.in, got, tc.want)
		}
	}
	if _, err := Number("Infinity"); err == nil {
		t.Fatal("expected error for Infinity")
	}
}

func TestKeyOrderIsUTF16(t *testing.T) {
	// U+1F600 (surrogate pair D83D DE00) sorts before U+FF01 when compared by
	// UTF-16 code units, but after it when compared by code point.
	got, err := Marshal(map[string]any{"\uFF01": 1, "\U0001F600": 2, "a": 3})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":3,"😀":2,"！":1}`
	if string(got) != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestStringEscapes(t *testing.T) {
	got, err := Marshal(map[string]any{"s": "\x00\x01\b\f\n\r\t\"\\é"})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"s":"\u0000\u0001\b\f\n\r\t\"\\é"}`
	if string(got) != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestSHA256ValueStable(t *testing.T) {
	a, err := SHA256Value(map[string]any{"b": 2, "a": 1})
	if err != nil {
		t.Fatal(err)
	}
	b, err := SHA256Value(map[string]any{"a": 1, "b": 2})
	if err != nil {
		t.Fatal(err)
	}
	if a != b || len(a) != 64 || strings.ToLower(a) != a {
		t.Fatalf("unstable hash %q %q", a, b)
	}
}

func TestFloat32Formatting(t *testing.T) {
	// Float32 values must serialize as their exact decimal, not the widened
	// binary artefact (0.10000000149011612).
	got, err := Marshal(map[string]any{"f": float32(0.1)})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"f":0.1}`
	if string(got) != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func BenchmarkCanonicalize(b *testing.B) {
	doc := []byte(`{"actor":"admin","action":"RULESET_ACTIVATE","object_type":"rule_set","object_id":"prod/7","before":{"version":6},"after":{"version":7,"rules":19},"at":"2026-11-16T00:00:00.000000Z","request_id":"req-1","source_ip_hash":"abc"}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := JSON(doc); err != nil {
			b.Fatal(err)
		}
	}
}

var _ = math.MaxFloat64
