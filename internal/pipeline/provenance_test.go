package pipeline

import (
	"testing"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

func TestSnapshotIDIsContentAddressed(t *testing.T) {
	c := rules.DefaultCombiner()
	a := snapshotIDFor("hash-a", "m1", c)
	if a != snapshotIDFor("hash-a", "m1", c) {
		t.Fatal("snapshot id is not stable")
	}
	if a == snapshotIDFor("hash-b", "m1", c) {
		t.Fatal("snapshot id must change with the rule set")
	}
	if a == snapshotIDFor("hash-a", "m2", c) {
		t.Fatal("snapshot id must change with the model")
	}
}

// BenchmarkProvenanceCapture measures the only cost Phase 5 adds to the decision
// path: hashing the immutable inputs into a snapshot id. The budget is < 1 ms
// p99; this stays in the low microseconds.
func BenchmarkProvenanceCapture(b *testing.B) {
	c := rules.DefaultCombiner()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if snapshotIDFor("a3f1", "model-2026-11-01", c) == "" {
			b.Fatal("empty snapshot id")
		}
	}
}
