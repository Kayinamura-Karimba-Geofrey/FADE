package config

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

func TestFileConfigAndCombinerOverride(t *testing.T) {
	r := NewResolver(nil, filepath.Join("..", "..", ".."))
	base, err := r.Resolve(context.Background(), "base", Ref{Rules: "file:rules/prod.json"})
	if err != nil {
		t.Fatal(err)
	}
	if base.RuleSet.Hash == "" || base.ConfigHash == "" {
		t.Fatal("missing hashes")
	}
	if base.Combiner.StepUpThreshold != 40 {
		t.Fatalf("threshold = %d", base.Combiner.StepUpThreshold)
	}
	patch := &CombinerPatch{Thresholds: &rules.Thresholds{StepUp: 45, Hold: 70, Block: 88},
		Floors: &rules.Floors{StepUp: ptr(45), Hold: ptr(70), Block: ptr(88)}}
	cand, err := r.Resolve(context.Background(), "cand", Ref{
		Rules: "file:rules/prod.json", Combiner: patch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cand.Combiner.StepUpThreshold != 45 || cand.Combiner.HoldThreshold != 70 {
		t.Fatalf("override = %+v", cand.Combiner)
	}
	if cand.ConfigHash == base.ConfigHash {
		t.Fatal("combiner override did not change the config hash")
	}
	if cand.RuleSet.Combiner.StepUpThreshold != 45 {
		t.Fatal("compiled rule set still uses the old thresholds")
	}
}

func TestPathEscapeAndModelValidation(t *testing.T) {
	r := NewResolver(nil, filepath.Join("..", "..", ".."))
	for _, ref := range []string{"file:../outside.json", "file:/etc/passwd"} {
		if _, err := r.Resolve(context.Background(), "x", Ref{Rules: ref}); err == nil {
			t.Fatalf("accepted %q", ref)
		}
	}
	if _, err := r.Resolve(context.Background(), "x", Ref{Rules: "file:rules/prod.json", Model: "lgbm-nope"}); err == nil {
		t.Fatal("accepted unknown model")
	}
	if _, err := r.Resolve(context.Background(), "x", Ref{Rules: "prod@1"}); err == nil ||
		!strings.Contains(err.Error(), "Postgres") {
		t.Fatalf("prod@1 without Postgres: %v", err)
	}
}

func TestResolveIsDeterministic(t *testing.T) {
	r := NewResolver(nil, filepath.Join("..", "..", ".."))
	a, err := r.Resolve(context.Background(), "a", Ref{Rules: "file:rules/prod.json"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Resolve(context.Background(), "b", Ref{Rules: "file:rules/prod.json"})
	if err != nil {
		t.Fatal(err)
	}
	if a.ConfigHash != b.ConfigHash || a.RuleSet.Hash != b.RuleSet.Hash {
		t.Fatal("same config produced different hashes")
	}
}

func ptr[T any](v T) *T { return &v }
