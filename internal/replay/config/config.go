// Package config resolves replay config references ("prod@14",
// "file:rules/draft.json", "model:lgbm-2026.12.1", combiner overrides) into
// immutable rule sets and hashes.
package config

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/canonical"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// Ref is one authored config. Rules, Model and Combiner are references.
type Ref struct {
	Rules      string          `json:"rules,omitempty"`
	Model      string          `json:"model,omitempty"`
	Combiner   *CombinerPatch  `json:"combiner,omitempty"`
	FailPolicy string          `json:"fail_policy,omitempty"`
	raw        json.RawMessage `json:"-"`
}

// CombinerPatch mirrors rules.CombinerConfig for authored JSON.
type CombinerPatch struct {
	Thresholds *rules.Thresholds `json:"thresholds,omitempty"`
	Floors     *rules.Floors     `json:"floors,omitempty"`
}

// Resolved is the fully materialised config used by the engine.
type Resolved struct {
	Label      string
	Ref        Ref
	RuleSet    *rules.RuleSet
	RuleFile   rules.RuleSetFile
	RulesRef   string
	Model      string
	ModelPath  string
	ModelHash  string
	Combiner   rules.Combiner
	ConfigHash string
}

// Resolver resolves references against Postgres, the filesystem and the model
// registry. It never activates anything.
type Resolver struct {
	PG        *pgxpool.Pool
	Root      string
	ModelRoot string
	Hasher    func([]byte) (string, error)
}

// NewResolver builds a resolver rooted at the repository.
func NewResolver(pg *pgxpool.Pool, root string) *Resolver {
	if root == "" {
		root = "."
	}
	return &Resolver{PG: pg, Root: root, ModelRoot: filepath.Join(root, "ml", "models"), Hasher: canonical.SHA256}
}

// Resolve validates and materialises one config.
func (r *Resolver) Resolve(ctx context.Context, label string, ref Ref) (*Resolved, error) {
	out := &Resolved{Label: label, Ref: ref}
	if ref.Rules == "" && ref.Model == "" && ref.Combiner == nil {
		return nil, fmt.Errorf("config %q: rules, model or combiner required", label)
	}
	if ref.Rules != "" {
		file, hash, err := r.resolveRules(ctx, ref.Rules)
		if err != nil {
			return nil, fmt.Errorf("config %q: %w", label, err)
		}
		out.RuleFile = file
		out.RulesRef = ref.Rules
		out.RuleSet, err = rules.Compile(file)
		if err != nil {
			return nil, fmt.Errorf("config %q: compile rules: %w", label, err)
		}
		_ = hash
	} else {
		// A combiner-only config needs a base rule set. Use the active set.
		file, _, err := r.activeRules(ctx)
		if err != nil {
			return nil, fmt.Errorf("config %q: %w", label, err)
		}
		out.RuleFile = file
		out.RulesRef = "active"
		out.RuleSet, err = rules.Compile(file)
		if err != nil {
			return nil, fmt.Errorf("config %q: compile active rules: %w", label, err)
		}
	}
	if ref.Model != "" && ref.Model != "none" {
		path, hash, err := r.resolveModel(ref.Model)
		if err != nil {
			return nil, fmt.Errorf("config %q: %w", label, err)
		}
		out.Model, out.ModelPath, out.ModelHash = ref.Model, path, hash
	}
	out.Combiner = out.RuleSet.Combiner
	if ref.Combiner != nil {
		out.Combiner = patchCombiner(out.Combiner, ref.Combiner)
	}
	// Recompile with the overridden combiner so the rule set's snapshot and
	// reasons use the same thresholds the run uses.
	if ref.Combiner != nil {
		file := out.RuleFile
		file.Config.Combiner = combinerConfig(out.Combiner, ref.Combiner)
		compiled, err := rules.Compile(file)
		if err != nil {
			return nil, fmt.Errorf("config %q: combiner override: %w", label, err)
		}
		out.RuleSet = compiled
		out.Combiner = compiled.Combiner
	}
	if out.RuleSet == nil || out.RuleSet.Empty {
		return nil, fmt.Errorf("config %q: rule set is empty", label)
	}
	canon := map[string]any{
		"rules":      ref.Rules,
		"rules_hash": out.RuleSet.Hash,
		"model":      out.Model,
		"model_hash": out.ModelHash,
		"combiner":   out.Combiner,
		"fail":       ref.FailPolicy,
	}
	raw, _ := json.Marshal(canon)
	h, err := r.Hasher(raw)
	if err != nil {
		return nil, err
	}
	out.ConfigHash = h
	return out, nil
}

func patchCombiner(base rules.Combiner, p *CombinerPatch) rules.Combiner {
	out := base
	if p.Thresholds != nil {
		if p.Thresholds.StepUp > 0 {
			out.StepUpThreshold = p.Thresholds.StepUp
		}
		if p.Thresholds.Hold > 0 {
			out.HoldThreshold = p.Thresholds.Hold
		}
		if p.Thresholds.Block > 0 {
			out.BlockThreshold = p.Thresholds.Block
		}
	}
	if p.Floors != nil {
		if p.Floors.StepUp != nil {
			out.StepUpFloor = *p.Floors.StepUp
		}
		if p.Floors.Hold != nil {
			out.HoldFloor = *p.Floors.Hold
		}
		if p.Floors.Block != nil {
			out.BlockFloor = *p.Floors.Block
		}
	}
	return out
}

func combinerConfig(c rules.Combiner, p *CombinerPatch) rules.CombinerConfig {
	floor := func(v int) *int { return &v }
	out := rules.CombinerConfig{
		Thresholds: &rules.Thresholds{StepUp: c.StepUpThreshold, Hold: c.HoldThreshold, Block: c.BlockThreshold},
		Floors:     &rules.Floors{StepUp: floor(c.StepUpFloor), Hold: floor(c.HoldFloor), Block: floor(c.BlockFloor)},
	}
	if p != nil && p.Thresholds != nil {
		out.Thresholds = p.Thresholds
	}
	if p != nil && p.Floors != nil {
		out.Floors = p.Floors
	}
	return out
}

func (r *Resolver) resolveRules(ctx context.Context, ref string) (rules.RuleSetFile, string, error) {
	switch {
	case strings.HasPrefix(ref, "prod@"), strings.HasPrefix(ref, "staging@"), strings.Contains(ref, "@"):
		name, version, err := parseVersioned(ref)
		if err != nil {
			return rules.RuleSetFile{}, "", err
		}
		if r.PG == nil {
			return rules.RuleSetFile{}, "", fmt.Errorf("rule reference %q needs Postgres", ref)
		}
		var content []byte
		err = r.PG.QueryRow(ctx,
			`SELECT content FROM rule_sets WHERE name=$1 AND version=$2`, name, version).Scan(&content)
		if err != nil {
			return rules.RuleSetFile{}, "", fmt.Errorf("rule set %s@%d: %w", name, version, err)
		}
		file, err := rules.ParseFile(content)
		if err != nil {
			return rules.RuleSetFile{}, "", fmt.Errorf("rule set %s@%d: %w", name, version, err)
		}
		return file, "", nil
	case strings.HasPrefix(ref, "file:"):
		rel := strings.TrimPrefix(ref, "file:")
		path, err := r.safePath(rel)
		if err != nil {
			return rules.RuleSetFile{}, "", err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return rules.RuleSetFile{}, "", fmt.Errorf("read %s: %w", rel, err)
		}
		file, err := rules.ParseFile(data)
		if err != nil {
			return rules.RuleSetFile{}, "", fmt.Errorf("%s: %w", rel, err)
		}
		hash, err := r.Hasher(data)
		if err != nil {
			return rules.RuleSetFile{}, "", err
		}
		return file, hash, nil
	default:
		return rules.RuleSetFile{}, "", fmt.Errorf("unknown rule reference %q", ref)
	}
}

func (r *Resolver) activeRules(ctx context.Context) (rules.RuleSetFile, string, error) {
	if r.PG == nil {
		return rules.RuleSetFile{}, "", fmt.Errorf("combiner-only config needs Postgres")
	}
	var content []byte
	if err := r.PG.QueryRow(ctx,
		`SELECT content FROM rule_sets WHERE active ORDER BY version DESC LIMIT 1`).Scan(&content); err != nil {
		return rules.RuleSetFile{}, "", fmt.Errorf("active rule set: %w", err)
	}
	file, err := rules.ParseFile(content)
	return file, "", err
}

func parseVersioned(ref string) (string, int, error) {
	at := strings.LastIndex(ref, "@")
	if at <= 0 || at == len(ref)-1 {
		return "", 0, fmt.Errorf("invalid versioned reference %q", ref)
	}
	name := ref[:at]
	var version int
	if _, err := fmt.Sscanf(ref[at+1:], "%d", &version); err != nil || version < 1 {
		return "", 0, fmt.Errorf("invalid version in %q", ref)
	}
	return name, version, nil
}

func (r *Resolver) resolveModel(ref string) (string, string, error) {
	if strings.ContainsAny(ref, "/\\") || ref == "." || ref == ".." {
		return "", "", fmt.Errorf("invalid model version %q", ref)
	}
	path := filepath.Join(r.ModelRoot, ref)
	if _, err := os.Stat(filepath.Join(path, "manifest.json")); err != nil {
		return "", "", fmt.Errorf("model %q is not available: %w", ref, err)
	}
	reg, err := os.ReadFile(filepath.Join(r.ModelRoot, "registry.json"))
	if err != nil {
		return "", "", fmt.Errorf("model registry: %w", err)
	}
	var doc struct {
		Versions map[string]struct {
			ArtifactHash string `json:"artifact_hash"`
			Status       string `json:"status"`
		} `json:"versions"`
	}
	if err := json.Unmarshal(reg, &doc); err != nil {
		return "", "", fmt.Errorf("model registry: %w", err)
	}
	v, ok := doc.Versions[ref]
	if !ok {
		return "", "", fmt.Errorf("model %q is not registered", ref)
	}
	return path, v.ArtifactHash, nil
}

func (r *Resolver) safePath(rel string) (string, error) {
	clean := filepath.Clean(rel)
	if filepath.IsAbs(clean) || strings.HasPrefix(clean, "..") {
		return "", fmt.Errorf("path %q escapes the repository", rel)
	}
	root, err := filepath.Abs(r.Root)
	if err != nil {
		return "", err
	}
	path, err := filepath.Abs(filepath.Join(root, clean))
	if err != nil {
		return "", err
	}
	if path != root && !strings.HasPrefix(path, root+string(os.PathSeparator)) {
		return "", fmt.Errorf("path %q escapes the repository", rel)
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		if resolved != root && !strings.HasPrefix(resolved, root+string(os.PathSeparator)) {
			return "", fmt.Errorf("path %q escapes the repository through a symlink", rel)
		}
	}
	return path, nil
}

// ParseRef decodes a config JSON object.
func ParseRef(raw []byte) (Ref, error) {
	var ref Ref
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ref); err != nil {
		return Ref{}, fmt.Errorf("decode config: %w", err)
	}
	ref.raw = append([]byte(nil), raw...)
	return ref, nil
}
