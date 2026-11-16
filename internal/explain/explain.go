// Package explain builds decision explanations from structured facts.
//
// Nothing in this package runs on the decision path: a decision stores its
// provenance and structured reasons, and explanations are rendered on demand
// for the explain endpoint, the CLI and the audit export.
package explain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// SchemaVersion is the explanation document version. Bumping it is a breaking
// change to the explain contract and requires an OpenAPI change.
const SchemaVersion = 1

// Audience selects how much of the explanation is disclosed.
type Audience string

// Supported audiences.
const (
	AudienceAnalyst   Audience = "analyst"
	AudienceCustomer  Audience = "customer"
	AudienceRegulator Audience = "regulator"
)

// ParseAudience validates the audience query parameter.
func ParseAudience(s string) (Audience, error) {
	switch Audience(strings.ToLower(strings.TrimSpace(s))) {
	case "", AudienceAnalyst:
		return AudienceAnalyst, nil
	case AudienceCustomer:
		return AudienceCustomer, nil
	case AudienceRegulator:
		return AudienceRegulator, nil
	default:
		return "", fmt.Errorf("unknown audience %q", s)
	}
}

// DefaultLanguage is used when the requested language is unsupported.
const DefaultLanguage = "en"

// supportedLanguages lists the languages with a message catalog. rw and fr
// are machine-drafted and flagged needs_review until a native reviewer signs
// off (see docs/explainability.md).
func supportedLanguages() map[string]bool {
	sets, err := LoadTemplateSets()
	out := map[string]bool{DefaultLanguage: true}
	if err != nil {
		return out
	}
	for lang := range sets {
		out[lang] = true
	}
	return out
}

// NormalizeLanguage returns a supported language code.
func NormalizeLanguage(s string) string {
	code := strings.ToLower(strings.TrimSpace(s))
	if code == "" {
		return DefaultLanguage
	}
	if i := strings.IndexAny(code, "-_"); i > 0 {
		code = code[:i]
	}
	if supportedLanguages()[code] {
		return code
	}
	return DefaultLanguage
}

// ReasonEntry is one structured explanation line.
type ReasonEntry struct {
	Source       string  `json:"source"`
	ID           string  `json:"id"`
	Text         string  `json:"text"`
	Weight       int     `json:"weight,omitempty"`
	Mode         string  `json:"mode,omitempty"`
	Feature      string  `json:"feature,omitempty"`
	Value        float64 `json:"value,omitempty"`
	Contribution float64 `json:"contribution,omitempty"`
}

// Factor is one aggregated driver of the risk score.
type Factor struct {
	Name         string  `json:"name"`
	Source       string  `json:"source"`
	Contribution float64 `json:"contribution"`
	Direction    string  `json:"direction"`
	Detail       string  `json:"detail,omitempty"`
}

// Counterfactual is the smallest change to one input that would flip the
// decision. Values are expressed in score points so they stay factual: FADE
// never invents a numeric feature change it cannot verify.
type Counterfactual struct {
	Field             string  `json:"field"`
	Current           float64 `json:"current"`
	Suggested         float64 `json:"suggested"`
	Delta             float64 `json:"delta"`
	ResultingDecision string  `json:"resulting_decision"`
	Explanation       string  `json:"explanation"`
}

// Provenance is the immutable input bundle behind a decision.
type Provenance struct {
	RuleSetVersion int            `json:"rule_set_version"`
	RuleSetHash    string         `json:"rule_set_hash"`
	ModelVersion   string         `json:"model_version,omitempty"`
	SnapshotID     string         `json:"snapshot_id,omitempty"`
	Combiner       map[string]any `json:"combiner,omitempty"`
	SchemaVersion  int            `json:"explain_schema_version"`
}

// FeatureFact is one redacted feature observation.
type FeatureFact struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
}

// View is the explain response contract.
type View struct {
	DecisionID string   `json:"decision_id"`
	EventID    string   `json:"event_id"`
	Decision   string   `json:"decision"`
	RiskScore  int      `json:"risk_score"`
	RuleScore  int      `json:"rule_score"`
	MLScore    *int     `json:"ml_score"`
	Summary    string   `json:"summary"`
	Audience   Audience `json:"audience"`
	Language   string   `json:"language"`
	// LanguageNeedsReview is set for machine-drafted catalogs (fr, rw) so a
	// consumer never presents unreviewed text as officially approved.
	LanguageNeedsReview bool          `json:"language_needs_review,omitempty"`
	Reasons             []ReasonEntry `json:"reasons"`
	// PrimaryFactor names the highest-contribution driver (the first factor).
	PrimaryFactor   string           `json:"primary_factor,omitempty"`
	Factors         []Factor         `json:"factors,omitempty"`
	Counterfactuals []Counterfactual `json:"counterfactuals,omitempty"`
	Provenance      *Provenance      `json:"provenance,omitempty"`
	Features        []FeatureFact    `json:"features,omitempty"`
	DecisionHash    string           `json:"decision_hash"`
	SchemaVersion   int              `json:"explain_schema_version"`
	GeneratedAt     time.Time        `json:"generated_at"`
}

// MarshalJSON enforces the customer-safe contract at the wire level: a customer
// document can never include scores, rule ids, provenance, features or the
// decision hash, even if a future field is added to View.
func (v View) MarshalJSON() ([]byte, error) {
	if v.Audience != AudienceCustomer {
		type alias View
		return json.Marshal(alias(v))
	}
	type customerView struct {
		DecisionID          string        `json:"decision_id"`
		Decision            string        `json:"decision"`
		Summary             string        `json:"summary"`
		Audience            Audience      `json:"audience"`
		Language            string        `json:"language"`
		LanguageNeedsReview bool          `json:"language_needs_review,omitempty"`
		Reasons             []ReasonEntry `json:"reasons"`
		SchemaVersion       int           `json:"explain_schema_version"`
		GeneratedAt         time.Time     `json:"generated_at"`
	}
	return json.Marshal(customerView{
		DecisionID: v.DecisionID, Decision: v.Decision, Summary: v.Summary,
		Audience: v.Audience, Language: v.Language,
		LanguageNeedsReview: v.LanguageNeedsReview, Reasons: v.Reasons,
		SchemaVersion: v.SchemaVersion, GeneratedAt: v.GeneratedAt,
	})
}

// Options controls one rendering.
type Options struct {
	Audience Audience
	Language string
}

// Builder renders explanations. It has no global state, so it is safe for
// concurrent use.
type Builder struct {
	now func() time.Time
}

// NewBuilder returns a builder using the wall clock.
func NewBuilder() *Builder { return &Builder{now: time.Now} }

// NewBuilderWithClock returns a builder with a deterministic clock (tests and
// the reproduction tool).
func NewBuilderWithClock(now func() time.Time) *Builder {
	if now == nil {
		now = time.Now
	}
	return &Builder{now: now}
}

// Build renders an explanation for a decision.
func (b *Builder) Build(d *pipeline.Decision, opts Options) (*View, error) {
	if d == nil {
		return nil, fmt.Errorf("explain: nil decision")
	}
	audience := opts.Audience
	if audience == "" {
		audience = AudienceAnalyst
	}
	lang := NormalizeLanguage(opts.Language)
	// The customer view never exposes the decision hash, so the canonical
	// pre-image is not computed for it at all.
	hash := ""
	if audience != AudienceCustomer {
		var err error
		if hash, err = DecisionHash(d); err != nil {
			return nil, err
		}
	}

	view := &View{
		LanguageNeedsReview: NeedsReview(lang) && lang != DefaultLanguage,
		DecisionID:          d.DecisionID,
		EventID:             d.EventID,
		Decision:            string(d.Decision),
		RiskScore:           d.RiskScore,
		RuleScore:           d.RuleScore,
		MLScore:             d.MLScore,
		Audience:            audience,
		Language:            lang,
		Reasons:             make([]ReasonEntry, 0, len(d.Reasons)),
		DecisionHash:        hash,
		SchemaVersion:       SchemaVersion,
		GeneratedAt:         b.now().UTC(),
	}

	for _, r := range d.Reasons {
		text := r.Text
		if r.Source == "model" && (lang != DefaultLanguage || text == "") {
			if localized, ok := Render("feature."+r.Feature, lang,
				map[string]string{"value": formatFact(r.Value)}); ok {
				text = localized
			}
		} else if audience != AudienceCustomer {
			if localized, ok := Render(r.ID, lang, r.Params); ok {
				text = localized
			}
		}
		view.Reasons = append(view.Reasons, ReasonEntry{
			Source: r.Source, ID: r.ID, Text: text, Weight: r.Weight,
			Mode: string(r.Mode), Feature: r.Feature, Value: r.Value,
			Contribution: r.Contribution,
		})
	}

	prov := provenanceOf(d)
	view.Provenance = prov

	switch audience {
	case AudienceCustomer:
		view.Reasons = customerReasons(d.Reasons, lang)
		view.Summary = customerSummary(lang, d.Decision)
		view.Provenance = nil
		view.Factors = nil
		view.Counterfactuals = nil
		view.Features = nil
		view.DecisionHash = ""
		view.RiskScore, view.RuleScore, view.MLScore = 0, 0, nil
		view.EventID = ""
	case AudienceRegulator:
		view.Summary = analystSummary(d, view.Reasons)
		view.Factors = factorsOf(d, view.Reasons)
		view.Counterfactuals = Counterfactuals(d)
		view.Features = featureFacts(d)
	default:
		view.Summary = analystSummary(d, view.Reasons)
		view.Factors = factorsOf(d, view.Reasons)
		view.Counterfactuals = Counterfactuals(d)
	}
	if len(view.Factors) > 0 {
		view.PrimaryFactor = view.Factors[0].Name
	}
	return view, nil
}

// DecisionHash is the stable digest of the decision facts. Two decisions that
// agree on all of it are indistinguishable to a reviewer.
func DecisionHash(d *pipeline.Decision) (string, error) {
	type fact struct {
		DecisionID     string            `json:"decision_id"`
		EventID        string            `json:"event_id"`
		Decision       string            `json:"decision"`
		RiskScore      int               `json:"risk_score"`
		RuleScore      int               `json:"rule_score"`
		MLScore        *int              `json:"ml_score"`
		MLStatus       string            `json:"ml_status"`
		RuleSetVersion int               `json:"rule_set_version"`
		RuleSetHash    string            `json:"rule_set_hash"`
		SnapshotID     string            `json:"snapshot_id"`
		Reasons        []pipeline.Reason `json:"reasons"`
		ShadowHits     []string          `json:"shadow_hits"`
	}
	f := fact{
		DecisionID: d.DecisionID, EventID: d.EventID, Decision: string(d.Decision),
		RiskScore: d.RiskScore, RuleScore: d.RuleScore, MLScore: d.MLScore,
		MLStatus: d.MLStatus, RuleSetVersion: d.RuleSetVersion,
		RuleSetHash: d.Provenance.RuleSetHash, SnapshotID: d.SnapshotID,
		Reasons: d.Reasons, ShadowHits: d.ShadowHits,
	}
	raw, err := rules.CanonicalJSON(mustJSON(f))
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func mustJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		return []byte("null")
	}
	return raw
}

func provenanceOf(d *pipeline.Decision) *Provenance {
	p := d.Provenance
	if p.ExplainSchemaVersion == 0 {
		p.ExplainSchemaVersion = SchemaVersion
	}
	if p.SnapshotID == "" {
		p.SnapshotID = d.SnapshotID
	}
	if p.RuleSetVersion == 0 {
		p.RuleSetVersion = d.RuleSetVersion
	}
	return &Provenance{
		RuleSetVersion: p.RuleSetVersion,
		RuleSetHash:    p.RuleSetHash,
		ModelVersion:   p.ModelVersion,
		SnapshotID:     p.SnapshotID,
		Combiner:       p.CombinerConfig,
		SchemaVersion:  p.ExplainSchemaVersion,
	}
}

// factorsOf aggregates reasons into drivers ordered by contribution.
func factorsOf(d *pipeline.Decision, reasons []ReasonEntry) []Factor {
	var factors []Factor
	for _, r := range reasons {
		c := r.Contribution
		if r.Source == "rule" {
			c = float64(r.Weight)
		}
		if c == 0 {
			continue
		}
		label := r.Text
		if label == "" {
			label = r.ID
		}
		factors = append(factors, Factor{
			Name: r.ID, Source: r.Source, Contribution: c,
			Direction: directionOf(c), Detail: label,
		})
	}
	sort.SliceStable(factors, func(i, j int) bool {
		a, b := factors[i].Contribution, factors[j].Contribution
		if a < 0 {
			a = -a
		}
		if b < 0 {
			b = -b
		}
		if a != b {
			return a > b
		}
		return factors[i].Name < factors[j].Name
	})
	if len(factors) > 8 {
		factors = factors[:8]
	}
	return factors
}

func directionOf(c float64) string {
	if c > 0 {
		return "increases_risk"
	}
	if c < 0 {
		return "decreases_risk"
	}
	return "neutral"
}

// thresholds returns the decision cut-offs, defaulting to the Phase 4 policy.
func thresholds(d *pipeline.Decision) (stepUp, hold, block int) {
	stepUp, hold, block = 40, 65, 85
	c := d.Provenance.CombinerConfig
	if c == nil {
		return
	}
	if v, ok := toInt(c["step_up_threshold"]); ok && v > 0 {
		stepUp = v
	}
	if v, ok := toInt(c["hold_threshold"]); ok && v > 0 {
		hold = v
	}
	if v, ok := toInt(c["block_threshold"]); ok && v > 0 {
		block = v
	}
	return
}

func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, false
		}
		return int(i), true
	}
	return 0, false
}

// Counterfactuals describes the score movement that would change the decision.
// It is derived from the thresholds and the observed score only.
func Counterfactuals(d *pipeline.Decision) []Counterfactual {
	if d == nil {
		return nil
	}
	stepUp, hold, block := thresholds(d)
	score := float64(d.RiskScore)
	switch d.Decision {
	case rules.Block:
		delta := score - float64(block) + 1
		return []Counterfactual{{
			Field: "risk_score", Current: score, Suggested: score - delta, Delta: -delta,
			ResultingDecision: string(rules.Hold),
			Explanation:       fmt.Sprintf("reduce risk score by %.0f to fall below the block threshold of %d", delta, block),
		}}
	case rules.Hold:
		delta := score - float64(hold) + 1
		return []Counterfactual{{
			Field: "risk_score", Current: score, Suggested: score - delta, Delta: -delta,
			ResultingDecision: string(rules.StepUp),
			Explanation:       fmt.Sprintf("reduce risk score by %.0f to fall below the hold threshold of %d", delta, hold),
		}}
	case rules.StepUp:
		delta := score - float64(stepUp) + 1
		return []Counterfactual{{
			Field: "risk_score", Current: score, Suggested: score - delta, Delta: -delta,
			ResultingDecision: string(rules.Allow),
			Explanation:       fmt.Sprintf("reduce risk score by %.0f to fall below the step-up threshold of %d", delta, stepUp),
		}}
	default:
		delta := float64(stepUp) - score
		return []Counterfactual{{
			Field: "risk_score", Current: score, Suggested: score + delta, Delta: delta,
			ResultingDecision: string(rules.StepUp),
			Explanation:       fmt.Sprintf("increase risk score by %.0f to reach the step-up threshold of %d", delta, stepUp),
		}}
	}
}

// featureFacts returns the non-zero feature observations. Only features are
// disclosed; identifiers are never part of the feature set.
func featureFacts(d *pipeline.Decision) []FeatureFact {
	if d.Features == nil {
		return nil
	}
	raw := mustJSON(d.Features)
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	names := make([]string, 0, len(m))
	for k := range m {
		if k == "event_id" || k == "feature_schema_version" {
			continue
		}
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]FeatureFact, 0, len(names))
	for _, n := range names {
		out = append(out, FeatureFact{Name: n, Value: m[n]})
	}
	return out
}

// customerReasons keeps customer-safe categories only: no rule ids, feature
// names, weights, thresholds or numbers. Each sentence is a fixed catalog entry
// selected by category, never a rewritten analyst sentence.
func customerReasons(in []pipeline.Reason, lang string) []ReasonEntry {
	out := make([]ReasonEntry, 0, 3)
	seen := map[string]bool{}
	for _, r := range in {
		category := customerCategory(r.ID)
		if category == "" || seen[category] {
			continue
		}
		text, ok := RenderCustomer(category, lang)
		if !ok {
			continue
		}
		seen[category] = true
		out = append(out, ReasonEntry{Source: "explain", Text: text})
		if len(out) == 3 {
			break
		}
	}
	if len(out) == 0 {
		if text, ok := RenderCustomer("review", lang); ok {
			out = append(out, ReasonEntry{Source: "explain", Text: text})
		}
	}
	return out
}

// customerCategory maps an internal reason id to the customer-safe sentence
// category. Shadow experiments and model contributions are never disclosed.
func customerCategory(id string) string {
	switch {
	case id == "LIST_BLOCK":
		return "blocked"
	case id == "LIST_ALLOW":
		return "allowed"
	case id == "TIER_LIMIT_EXCEEDED":
		return "limit"
	case strings.HasPrefix(id, "R-ATO"):
		return "account_security"
	case strings.HasPrefix(id, "R-MUL"), strings.HasPrefix(id, "R-AGT"):
		return "recipient_pattern"
	case strings.HasPrefix(id, "R-SOC"):
		return "new_recipient"
	case strings.HasPrefix(id, "R-"):
		return "unusual_activity"
	case id == "ML_VERY_HIGH_CONFIDENCE":
		return "review"
	case strings.HasPrefix(id, "FAIL_POLICY_"):
		return "review"
	default:
		return ""
	}
}

var decisionCustomerCategory = map[rules.Decision]string{
	rules.Allow:  "allowed",
	rules.StepUp: "step_up",
	rules.Hold:   "hold",
	rules.Block:  "blocked",
}

func customerSummary(lang string, d rules.Decision) string {
	category, ok := decisionCustomerCategory[d]
	if !ok {
		category = "review"
	}
	if s, ok := RenderCustomer(category, lang); ok {
		return s
	}
	s, _ := RenderCustomer("review", DefaultLanguage)
	return s
}

// formatFact renders a feature value without exponent surprises.
func formatFact(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func analystSummary(d *pipeline.Decision, reasons []ReasonEntry) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s with risk score %d (rule %d", d.Decision, d.RiskScore, d.RuleScore)
	if d.MLScore != nil {
		fmt.Fprintf(&b, ", ml %d", *d.MLScore)
	} else {
		b.WriteString(", ml n/a")
	}
	b.WriteString(")")
	if len(reasons) > 0 {
		b.WriteString("; driven by ")
		names := make([]string, 0, 3)
		for _, r := range reasons {
			if r.ID == "" {
				continue
			}
			names = append(names, r.ID)
			if len(names) == 3 {
				break
			}
		}
		b.WriteString(strings.Join(names, ", "))
	}
	return b.String()
}
