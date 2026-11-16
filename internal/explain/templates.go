package explain

import (
	"embed"
	"fmt"
	"sort"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// Templates are authored message catalogs, one file per language. They are the
// only place user-facing explanation text lives, so every sentence is
// reviewable, translatable and traceable to a structured fact. Nothing is
// generated free-form at request time.
//
//go:embed templates/*.yaml
var templateFS embed.FS

// TemplateSet is one language catalog.
type TemplateSet struct {
	Language    string            `yaml:"language"`
	NeedsReview bool              `yaml:"needs_review"`
	Version     int               `yaml:"version"`
	Templates   map[string]string `yaml:"templates"`
	Customer    map[string]string `yaml:"customer"`
}

var (
	templateOnce sync.Once
	templateSets map[string]*TemplateSet
	templateErr  error
)

// LoadTemplateSets parses every embedded catalog. It is safe for concurrent use
// and parsed once.
func LoadTemplateSets() (map[string]*TemplateSet, error) {
	templateOnce.Do(func() {
		entries, err := templateFS.ReadDir("templates")
		if err != nil {
			templateErr = err
			return
		}
		sets := make(map[string]*TemplateSet, len(entries))
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
				continue
			}
			raw, err := templateFS.ReadFile("templates/" + e.Name())
			if err != nil {
				templateErr = err
				return
			}
			var set TemplateSet
			if err := yaml.Unmarshal(raw, &set); err != nil {
				templateErr = fmt.Errorf("explain: parse %s: %w", e.Name(), err)
				return
			}
			if set.Language == "" {
				set.Language = strings.TrimSuffix(e.Name(), ".yaml")
			}
			if set.Templates == nil || set.Customer == nil {
				templateErr = fmt.Errorf("explain: %s has no template catalog", e.Name())
				return
			}
			sets[set.Language] = &set
		}
		if _, ok := sets[DefaultLanguage]; !ok {
			templateErr = fmt.Errorf("explain: missing %s template catalog", DefaultLanguage)
			return
		}
		templateSets = sets
	})
	return templateSets, templateErr
}

// TemplateSetFor returns the catalog for a language, falling back to English.
func TemplateSetFor(lang string) *TemplateSet {
	sets, err := LoadTemplateSets()
	if err != nil {
		return nil
	}
	if set, ok := sets[lang]; ok {
		return set
	}
	return sets[DefaultLanguage]
}

// NeedsReview reports whether a catalog is still machine-drafted. English is
// authored; fr and rw remain flagged until a native reviewer signs off.
func NeedsReview(lang string) bool {
	set := TemplateSetFor(lang)
	return set != nil && set.NeedsReview
}

// Render fills a template from structured parameters. It reports false when the
// catalog has no entry or a required parameter is absent, so the caller can fall
// back to the stored (already structured) decision text instead of inventing
// content.
func Render(id, lang string, params map[string]string) (string, bool) {
	set := TemplateSetFor(lang)
	if set == nil {
		return "", false
	}
	text, ok := set.Templates[id]
	if !ok {
		if strings.HasPrefix(id, "FAIL_POLICY_") {
			text, ok = set.Templates["FAIL_POLICY"]
			params = withParam(params, "cause", strings.TrimPrefix(id, "FAIL_POLICY_"))
		}
		if !ok {
			return "", false
		}
	}
	out, ok := fillStructured(text, params)
	return out, ok
}

// RenderCustomer returns the customer-safe sentence for a category. Customer
// text is a fixed sentence with no placeholders, so no internal value can leak
// through a template.
func RenderCustomer(category, lang string) (string, bool) {
	set := TemplateSetFor(lang)
	if set == nil {
		return "", false
	}
	text, ok := set.Customer[category]
	return text, ok
}

// fillStructured replaces {name} placeholders. It fails rather than emitting a
// half-rendered sentence when a parameter is missing.
func fillStructured(text string, params map[string]string) (string, bool) {
	var b strings.Builder
	b.Grow(len(text) + 16)
	for i := 0; i < len(text); {
		if text[i] != '{' {
			b.WriteByte(text[i])
			i++
			continue
		}
		end := strings.IndexByte(text[i:], '}')
		if end < 0 {
			return "", false
		}
		name := text[i+1 : i+end]
		value, ok := params[name]
		if !ok {
			return "", false
		}
		b.WriteString(value)
		i += end + 1
	}
	return b.String(), true
}

func withParam(params map[string]string, key, value string) map[string]string {
	out := make(map[string]string, len(params)+1)
	for k, v := range params {
		out[k] = v
	}
	out[key] = value
	return out
}

// RuleIDsWithTemplates returns the rule ids covered by the English catalog,
// sorted, for coverage tests and documentation.
func RuleIDsWithTemplates() []string {
	set := TemplateSetFor(DefaultLanguage)
	if set == nil {
		return nil
	}
	out := make([]string, 0, len(set.Templates))
	for id := range set.Templates {
		if strings.HasPrefix(id, "R-") {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}
