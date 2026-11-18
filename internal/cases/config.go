package cases

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the analyst-tunable case policy. It mirrors config/cases.yaml.
type Config struct {
	Priority     PriorityWeights `json:"priority" yaml:"priority"`
	SLA          SLAConfig       `json:"sla" yaml:"sla"`
	Grouping     GroupingConfig  `json:"grouping" yaml:"grouping"`
	FourEyes     FourEyesConfig  `json:"four_eyes" yaml:"four_eyes"`
	QASampleRate float64         `json:"qa_sample_rate" yaml:"qa_sample_rate"`
	NoteMaxChars int             `json:"note_max_chars" yaml:"note_max_chars"`
	LabelMature  time.Duration   `json:"-" yaml:"-"`
	LabelMatureH int             `json:"label_mature_hours" yaml:"label_mature_hours"`
	BreachPolicy string          `json:"breach_policy" yaml:"breach_policy"`
}

// GroupingConfig controls how decisions attach to an existing case. A decision
// joins the newest open case for the same account inside the window; the
// account is the grouping key because decisions carry no scenario-run id.
type GroupingConfig struct {
	WindowMinutes int `json:"window_minutes" yaml:"window_minutes"`
}

// FourEyesConfig requires a second reviewer for sensitive resolutions.
type FourEyesConfig struct {
	Enabled         bool  `json:"enabled" yaml:"enabled"`
	AmountThreshold int64 `json:"amount_threshold" yaml:"amount_threshold"`
	BlockOverturn   bool  `json:"block_overturn_requires_approval" yaml:"block_overturn_requires_approval"`
}

// DefaultConfig returns the documented defaults.
func DefaultConfig() Config {
	return Config{
		Priority:     DefaultPriorityWeights(),
		SLA:          DefaultSLAConfig(),
		Grouping:     GroupingConfig{WindowMinutes: 60},
		FourEyes:     FourEyesConfig{Enabled: true, AmountThreshold: 5_000_000, BlockOverturn: true},
		QASampleRate: 0.005,
		NoteMaxChars: 2000,
		LabelMatureH: 7 * 24,
		BreachPolicy: "escalate",
	}
}

// LoadConfig reads config/cases.yaml. A missing file yields defaults rather
// than an error so a deployment without the file still triages cases.
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, err
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// Validate rejects policy values the triage engine cannot honour.
func (c *Config) Validate() error {
	if c.Priority.AmountDivisor <= 0 {
		c.Priority.AmountDivisor = 100000
	}
	if c.Priority.LinkedDecisionCap < 0 || c.Priority.AmountCap < 0 {
		return fmt.Errorf("priority caps must be non-negative")
	}
	if c.SLA.BlockMinutes <= 0 || c.SLA.HoldMinutes <= 0 || c.SLA.QASampleHours <= 0 {
		return fmt.Errorf("sla durations must be positive")
	}
	if c.Grouping.WindowMinutes <= 0 {
		c.Grouping.WindowMinutes = 60
	}
	if c.NoteMaxChars <= 0 || c.NoteMaxChars > 10000 {
		return fmt.Errorf("note_max_chars must be between 1 and 10000")
	}
	if c.QASampleRate < 0 || c.QASampleRate > 1 {
		return fmt.Errorf("qa_sample_rate must be within [0,1]")
	}
	if c.LabelMatureH <= 0 {
		c.LabelMatureH = 7 * 24
	}
	c.LabelMature = time.Duration(c.LabelMatureH) * time.Hour
	switch c.BreachPolicy {
	case "", "escalate", "notify", "none":
		if c.BreachPolicy == "" {
			c.BreachPolicy = "escalate"
		}
	default:
		return fmt.Errorf("breach_policy must be escalate, notify or none")
	}
	return nil
}

// PriorityFor computes the case priority with this policy.
func (c Config) PriorityFor(riskScore int, amount int64, repeat bool, class string, linked int) int {
	return ComputePriority(riskScore, amount, repeat, class, linked, c.Priority)
}

// NeedsFourEyes reports whether a resolution must wait for a second reviewer.
func (c Config) NeedsFourEyes(class string, outcome Outcome, amount int64) bool {
	if !c.FourEyes.Enabled {
		return false
	}
	if c.FourEyes.BlockOverturn && class == "BLOCK" && outcome == OutcomeLegit {
		return true
	}
	return amount > c.FourEyes.AmountThreshold
}
