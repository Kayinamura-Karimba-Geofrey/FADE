package cases

import "time"

type SLAConfig struct {
	BlockMinutes        int `json:"block_minutes"`
	HoldMinutes         int `json:"hold_minutes"`
	StepUpFailedMinutes int `json:"step_up_failed_minutes"`
	QASampleHours       int `json:"qa_sample_hours"`
}

func DefaultSLAConfig() SLAConfig {
	return SLAConfig{
		BlockMinutes:        30,
		HoldMinutes:         15,
		StepUpFailedMinutes: 15,
		QASampleHours:       24,
	}
}

func SLADuration(source Source, decisionClass string, cfg SLAConfig) time.Duration {
	switch source {
	case SourceStepUpFailed:
		return time.Duration(cfg.StepUpFailedMinutes) * time.Minute
	case SourceQASample:
		return time.Duration(cfg.QASampleHours) * time.Hour
	}
	if decisionClass == "BLOCK" {
		return time.Duration(cfg.BlockMinutes) * time.Minute
	}
	return time.Duration(cfg.HoldMinutes) * time.Minute
}

func SLADueAt(source Source, decisionClass string, cfg SLAConfig, now time.Time) time.Time {
	return now.Add(SLADuration(source, decisionClass, cfg))
}
