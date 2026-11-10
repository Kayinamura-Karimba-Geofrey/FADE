package features

import (
	"encoding/json"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/config"
)

// FeatureVector is the Phase 2 output contract. Field order is the canonical
// JSON order: event_id, the 25 features, feature_schema_version.
type FeatureVector struct {
	EventID string `json:"event_id"`

	Velocity1m              int64   `json:"velocity_1m"`
	Velocity10m             int64   `json:"velocity_10m"`
	Velocity1h              int64   `json:"velocity_1h"`
	AmountSum10m            int64   `json:"amount_sum_10m"`
	AmountSum24h            int64   `json:"amount_sum_24h"`
	DistinctRecip24h        int64   `json:"distinct_recip_24h"`
	AmountVsAvg30d          float64 `json:"amount_vs_avg_30d"`
	AmountZScore30d         float64 `json:"amount_zscore_30d"`
	NearLimitRatio          float64 `json:"near_limit_ratio"`
	SmallTxnRun1h           int64   `json:"small_txn_run_1h"`
	NewDevice               bool    `json:"new_device"`
	NewGeoKm                float64 `json:"new_geo_km"`
	SIMChanged72h           bool    `json:"sim_changed_72h"`
	TimeSinceSIMChange      int64   `json:"time_since_sim_change_min"`
	AccPerDevice7d          int64   `json:"acc_per_device_7d"`
	RecipNewForSender       bool    `json:"recip_new_for_sender"`
	RecipDistinctSenders24h int64   `json:"recip_distinct_senders_24h"`
	RecipCashoutLagMin      int64   `json:"recip_cashout_lag_min"`
	InboundOutboundRatio1h  float64 `json:"inbound_outbound_ratio_1h"`
	NightHour               bool    `json:"night_hour"`
	CycleLen2To4            int64   `json:"cycle_len_2_4"`
	AgentCicoRatio24h       float64 `json:"agent_cico_ratio_24h"`
	AgentFloatDeltaPct      float64 `json:"agent_float_delta_pct"`
	AccountAgeDays          int64   `json:"account_age_days"`
	KYCTier                 int64   `json:"kyc_tier"`

	FeatureSchemaVersion string `json:"feature_schema_version"`
}

// Status is the outcome of one event against the feature store.
type Status string

const (
	StatusOK        Status = "ok"
	StatusDuplicate Status = "duplicate"
	StatusLate      Status = "late"
)

// MarshalCanonical returns the canonical JSON without a trailing newline.
// encoding/json preserves struct field order, which is the contract order.
func (v FeatureVector) MarshalCanonical() ([]byte, error) {
	return json.Marshal(v)
}

// NewVector fills the pass-through fields and schema version.
func NewVector(eventID string, accountAgeDays, kycTier int, version string) FeatureVector {
	return FeatureVector{
		EventID:              eventID,
		AmountVsAvg30d:       1.0,
		TimeSinceSIMChange:   -1,
		RecipCashoutLagMin:   -1,
		AccountAgeDays:       int64(accountAgeDays),
		KYCTier:              int64(kycTier),
		FeatureSchemaVersion: version,
	}
}

// SchemaVersion returns the compiled schema version.
func SchemaVersion() string { return config.SchemaVersion }
