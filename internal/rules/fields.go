package rules

import (
	"sort"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
)

// Fields is the canonical identifier table for the rule DSL. It must stay in
// sync with the 25 features in docs/features.md plus the six raw identifiers
// documented in docs/rules.md. Compilation resolves names here once; the hot
// path only calls the specialized accessor closures.
var Fields = map[string]Field{
	// ---- 25 Phase 2 features -------------------------------------------------
	"velocity_1m":                {Name: "velocity_1m", Kind: KindInt, Int: func(c *EvalContext) int64 { return c.Vector.Velocity1m }},
	"velocity_10m":               {Name: "velocity_10m", Kind: KindInt, Int: func(c *EvalContext) int64 { return c.Vector.Velocity10m }},
	"velocity_1h":                {Name: "velocity_1h", Kind: KindInt, Int: func(c *EvalContext) int64 { return c.Vector.Velocity1h }},
	"amount_sum_10m":             {Name: "amount_sum_10m", Kind: KindInt, Int: func(c *EvalContext) int64 { return c.Vector.AmountSum10m }},
	"amount_sum_24h":             {Name: "amount_sum_24h", Kind: KindInt, Int: func(c *EvalContext) int64 { return c.Vector.AmountSum24h }},
	"distinct_recip_24h":         {Name: "distinct_recip_24h", Kind: KindInt, Int: func(c *EvalContext) int64 { return c.Vector.DistinctRecip24h }},
	"amount_vs_avg_30d":          {Name: "amount_vs_avg_30d", Kind: KindFloat, Float: func(c *EvalContext) float64 { return c.Vector.AmountVsAvg30d }},
	"amount_zscore_30d":          {Name: "amount_zscore_30d", Kind: KindFloat, Float: func(c *EvalContext) float64 { return c.Vector.AmountZScore30d }},
	"near_limit_ratio":           {Name: "near_limit_ratio", Kind: KindFloat, Float: func(c *EvalContext) float64 { return c.Vector.NearLimitRatio }},
	"small_txn_run_1h":           {Name: "small_txn_run_1h", Kind: KindInt, Int: func(c *EvalContext) int64 { return c.Vector.SmallTxnRun1h }},
	"new_device":                 {Name: "new_device", Kind: KindBool, Bool: func(c *EvalContext) bool { return c.Vector.NewDevice }},
	"new_geo_km":                 {Name: "new_geo_km", Kind: KindFloat, Float: func(c *EvalContext) float64 { return c.Vector.NewGeoKm }},
	"sim_changed_72h":            {Name: "sim_changed_72h", Kind: KindBool, Bool: func(c *EvalContext) bool { return c.Vector.SIMChanged72h }},
	"time_since_sim_change_min":  {Name: "time_since_sim_change_min", Kind: KindInt, Int: func(c *EvalContext) int64 { return c.Vector.TimeSinceSIMChange }},
	"acc_per_device_7d":          {Name: "acc_per_device_7d", Kind: KindInt, Int: func(c *EvalContext) int64 { return c.Vector.AccPerDevice7d }},
	"recip_new_for_sender":       {Name: "recip_new_for_sender", Kind: KindBool, Bool: func(c *EvalContext) bool { return c.Vector.RecipNewForSender }},
	"recip_distinct_senders_24h": {Name: "recip_distinct_senders_24h", Kind: KindInt, Int: func(c *EvalContext) int64 { return c.Vector.RecipDistinctSenders24h }},
	"recip_cashout_lag_min":      {Name: "recip_cashout_lag_min", Kind: KindInt, Int: func(c *EvalContext) int64 { return c.Vector.RecipCashoutLagMin }},
	"inbound_outbound_ratio_1h":  {Name: "inbound_outbound_ratio_1h", Kind: KindFloat, Float: func(c *EvalContext) float64 { return c.Vector.InboundOutboundRatio1h }},
	"night_hour":                 {Name: "night_hour", Kind: KindBool, Bool: func(c *EvalContext) bool { return c.Vector.NightHour }},
	"cycle_len_2_4":              {Name: "cycle_len_2_4", Kind: KindInt, Int: func(c *EvalContext) int64 { return c.Vector.CycleLen2To4 }},
	"agent_cico_ratio_24h":       {Name: "agent_cico_ratio_24h", Kind: KindFloat, Float: func(c *EvalContext) float64 { return c.Vector.AgentCicoRatio24h }},
	"agent_float_delta_pct":      {Name: "agent_float_delta_pct", Kind: KindFloat, Float: func(c *EvalContext) float64 { return c.Vector.AgentFloatDeltaPct }},
	"account_age_days":           {Name: "account_age_days", Kind: KindInt, Int: func(c *EvalContext) int64 { return c.Vector.AccountAgeDays }},
	"kyc_tier":                   {Name: "kyc_tier", Kind: KindInt, Raw: true, Int: func(c *EvalContext) int64 { return c.KYCTier }},

	// ---- raw event identifiers ----------------------------------------------
	"amount":               {Name: "amount", Kind: KindInt, Raw: true, Int: func(c *EvalContext) int64 { return c.Amount }},
	"fee":                  {Name: "fee", Kind: KindInt, Raw: true, Int: func(c *EvalContext) int64 { return c.Fee }},
	"txn_type":             {Name: "txn_type", Kind: KindString, Raw: true, Str: func(c *EvalContext) string { return c.TxnType }},
	"channel":              {Name: "channel", Kind: KindString, Raw: true, Str: func(c *EvalContext) string { return c.Channel }},
	"balance_before_ratio": {Name: "balance_before_ratio", Kind: KindFloat, Raw: true, Float: func(c *EvalContext) float64 { return c.BalanceBeforeRatio() }},
}

// FieldNames returns the identifiers sorted, for docs and error messages.
func FieldNames() []string {
	names := make([]string, 0, len(Fields))
	for name := range Fields {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// EvalContextFor builds the evaluator context for an event. Feature values must
// come from ComputeFeatures; raw fields come from the validated event.
func EvalContextFor(v *features.FeatureVector, amount, fee int64, txnType, channel string, kycTier, balanceBefore int64) EvalContext {
	return EvalContext{
		Vector:        v,
		Amount:        amount,
		Fee:           fee,
		TxnType:       txnType,
		Channel:       channel,
		KYCTier:       kycTier,
		BalanceBefore: balanceBefore,
	}
}
