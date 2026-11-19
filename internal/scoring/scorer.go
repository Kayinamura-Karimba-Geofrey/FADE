// Package scoring implements the FADE ML scorer client used by the decision
// pipeline. It calls the Python scorer over HTTP with a 15 ms budget, never
// retries, and protects the hot path with a circuit breaker.
package scoring

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// FeatureNames is the frozen order shared with ml/config.py and features.json.
var FeatureNames = []string{
	"velocity_1m", "velocity_10m", "velocity_1h", "amount_sum_10m",
	"amount_sum_24h", "distinct_recip_24h", "amount_vs_avg_30d",
	"amount_zscore_30d", "near_limit_ratio", "small_txn_run_1h", "new_device",
	"new_geo_km", "sim_changed_72h", "time_since_sim_change_min",
	"acc_per_device_7d", "recip_new_for_sender", "recip_distinct_senders_24h",
	"recip_cashout_lag_min", "inbound_outbound_ratio_1h", "night_hour",
	"cycle_len_2_4", "agent_cico_ratio_24h", "agent_float_delta_pct",
	"account_age_days", "kyc_tier", "txn_type", "channel",
}

// Contribution is one SHAP entry returned by the scorer.
type Contribution struct {
	Feature   string  `json:"feature"`
	Value     float64 `json:"value"`
	SHAP      float64 `json:"shap"`
	Direction string  `json:"direction"`
}

// ScoreRequest is the scorer input.
type ScoreRequest struct {
	Features map[string]any `json:"features"`
	Explain  bool           `json:"explain"`
}

// ScoreResponse is the scorer output.
type ScoreResponse struct {
	ModelVersion     string         `json:"model_version"`
	FraudProb        float64        `json:"fraud_prob"`
	AnomalyScore     float64        `json:"anomaly_score"`
	MLScore          int            `json:"ml_score"`
	Status           string         `json:"status"`
	LatencyMS        float64        `json:"latency_ms"`
	TopContributions []Contribution `json:"top_contributions,omitempty"`
	ExplainSkipped   bool           `json:"explain_skipped"`
}

// Config configures the client.
type Config struct {
	BaseURL              string
	Timeout              time.Duration
	BreakerThreshold     int
	BreakerOpenFor       time.Duration
	ExpectedModelVersion string
	// BatchSize > 1 enables micro-batching of concurrent single-row calls; the
	// window bounds the added latency.
	BatchSize   int
	BatchWindow time.Duration
	Metrics     *obs.Metrics
	Now         func() time.Time
}

// Client is an HTTP scorer with a circuit breaker.
type Client struct {
	baseURL              string
	http                 *http.Client
	timeout              time.Duration
	expectedModelVersion string
	now                  func() time.Time

	mu            sync.Mutex
	consecutive   int
	threshold     int
	openUntil     time.Time
	openFor       time.Duration
	halfOpen      bool
	probeInFlight bool

	// rolling samples over 10s
	samples      []sample
	sampleWindow time.Duration

	metrics *obs.Metrics

	batch microBatcherClient
}

type sample struct {
	at      time.Time
	latency time.Duration
	ok      bool
}

// New creates a client.
func New(cfg Config) *Client {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Millisecond
	}
	if cfg.BreakerThreshold <= 0 {
		cfg.BreakerThreshold = 5
	}
	if cfg.BreakerOpenFor <= 0 {
		cfg.BreakerOpenFor = 5 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	tr := &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	c := &Client{
		baseURL:              cfg.BaseURL,
		http:                 &http.Client{Transport: tr, Timeout: cfg.Timeout},
		timeout:              cfg.Timeout,
		expectedModelVersion: cfg.ExpectedModelVersion,
		now:                  cfg.Now,
		threshold:            cfg.BreakerThreshold,
		openFor:              cfg.BreakerOpenFor,
		sampleWindow:         10 * time.Second,
		metrics:              cfg.Metrics,
	}
	if c.metrics != nil {
		c.metrics.MLBreakerState.Set(0)
	}
	if cfg.BatchSize > 1 {
		c.batch = newMicroBatcher(c, cfg.BatchSize, cfg.BatchWindow, NewBatchClient(cfg.BaseURL, cfg.Timeout))
	}
	return c
}

// Available reports whether the client is configured.
func (c *Client) Available() bool { return c != nil && c.baseURL != "" }

// State returns breaker state string.
func (c *Client) State() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.openUntil.IsZero() && c.now().Before(c.openUntil) {
		if c.halfOpen {
			return "half-open"
		}
		return "open"
	}
	return "closed"
}

func (c *Client) allow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if c.openUntil.IsZero() {
		return true
	}
	if now.After(c.openUntil) {
		// half-open: allow single probe
		if c.probeInFlight {
			return false
		}
		c.halfOpen = true
		c.probeInFlight = true
		if c.metrics != nil {
			c.metrics.MLBreakerState.Set(2)
		}
		return true
	}
	return false
}

func (c *Client) record(ok bool, latency time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	// rolling samples
	c.samples = append(c.samples, sample{at: now, latency: latency, ok: ok})
	cutoff := now.Add(-c.sampleWindow)
	kept := c.samples[:0]
	for _, s := range c.samples {
		if s.at.After(cutoff) {
			kept = append(kept, s)
		}
	}
	c.samples = kept

	if ok {
		c.consecutive = 0
		if c.halfOpen {
			// success => closed
			c.openUntil = time.Time{}
			c.halfOpen = false
			c.probeInFlight = false
			c.samples = nil
			if c.metrics != nil {
				c.metrics.MLBreakerState.Set(0)
				c.metrics.MLBreakerTransitions.Inc()
			}
		}
		// check rolling conditions only when closed
		if c.openUntil.IsZero() {
			// maybe p95 / error ratio
			if tripped, reason := c.rollingTrippedLocked(); tripped {
				c.tripLocked(reason)
			}
		}
		return
	}

	c.consecutive++
	if !c.halfOpen && c.consecutive >= c.threshold {
		c.tripLocked("consecutive")
		return
	}
	if c.halfOpen {
		// probe failed => reopen
		c.openUntil = now.Add(c.openFor)
		c.halfOpen = false
		c.probeInFlight = false
		if c.metrics != nil {
			c.metrics.MLBreakerState.Set(1)
			c.metrics.MLBreakerTransitions.Inc()
		}
		return
	}
	// rolling conditions
	if tripped, _ := c.rollingTrippedLocked(); tripped {
		c.tripLocked("rolling")
	}
}

func (c *Client) rollingTrippedLocked() (bool, string) {
	if len(c.samples) < 50 {
		return false, ""
	}
	// error ratio
	errs := 0
	for _, s := range c.samples {
		if !s.ok {
			errs++
		}
	}
	if float64(errs)/float64(len(c.samples)) > 0.20 {
		return true, "error_ratio"
	}
	// p95 latency
	lats := make([]time.Duration, 0, len(c.samples))
	for _, s := range c.samples {
		lats = append(lats, s.latency)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	idx := int(0.95 * float64(len(lats)))
	if idx >= len(lats) {
		idx = len(lats) - 1
	}
	if lats[idx] > 30*time.Millisecond {
		return true, "p95"
	}
	return false, ""
}

func (c *Client) tripLocked(reason string) {
	c.openUntil = c.now().Add(c.openFor)
	c.consecutive = 0
	if c.metrics != nil {
		c.metrics.MLBreakerState.Set(1)
		c.metrics.MLBreakerTransitions.Inc()
	}
}

func (c *Client) Score(ctx context.Context, fv *features.FeatureVector) (rules.MLResult, error) {
	return c.ScoreResult(ctx, fv)
}

func (c *Client) Explain(ctx context.Context, fv *features.FeatureVector) ([]rules.Contribution, error) {
	res, err := c.call(ctx, fv, true)
	if err != nil {
		return nil, err
	}
	out := make([]rules.Contribution, len(res.TopContributions))
	for i, cc := range res.TopContributions {
		out[i] = rules.Contribution{Feature: cc.Feature, Value: cc.Value, SHAP: cc.SHAP, Direction: cc.Direction}
	}
	return out, nil
}

func (c *Client) call(ctx context.Context, fv *features.FeatureVector, explain bool) (ScoreResponse, error) {
	var zero ScoreResponse
	if !c.Available() {
		return zero, errors.New("scorer not configured")
	}
	if !c.allow() {
		return zero, errors.New("circuit breaker open")
	}
	cctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	start := c.now()
	body := ScoreRequest{Features: featureMap(fv), Explain: explain}
	raw, err := json.Marshal(body)
	if err != nil {
		c.record(false, c.now().Sub(start))
		return zero, err
	}
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, c.baseURL+"/score", bytes.NewReader(raw))
	if err != nil {
		c.record(false, c.now().Sub(start))
		return zero, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	lat := c.now().Sub(start)
	if err != nil {
		c.record(false, lat)
		return zero, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.record(false, lat)
		return zero, fmt.Errorf("scorer status %d", resp.StatusCode)
	}
	var out ScoreResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		c.record(false, lat)
		return zero, err
	}
	c.record(true, lat)
	return out, nil
}

func (c *Client) ScoreResult(ctx context.Context, fv *features.FeatureVector) (rules.MLResult, error) {
	if c.batch != nil {
		return c.batch.score(ctx, fv)
	}
	resp, err := c.call(ctx, fv, false)
	if err != nil {
		return rules.MLResult{Status: rules.MLDegraded}, err
	}
	return c.resultFromResponse(resp), nil
}

// resultFromResponse converts a successful scorer response into the rules layer
// result, applying model-version pinning.
func (c *Client) resultFromResponse(resp ScoreResponse) rules.MLResult {
	score := resp.MLScore
	st := resp.Status
	if st == "" {
		st = rules.MLOK
	}
	// STALE_MODEL detection handled by caller with expected version; mark here if mismatch.
	if c.expectedModelVersion != "" && resp.ModelVersion != "" && resp.ModelVersion != c.expectedModelVersion {
		st = rules.MLStaleModel
	}
	return rules.MLResult{
		Score:            &score,
		Status:           st,
		ModelVersion:     resp.ModelVersion,
		FraudProb:        resp.FraudProb,
		AnomalyScore:     resp.AnomalyScore,
		MLScore:          &score,
		TopContributions: toContributions(resp.TopContributions),
		ExplainSkipped:   resp.ExplainSkipped,
		LatencyMS:        resp.LatencyMS,
	}
}

func toContributions(in []Contribution) []rules.Contribution {
	out := make([]rules.Contribution, len(in))
	for i, cc := range in {
		out[i] = rules.Contribution{Feature: cc.Feature, Value: cc.Value, SHAP: cc.SHAP, Direction: cc.Direction}
	}
	return out
}

func featureMap(fv *features.FeatureVector) map[string]any {
	m := map[string]any{}
	if fv == nil {
		return m
	}
	m["velocity_1m"] = fv.Velocity1m
	m["velocity_10m"] = fv.Velocity10m
	m["velocity_1h"] = fv.Velocity1h
	m["amount_sum_10m"] = fv.AmountSum10m
	m["amount_sum_24h"] = fv.AmountSum24h
	m["distinct_recip_24h"] = fv.DistinctRecip24h
	m["amount_vs_avg_30d"] = fv.AmountVsAvg30d
	m["amount_zscore_30d"] = fv.AmountZScore30d
	m["near_limit_ratio"] = fv.NearLimitRatio
	m["small_txn_run_1h"] = fv.SmallTxnRun1h
	m["new_device"] = boolToInt(fv.NewDevice)
	m["new_geo_km"] = fv.NewGeoKm
	m["sim_changed_72h"] = boolToInt(fv.SIMChanged72h)
	m["time_since_sim_change_min"] = fv.TimeSinceSIMChange
	m["acc_per_device_7d"] = fv.AccPerDevice7d
	m["recip_new_for_sender"] = boolToInt(fv.RecipNewForSender)
	m["recip_distinct_senders_24h"] = fv.RecipDistinctSenders24h
	m["recip_cashout_lag_min"] = fv.RecipCashoutLagMin
	m["inbound_outbound_ratio_1h"] = fv.InboundOutboundRatio1h
	m["night_hour"] = boolToInt(fv.NightHour)
	m["cycle_len_2_4"] = fv.CycleLen2To4
	m["agent_cico_ratio_24h"] = fv.AgentCicoRatio24h
	m["agent_float_delta_pct"] = fv.AgentFloatDeltaPct
	m["account_age_days"] = fv.AccountAgeDays
	m["kyc_tier"] = fv.KYCTier
	m["txn_type"] = fv.TxnTypeCode
	m["channel"] = fv.ChannelCode
	return m
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
