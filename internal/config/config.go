// Package config loads and validates the Phase 2 worker configuration.
//
// Environment variables are validated eagerly (fail fast): the worker must
// not start with a partial broker list, an unusable Redis address or a
// feature file that disagrees with the compiled schema version.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // keep Africa/Kigali available in scratch containers

	"gopkg.in/yaml.v3"
)

// SchemaVersion is the feature schema this binary implements. It must match
// config/features.yaml and the Python reference.
const SchemaVersion = "1.0.0"

// DefaultFeaturePath is used when FEATURE_CONFIG is unset.
const DefaultFeaturePath = "config/features.yaml"

// Config is the complete worker configuration.
type Config struct {
	KafkaBrokers        []string
	KafkaTopic          string
	KafkaDLQTopic       string
	KafkaGroupID        string
	FeatureOutputTopic  string
	RedisAddr           string
	RedisPassword       string
	RedisDB             int
	PostgresHost        string
	PostgresPort        int
	PostgresDB          string
	PostgresUser        string
	PostgresPassword    string
	WorkersPerPartition int
	MaxInflight         int
	LateTolerance       time.Duration
	DedupeTTL           time.Duration
	HTTPAddr            string
	LogLevel            string
	FeaturesPath        string
	Features            Features
}

// Features mirrors config/features.yaml. Durations are expressed in seconds
// in YAML and converted to time.Duration at load time.
type Features struct {
	SchemaVersion string        `yaml:"schema_version"`
	Windows       Windows       `yaml:"windows"`
	Stats         Stats         `yaml:"stats"`
	KYCLimits     map[int]int64 `yaml:"kyc_daily_limits_rwf"`
	Structuring   Structuring   `yaml:"structuring"`
	Time          TimeConfig    `yaml:"time"`
	Geo           GeoConfig     `yaml:"geo"`
	Cycle         CycleConfig   `yaml:"cycle"`
	Agent         AgentConfig   `yaml:"agent"`
	Caps          Caps          `yaml:"caps"`
	Retention     Retention     `yaml:"retention"`
}

type Windows struct {
	Velocity1mS          int `yaml:"velocity_1m_s"`
	Velocity10mS         int `yaml:"velocity_10m_s"`
	Velocity1hS          int `yaml:"velocity_1h_s"`
	Amount10mS           int `yaml:"amount_10m_s"`
	Amount24hS           int `yaml:"amount_24h_s"`
	DistinctRecip24hS    int `yaml:"distinct_recip_24h_s"`
	SmallTxn1hS          int `yaml:"small_txn_1h_s"`
	Device7dS            int `yaml:"device_7d_s"`
	Recipient30dS        int `yaml:"recipient_30d_s"`
	RecipientSenders24hS int `yaml:"recipient_senders_24h_s"`
	InboundOutbound1hS   int `yaml:"inbound_outbound_1h_s"`
	AgentCico24hS        int `yaml:"agent_cico_24h_s"`
	Cycle24hS            int `yaml:"cycle_24h_s"`
	SimChange72hS        int `yaml:"sim_change_72h_s"`
}

type Stats struct {
	MinCount30d     int `yaml:"min_count_30d"`
	DailyBuckets    int `yaml:"daily_buckets"`
	DailyWindowDays int `yaml:"daily_window_days"`
}

type Structuring struct {
	ThresholdRWF int64   `yaml:"threshold_rwf"`
	LowerRatio   float64 `yaml:"lower_ratio"`
	UpperRatio   float64 `yaml:"upper_ratio"`
	MinRun       int     `yaml:"min_run"`
}

type TimeConfig struct {
	Timezone       string `yaml:"timezone"`
	NightStartHour int    `yaml:"night_start_hour"`
	NightEndHour   int    `yaml:"night_end_hour"`
}

type GeoConfig struct {
	EarthRadiusKM float64 `yaml:"earth_radius_km"`
}

type CycleConfig struct {
	MaxHops      int `yaml:"max_hops"`
	MaxNeighbors int `yaml:"max_neighbors"`
	MaxVisited   int `yaml:"max_visited"`
	TimeBudgetMS int `yaml:"time_budget_ms"`
	CacheTTLS    int `yaml:"cache_ttl_s"`
}

type AgentConfig struct {
	FloatBaselineRWF int64 `yaml:"float_baseline_rwf"`
	HourlyBuckets    int   `yaml:"hourly_buckets"`
}

type Caps struct {
	WindowZSet   int `yaml:"window_zset"`
	OutEdges     int `yaml:"out_edges"`
	StateHistory int `yaml:"state_history"`
}

type Retention struct {
	StateTTLS  int `yaml:"state_ttl_s"`
	WindowTTLS int `yaml:"window_ttl_s"`
	EdgeTTLS   int `yaml:"edge_ttl_s"`
	DeviceTTLS int `yaml:"device_ttl_s"`
}

// Location is the Kigali timezone used by night_hour.
func (f Features) Location() *time.Location {
	loc, err := time.LoadLocation(f.Time.Timezone)
	if err != nil {
		return time.FixedZone("CAT", 2*60*60)
	}
	return loc
}

func env(lookup func(string) (string, bool), key, fallback string) string {
	if v, ok := lookup(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}

func envInt(lookup func(string) (string, bool), key string, fallback int) (int, error) {
	raw := env(lookup, key, "")
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return v, nil
}

// Load reads the feature file and environment. lookup is normally
// os.LookupEnv; tests inject their own map.
func Load(featuresPath string, lookup func(string) (string, bool)) (*Config, error) {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	if strings.TrimSpace(featuresPath) == "" {
		featuresPath = DefaultFeaturePath
	}
	features, err := LoadFeatures(featuresPath)
	if err != nil {
		return nil, err
	}

	brokersRaw := env(lookup, "KAFKA_BROKERS", "localhost:9092")
	brokers := splitNonEmpty(brokersRaw)
	if len(brokers) == 0 {
		return nil, errors.New("KAFKA_BROKERS must contain at least one broker")
	}
	redisAddr := env(lookup, "REDIS_ADDR", "localhost:6379")
	if !strings.Contains(redisAddr, ":") {
		return nil, fmt.Errorf("REDIS_ADDR %q must be host:port", redisAddr)
	}

	workers, err := envInt(lookup, "WORKERS_PER_PARTITION", 1)
	if err != nil {
		return nil, err
	}
	inflight, err := envInt(lookup, "MAX_INFLIGHT_PER_PARTITION", 256)
	if err != nil {
		return nil, err
	}
	lateS, err := envInt(lookup, "LATE_TOLERANCE_S", 60)
	if err != nil {
		return nil, err
	}
	dedupeS, err := envInt(lookup, "DEDUPE_TTL_S", 86400)
	if err != nil {
		return nil, err
	}
	redisDB, err := envInt(lookup, "REDIS_DB", 0)
	if err != nil {
		return nil, err
	}
	pgPort, err := envInt(lookup, "POSTGRES_PORT", 5432)
	if err != nil {
		return nil, err
	}
	if workers < 1 {
		return nil, errors.New("WORKERS_PER_PARTITION must be >= 1")
	}
	if inflight < workers {
		return nil, errors.New("MAX_INFLIGHT_PER_PARTITION must be >= WORKERS_PER_PARTITION")
	}
	if lateS < 0 {
		return nil, errors.New("LATE_TOLERANCE_S must be >= 0")
	}
	if dedupeS < int(features.Windows.Amount24hS) {
		return nil, errors.New("DEDUPE_TTL_S must cover the 24h window")
	}

	cfg := &Config{
		KafkaBrokers:        brokers,
		KafkaTopic:          env(lookup, "KAFKA_TOPIC", "txn.raw"),
		KafkaDLQTopic:       env(lookup, "KAFKA_DLQ_TOPIC", "txn.dead"),
		KafkaGroupID:        env(lookup, "KAFKA_GROUP_ID", "fade-feature-worker"),
		FeatureOutputTopic:  env(lookup, "FEATURE_OUTPUT_TOPIC", ""),
		RedisAddr:           redisAddr,
		RedisPassword:       env(lookup, "REDIS_PASSWORD", ""),
		RedisDB:             redisDB,
		PostgresHost:        env(lookup, "POSTGRES_HOST", "localhost"),
		PostgresPort:        pgPort,
		PostgresDB:          env(lookup, "POSTGRES_DB", "fade"),
		PostgresUser:        env(lookup, "POSTGRES_USER", "fade"),
		PostgresPassword:    env(lookup, "POSTGRES_PASSWORD", "change-me"),
		WorkersPerPartition: workers,
		MaxInflight:         inflight,
		LateTolerance:       time.Duration(lateS) * time.Second,
		DedupeTTL:           time.Duration(dedupeS) * time.Second,
		HTTPAddr:            env(lookup, "WORKER_HTTP_ADDR", ":8083"),
		LogLevel:            env(lookup, "LOG_LEVEL", "info"),
		FeaturesPath:        featuresPath,
		Features:            features,
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadFeatures parses and validates config/features.yaml.
func LoadFeatures(path string) (Features, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Features{}, fmt.Errorf("read feature config %s: %w", path, err)
	}
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	var f Features
	if err := dec.Decode(&f); err != nil {
		return Features{}, fmt.Errorf("parse feature config %s: %w", path, err)
	}
	if err := f.Validate(); err != nil {
		return Features{}, fmt.Errorf("feature config %s: %w", path, err)
	}
	return f, nil
}

// Validate checks every invariant the runtime relies on.
func (f Features) Validate() error {
	if f.SchemaVersion != SchemaVersion {
		return fmt.Errorf("schema_version %q does not match compiled %q", f.SchemaVersion, SchemaVersion)
	}
	windows := map[string]int{
		"velocity_1m_s": f.Windows.Velocity1mS, "velocity_10m_s": f.Windows.Velocity10mS,
		"velocity_1h_s": f.Windows.Velocity1hS, "amount_10m_s": f.Windows.Amount10mS,
		"amount_24h_s": f.Windows.Amount24hS, "distinct_recip_24h_s": f.Windows.DistinctRecip24hS,
		"small_txn_1h_s": f.Windows.SmallTxn1hS, "device_7d_s": f.Windows.Device7dS,
		"recipient_30d_s": f.Windows.Recipient30dS, "recipient_senders_24h_s": f.Windows.RecipientSenders24hS,
		"inbound_outbound_1h_s": f.Windows.InboundOutbound1hS, "agent_cico_24h_s": f.Windows.AgentCico24hS,
		"cycle_24h_s": f.Windows.Cycle24hS, "sim_change_72h_s": f.Windows.SimChange72hS,
	}
	for name, v := range windows {
		if v <= 0 {
			return fmt.Errorf("%s must be > 0", name)
		}
	}
	if f.Stats.MinCount30d < 0 || f.Stats.DailyWindowDays < 1 || f.Stats.DailyBuckets < f.Stats.DailyWindowDays {
		return errors.New("stats: min_count_30d >= 0 and daily_buckets >= daily_window_days >= 1 required")
	}
	for _, tier := range []int{1, 2, 3} {
		if f.KYCLimits[tier] <= 0 {
			return fmt.Errorf("kyc_daily_limits_rwf[%d] must be > 0", tier)
		}
	}
	if f.Structuring.ThresholdRWF <= 0 || f.Structuring.LowerRatio <= 0 ||
		f.Structuring.LowerRatio > f.Structuring.UpperRatio || f.Structuring.UpperRatio > 1 {
		return errors.New("structuring ratios must satisfy 0 < lower <= upper <= 1 and threshold > 0")
	}
	if f.Time.Timezone == "" || f.Time.NightStartHour < 0 || f.Time.NightStartHour > 23 ||
		f.Time.NightEndHour < f.Time.NightStartHour || f.Time.NightEndHour > 23 {
		return errors.New("time: valid timezone and 0 <= night_start_hour <= night_end_hour <= 23 required")
	}
	if _, err := time.LoadLocation(f.Time.Timezone); err != nil {
		return fmt.Errorf("time.timezone: %w", err)
	}
	if f.Geo.EarthRadiusKM <= 0 {
		return errors.New("geo.earth_radius_km must be > 0")
	}
	if f.Cycle.MaxHops != 3 || f.Cycle.MaxNeighbors < 1 || f.Cycle.MaxVisited < 1 ||
		f.Cycle.TimeBudgetMS < 1 || f.Cycle.CacheTTLS < 0 {
		return errors.New("cycle: max_hops must be 3 (cycle_len_2_4) and limits positive")
	}
	if f.Agent.FloatBaselineRWF <= 0 || f.Agent.HourlyBuckets < 31 {
		return errors.New("agent: float_baseline_rwf > 0 and hourly_buckets >= 31 required")
	}
	if f.Caps.WindowZSet < 100 || f.Caps.OutEdges < 1 || f.Caps.StateHistory < 10 {
		return errors.New("caps: window_zset >= 100, out_edges >= 1, state_history >= 10 required")
	}
	if f.Retention.StateTTLS < f.Windows.Amount24hS || f.Retention.WindowTTLS < f.Windows.Recipient30dS ||
		f.Retention.EdgeTTLS < f.Windows.Recipient30dS || f.Retention.DeviceTTLS < f.Windows.Device7dS {
		return errors.New("retention TTLs must cover their windows")
	}
	return nil
}

func splitNonEmpty(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Validate checks the complete worker configuration.
func (c Config) Validate() error {
	if len(c.KafkaBrokers) == 0 {
		return errors.New("at least one Kafka broker is required")
	}
	if c.KafkaTopic == "" || c.KafkaDLQTopic == "" || c.KafkaGroupID == "" {
		return errors.New("KAFKA_TOPIC, KAFKA_DLQ_TOPIC and KAFKA_GROUP_ID must be non-empty")
	}
	if c.KafkaTopic == c.KafkaDLQTopic {
		return errors.New("KAFKA_TOPIC and KAFKA_DLQ_TOPIC must differ")
	}
	if c.FeatureOutputTopic == c.KafkaTopic {
		return errors.New("FEATURE_OUTPUT_TOPIC must differ from KAFKA_TOPIC")
	}
	if c.RedisAddr == "" || !strings.Contains(c.RedisAddr, ":") {
		return fmt.Errorf("REDIS_ADDR %q must be host:port", c.RedisAddr)
	}
	if c.WorkersPerPartition < 1 || c.MaxInflight < c.WorkersPerPartition {
		return errors.New("invalid worker/inflight counts")
	}
	if c.PostgresHost == "" || c.PostgresPort < 1 || c.PostgresPort > 65535 ||
		c.PostgresDB == "" || c.PostgresUser == "" {
		return errors.New("POSTGRES_HOST/PORT/DB/USER must be valid")
	}
	return c.Features.Validate()
}

// PostgresDSN returns the pgx connection string (decision-api compatibility).
func (c Config) PostgresDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		c.PostgresUser, c.PostgresPassword, c.PostgresHost, c.PostgresPort, c.PostgresDB)
}
