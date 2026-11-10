package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func repoFeaturePath(t *testing.T) string {
	t.Helper()
	// internal/config -> repo root
	path, err := filepath.Abs("../../config/features.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadValidEnvironment(t *testing.T) {
	env := map[string]string{
		"KAFKA_BROKERS":              "a:9092, b:9093",
		"REDIS_ADDR":                 "localhost:6379",
		"FEATURE_OUTPUT_TOPIC":       "features.live",
		"WORKERS_PER_PARTITION":      "2",
		"MAX_INFLIGHT_PER_PARTITION": "64",
		"LATE_TOLERANCE_S":           "60",
		"DEDUPE_TTL_S":               "86400",
	}
	cfg, err := Load(repoFeaturePath(t), func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[1] != "b:9093" {
		t.Fatalf("brokers: %#v", cfg.KafkaBrokers)
	}
	if cfg.FeatureOutputTopic != "features.live" || cfg.WorkersPerPartition != 2 || cfg.MaxInflight != 64 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.LateTolerance != 60*time.Second || cfg.DedupeTTL != 24*time.Hour {
		t.Fatalf("durations: %v %v", cfg.LateTolerance, cfg.DedupeTTL)
	}
	if cfg.Features.KYCLimits[3] != 5_000_000 {
		t.Fatalf("kyc limits: %#v", cfg.Features.KYCLimits)
	}
	if cfg.Features.Location().String() != "Africa/Kigali" {
		t.Fatalf("location: %s", cfg.Features.Location())
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	valid := map[string]string{"KAFKA_BROKERS": "a:9092", "REDIS_ADDR": "r:6379"}
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"workers", map[string]string{"WORKERS_PER_PARTITION": "0"}, "WORKERS_PER_PARTITION"},
		{"inflight", map[string]string{"MAX_INFLIGHT_PER_PARTITION": "0"}, "MAX_INFLIGHT"},
		{"brokers", map[string]string{"KAFKA_BROKERS": ","}, "KAFKA_BROKERS"},
		{"redis", map[string]string{"REDIS_ADDR": "noport"}, "REDIS_ADDR"},
		{"dedupe", map[string]string{"DEDUPE_TTL_S": "60"}, "DEDUPE_TTL_S"},
		{"late", map[string]string{"LATE_TOLERANCE_S": "-1"}, "LATE_TOLERANCE_S"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{}
			for k, v := range valid {
				env[k] = v
			}
			for k, v := range tc.env {
				env[k] = v
			}
			if _, err := Load(repoFeaturePath(t), func(k string) (string, bool) { v, ok := env[k]; return v, ok }); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestFeaturesRejectSchemaMismatch(t *testing.T) {
	raw, err := os.ReadFile(repoFeaturePath(t))
	if err != nil {
		t.Fatal(err)
	}
	bad := strings.Replace(string(raw), `schema_version: "1.0.0"`, `schema_version: "9.9.9"`, 1)
	path := filepath.Join(t.TempDir(), "features.yaml")
	if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFeatures(path); err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Fatalf("want schema error, got %v", err)
	}
}

func TestRepositoryFeatureFileLoads(t *testing.T) {
	if _, err := LoadFeatures(repoFeaturePath(t)); err != nil {
		t.Fatalf("repository feature file: %v", err)
	}
}
