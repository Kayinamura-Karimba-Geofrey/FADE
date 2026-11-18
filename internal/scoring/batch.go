package scoring

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// BatchClient scores many feature vectors in one request, pinned to a model
// version. It is used only by replay; the live path keeps the low-latency
// single-request client.
type BatchClient struct {
	baseURL string
	http    *http.Client
	timeout time.Duration
}

// NewBatchClient builds a batch scorer client.
func NewBatchClient(baseURL string, timeout time.Duration) *BatchClient {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &BatchClient{
		baseURL: baseURL,
		timeout: timeout,
		http: &http.Client{Transport: &http.Transport{
			MaxIdleConns: 32, MaxIdleConnsPerHost: 32, IdleConnTimeout: 90 * time.Second,
		}},
	}
}

// Available reports whether a base URL was configured.
func (b *BatchClient) Available() bool { return b != nil && b.baseURL != "" }

type batchItem struct {
	Features     map[string]any `json:"features"`
	Explain      bool           `json:"explain"`
	ModelVersion string         `json:"model_version,omitempty"`
}

type batchRequest struct {
	Items []batchItem `json:"items"`
}

type batchResponse struct {
	Results []ScoreResponse `json:"results"`
}

// ScoreBatch returns one MLResult per vector, in order. An error is returned
// for transport failure or for any response whose model version differs from
// the pinned version; replay never silently uses another model.
func (b *BatchClient) ScoreBatch(ctx context.Context, vectors []*features.FeatureVector, modelVersion string, explain bool) ([]rules.MLResult, error) {
	if !b.Available() {
		return nil, errors.New("scorer not configured")
	}
	items := make([]batchItem, len(vectors))
	for i, fv := range vectors {
		items[i] = batchItem{Features: featureMap(fv), Explain: explain, ModelVersion: modelVersion}
	}
	raw, err := json.Marshal(batchRequest{Items: items})
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, b.baseURL+"/score/batch", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var body struct {
			Detail any `json:"detail"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return nil, fmt.Errorf("scorer batch status %d: %v", resp.StatusCode, body.Detail)
	}
	var out batchResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if len(out.Results) != len(vectors) {
		return nil, fmt.Errorf("scorer batch returned %d results for %d vectors", len(out.Results), len(vectors))
	}
	results := make([]rules.MLResult, len(out.Results))
	for i, r := range out.Results {
		if modelVersion != "" && r.ModelVersion != "" && r.ModelVersion != modelVersion {
			return nil, fmt.Errorf("scorer returned model %q, pinned %q", r.ModelVersion, modelVersion)
		}
		score := r.MLScore
		status := r.Status
		if status == "" {
			status = rules.MLOK
		}
		results[i] = rules.MLResult{
			Score: &score, MLScore: &score, Status: status,
			ModelVersion: r.ModelVersion, FraudProb: r.FraudProb,
			AnomalyScore: r.AnomalyScore, LatencyMS: r.LatencyMS,
			TopContributions: toContributions(r.TopContributions),
			ExplainSkipped:   r.ExplainSkipped,
		}
	}
	return results, nil
}
