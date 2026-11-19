package scoring

import (
	"context"
	"sync"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/features"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// microBatcher coalesces concurrent single-row score calls into one
// /score/batch request. Per-row scoring in the Python scorer costs ~13 ms
// (IsolationForest walks 200 trees per row); a 32-row vectorized call costs
// ~3 ms total, i.e. ~0.1 ms/row. The window keeps added latency <= 2 ms and
// every decision still uses the same per-row model output.
type microBatcherClient interface {
	score(ctx context.Context, fv *features.FeatureVector) (rules.MLResult, error)
}

type microBatcher struct {
	client *Client
	batch  *BatchClient
	max    int
	window time.Duration
	reqs   chan *batchCall

	once sync.Once
}

type batchCall struct {
	vector *features.FeatureVector
	out    chan batchOutcome
}

type batchOutcome struct {
	result rules.MLResult
	err    error
}

func newMicroBatcher(client *Client, max int, window time.Duration, batch *BatchClient) *microBatcher {
	if max < 2 {
		max = 32
	}
	if window <= 0 {
		window = 2 * time.Millisecond
	}
	return &microBatcher{
		client: client,
		batch:  batch,
		max:    max,
		window: window,
		reqs:   make(chan *batchCall, max*8),
	}
}

// score enqueues a vector and waits for its result. A full batch or the
// elapsed window triggers a flush; the batch itself is scored with the
// existing single-request client contract (breaker, timeout, model pinning).
func (b *microBatcher) score(ctx context.Context, fv *features.FeatureVector) (rules.MLResult, error) {
	b.once.Do(func() { go b.loop() })
	call := &batchCall{vector: fv, out: make(chan batchOutcome, 1)}
	select {
	case b.reqs <- call:
	case <-ctx.Done():
		return rules.MLResult{Status: rules.MLDegraded}, ctx.Err()
	}
	select {
	case out := <-call.out:
		return out.result, out.err
	case <-ctx.Done():
		return rules.MLResult{Status: rules.MLDegraded}, ctx.Err()
	}
}

func (b *microBatcher) loop() {
	for {
		first, ok := <-b.reqs
		if !ok {
			return
		}
		batch := []*batchCall{first}
		timer := time.NewTimer(b.window)
	collect:
		for len(batch) < b.max {
			select {
			case call, ok := <-b.reqs:
				if !ok {
					break collect
				}
				batch = append(batch, call)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		b.flush(batch)
	}
}

func (b *microBatcher) flush(batch []*batchCall) {
	if len(batch) == 1 {
		// A lone request costs the same either way; skip the batch endpoint.
		res, err := b.client.call(context.Background(), batch[0].vector, false)
		out := batchOutcome{err: err}
		if err == nil {
			out.result = b.client.resultFromResponse(res)
		} else {
			out.result = rules.MLResult{Status: rules.MLDegraded}
		}
		batch[0].out <- out
		return
	}
	vectors := make([]*features.FeatureVector, len(batch))
	for i, call := range batch {
		vectors[i] = call.vector
	}
	results, err := b.batch.ScoreBatch(context.Background(), vectors, b.client.expectedModelVersion, false)
	if err != nil {
		for _, call := range batch {
			call.out <- batchOutcome{result: rules.MLResult{Status: rules.MLDegraded}, err: err}
		}
		return
	}
	for i, call := range batch {
		call.out <- batchOutcome{result: results[i]}
	}
}
