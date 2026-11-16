package audit

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// VerifyOptions bounds one verification run.
type VerifyOptions struct {
	// From is the first sequence number to verify. Zero starts at the newest
	// checkpoint export, or at genesis when none exists.
	From int64
	// To is the last sequence number to verify. Zero means the chain head.
	To int64
	// CheckpointDir is the append-only export directory. Empty skips the
	// checkpoint-file comparison.
	CheckpointDir string
	// HMACKey verifies checkpoint signatures.
	HMACKey []byte
}

const verifyContextRows = 3

// VerifyChain walks entries in ascending sequence order and returns the first
// break with a small context window. Memory use is constant: entries are
// streamed from the store and only the last few are retained.
func VerifyChain(ctx context.Context, store Store, opts VerifyOptions) (*VerifyResult, error) {
	if err := checkSeqRange(opts.From, opts.To); err != nil {
		return nil, err
	}
	started := time.Now()
	res := &VerifyResult{OK: true}

	expectedPrev := GenesisHash
	expectedSeq := int64(1)
	if opts.From == 0 && opts.CheckpointDir != "" {
		files, err := LoadCheckpointFiles(opts.CheckpointDir)
		if err != nil {
			return nil, err
		}
		if len(files) > 0 {
			sort.Slice(files, func(i, j int) bool { return files[i].Seq < files[j].Seq })
			latest := files[len(files)-1]
			if len(opts.HMACKey) > 0 {
				if err := latest.VerifySignature(opts.HMACKey); err != nil {
					return nil, err
				}
			}
			expectedPrev = latest.Hash
			expectedSeq = latest.Seq + 1
			res.From = expectedSeq
		}
	}
	if opts.From > 0 {
		prev, err := store.AtSeq(ctx, opts.From-1)
		if err != nil {
			return nil, fmt.Errorf("audit: load predecessor of seq %d: %w", opts.From, err)
		}
		expectedPrev = prev.Hash
		expectedSeq = opts.From
		res.From = opts.From
	}
	if res.From == 0 {
		res.From = 1
	}

	var ring []Entry
	var first *BreakError
	var breakEntry Entry
	err := store.Range(ctx, expectedSeq, opts.To, func(e Entry) error {
		if e.Seq != expectedSeq {
			first = &BreakError{Seq: e.Seq, Kind: BreakSeqGap,
				Detail: fmt.Sprintf("expected seq %d, found %d", expectedSeq, e.Seq)}
			breakEntry = e
			return errStopVerify
		}
		if e.PrevHash != expectedPrev {
			// A chain created before Phase 5 starts with an empty prev_hash.
			// Accept that only for the very first row, and record it so the
			// operator knows the chain predates the genesis convention.
			if expectedPrev == GenesisHash && e.Seq <= 1 && e.PrevHash == "" {
				res.LegacyPrelude = true
				expectedPrev = ""
			} else {
				first = &BreakError{Seq: e.Seq, Kind: BreakPrevHash,
					Detail: fmt.Sprintf("prev_hash %q does not match predecessor %q", e.PrevHash, expectedPrev)}
				breakEntry = e
				return errStopVerify
			}
		}
		if err := VerifyLink(e.PrevHash, e); err != nil {
			var be *BreakError
			if errors.As(err, &be) {
				first = be
			} else {
				first = &BreakError{Seq: e.Seq, Kind: BreakRowHash, Detail: err.Error()}
			}
			breakEntry = e
			return errStopVerify
		}
		res.Checked++
		res.To = e.Seq
		expectedPrev = e.Hash
		expectedSeq = e.Seq + 1
		if len(ring) == verifyContextRows {
			copy(ring, ring[1:])
			ring[len(ring)-1] = e
		} else {
			ring = append(ring, e)
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStopVerify) {
		return nil, err
	}
	if first != nil {
		res.OK = false
		res.FirstBreak = first
		res.Context = append(append([]Entry(nil), ring...), breakEntry)
	}

	if opts.CheckpointDir != "" {
		checks, err := VerifyCheckpointFiles(ctx, store, opts.CheckpointDir, opts.HMACKey)
		if err != nil {
			return nil, err
		}
		res.Checkpoints = checks
		for i := range checks {
			if !checks[i].OK {
				res.OK = false
				if res.FirstBreak == nil {
					res.FirstBreak = &BreakError{Seq: checks[i].Seq, Kind: BreakCheckpoint, Detail: checks[i].Detail}
				}
			}
		}
	}

	res.Seconds = time.Since(started).Seconds()
	if res.Seconds > 0 {
		res.RowsPerSec = float64(res.Checked) / res.Seconds
	}
	return res, nil
}

var errStopVerify = errors.New("audit: stop verification")
