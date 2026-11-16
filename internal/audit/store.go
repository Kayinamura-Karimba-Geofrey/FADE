package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// ErrChainEmpty is returned by Latest when the log has no rows. Head never
// returns it: an empty chain has the documented genesis head.
var ErrChainEmpty = errors.New("audit: chain is empty")

// ErrSeqNotFound is returned when a sequence number is not present.
var ErrSeqNotFound = errors.New("audit: sequence not found")

// Filter selects audit rows for GET /v1/audit.
type Filter struct {
	Actor      string
	Action     string
	ObjectType string
	ObjectID   string
	From       time.Time
	To         time.Time
	AfterSeq   int64
	Limit      int
}

// Store is the audit persistence contract. Implementations must serialise
// Append so sequence numbers are contiguous and the chain never forks.
type Store interface {
	Append(ctx context.Context, row Row) (Entry, error)
	Head(ctx context.Context) (Entry, error)
	Latest(ctx context.Context) (Entry, error)
	AtSeq(ctx context.Context, seq int64) (Entry, error)
	CountTo(ctx context.Context, seq int64) (int64, error)
	Count(ctx context.Context) (int64, error)
	// Range streams entries in ascending sequence order. to=0 means the head.
	Range(ctx context.Context, from, to int64, fn func(Entry) error) error
	List(ctx context.Context, f Filter) ([]Entry, int64, error)
	InsertCheckpoint(ctx context.Context, cp Checkpoint, path string) error
	LatestCheckpoint(ctx context.Context) (*Checkpoint, error)
	Checkpoints(ctx context.Context, limit int) ([]Checkpoint, error)
	// Rejects reports whether the application role can mutate the log. A
	// healthy deployment returns false; used by readiness and tests.
	Writable(ctx context.Context) (bool, error)
}

// GenesisEntry is the predecessor of the first row in a chain.
func GenesisEntry() Entry { return Entry{Seq: 0, Hash: GenesisHash, PrevHash: ""} }

// NextSeq computes the sequence number following head.
func NextSeq(head Entry) int64 { return head.Seq + 1 }

// BuildEntry computes prev/hash for a row at a sequence number.
func BuildEntry(seq int64, prev string, row Row) (Entry, error) {
	hash, err := ChainHash(prev, row)
	if err != nil {
		return Entry{}, err
	}
	return Entry{Seq: seq, PrevHash: prev, Hash: hash, Row: row}, nil
}

// HashIP returns the salted hash of a client IP. Raw IPs are never stored.
func HashIP(salt []byte, ip string) string {
	if ip == "" {
		return ""
	}
	h := sha256.New()
	h.Write(salt)
	h.Write([]byte(ip))
	return hex.EncodeToString(h.Sum(nil))
}

func validateFilter(f *Filter) {
	if f.Limit <= 0 || f.Limit > 1000 {
		f.Limit = 100
	}
	if f.AfterSeq < 0 {
		f.AfterSeq = 0
	}
}

func checkSeqRange(from, to int64) error {
	if from < 0 || to < 0 {
		return fmt.Errorf("audit: sequence bounds must be non-negative")
	}
	if to != 0 && to < from {
		return fmt.Errorf("audit: to_seq must be >= from_seq")
	}
	return nil
}
