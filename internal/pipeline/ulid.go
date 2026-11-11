package pipeline

import (
	"crypto/rand"
	"time"
)

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// NewDecisionID returns "dec_" + a 26-character Crockford ULID. ULIDs sort by
// time and carry 80 random bits, so decision ids are unique without a lock.
func NewDecisionID(now time.Time) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	ms := uint64(now.UTC().UnixMilli())
	var out [26]byte
	for i := 9; i >= 0; i-- {
		out[i] = crockford[ms&31]
		ms >>= 5
	}
	// 80 random bits -> 16 base32 characters (8 from each 40-bit half).
	var hi, lo uint64
	for i := 0; i < 5; i++ {
		hi = hi<<8 | uint64(b[i])
		lo = lo<<8 | uint64(b[i+5])
	}
	for i := 25; i >= 18; i-- {
		out[i] = crockford[lo&31]
		lo >>= 5
	}
	for i := 17; i >= 10; i-- {
		out[i] = crockford[hi&31]
		hi >>= 5
	}
	return "dec_" + string(out[:])
}
