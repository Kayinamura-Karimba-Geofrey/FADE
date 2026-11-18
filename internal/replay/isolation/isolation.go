// Package isolation enforces replay side-effect boundaries. A replay may only
// touch keys under its own namespace and may never write decisions, cases,
// lists or audit decision rows.
package isolation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrNamespaceCollision means another job already owns this namespace.
var ErrNamespaceCollision = errors.New("replay: namespace already in use")

// ErrProductionKey means a write targeted a key outside the job namespace.
var ErrProductionKey = errors.New("replay: attempted write outside job namespace")

// Prefix returns the canonical namespace for a job, e.g. "rp:{42}:".
func Prefix(jobID int64) string {
	return "rp:{" + strconv.FormatInt(jobID, 10) + "}:"
}

// ValidatePrefix accepts only "rp:{<digits>}:".
func ValidatePrefix(prefix string) error {
	if !strings.HasPrefix(prefix, "rp:{") || !strings.HasSuffix(prefix, "}:") {
		return fmt.Errorf("replay: invalid namespace %q", prefix)
	}
	body := strings.TrimSuffix(strings.TrimPrefix(prefix, "rp:{"), "}:")
	if body == "" {
		return fmt.Errorf("replay: empty job id in namespace")
	}
	for _, c := range body {
		if c < '0' || c > '9' {
			return fmt.Errorf("replay: invalid job id in namespace %q", prefix)
		}
	}
	return nil
}

// Guard validates that a key belongs to prefix.
type Guard struct{ prefix string }

// NewGuard builds a write guard.
func NewGuard(prefix string) (*Guard, error) {
	if err := ValidatePrefix(prefix); err != nil {
		return nil, err
	}
	return &Guard{prefix: prefix}, nil
}

// Check returns ErrProductionKey for any key outside the namespace.
func (g *Guard) Check(keys ...string) error {
	for _, k := range keys {
		if !strings.HasPrefix(k, g.prefix) {
			return fmt.Errorf("%w: %q not under %q", ErrProductionKey, k, g.prefix)
		}
	}
	return nil
}

// Reserve claims the namespace with SET NX. A second job with the same id is
// refused, even if the first worker is still starting.
func Reserve(ctx context.Context, rdb redis.Cmdable, prefix string, ttl time.Duration) error {
	if err := ValidatePrefix(prefix); err != nil {
		return err
	}
	if ttl <= 0 {
		ttl = 48 * time.Hour
	}
	ok, err := rdb.SetNX(ctx, prefix+"meta:owner", time.Now().UTC().Format(time.RFC3339Nano), ttl).Result()
	if err != nil {
		return fmt.Errorf("replay: reserve namespace: %w", err)
	}
	if !ok {
		return ErrNamespaceCollision
	}
	return nil
}

// Release deletes every key under the namespace. It is idempotent. When keep
// is true the keys are left for debugging and expire by their own TTL.
func Release(ctx context.Context, rdb redis.Cmdable, prefix string, keep bool) (int, error) {
	if err := ValidatePrefix(prefix); err != nil {
		return 0, err
	}
	if keep {
		return 0, nil
	}
	var cursor uint64
	deleted := 0
	for {
		keys, next, err := rdb.Scan(ctx, cursor, prefix+"*", 1000).Result()
		if err != nil {
			return deleted, fmt.Errorf("replay: scan namespace: %w", err)
		}
		if len(keys) > 0 {
			n, derr := rdb.Del(ctx, keys...).Result()
			if derr != nil {
				return deleted, fmt.Errorf("replay: delete namespace keys: %w", derr)
			}
			deleted += int(n)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return deleted, nil
}

// Fingerprint hashes every production key not under any replay namespace. It is
// used by the isolation guard test to prove a replay changed nothing. Values
// are hashed, not returned, so no feature state is exposed in test logs.
func Fingerprint(ctx context.Context, rdb redis.Cmdable) (string, int, error) {
	var cursor uint64
	h := sha256.New()
	count := 0
	var keys []string
	for {
		batch, next, err := rdb.Scan(ctx, cursor, "*", 1000).Result()
		if err != nil {
			return "", 0, err
		}
		for _, k := range batch {
			if strings.HasPrefix(k, "rp:{") {
				continue
			}
			keys = append(keys, k)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		typ, err := rdb.Type(ctx, k).Result()
		if err != nil {
			return "", 0, err
		}
		var val string
		switch typ {
		case "string":
			val, err = rdb.Get(ctx, k).Result()
		case "hash":
			var m map[string]string
			m, err = rdb.HGetAll(ctx, k).Result()
			pairs := make([]string, 0, len(m))
			for f, v := range m {
				pairs = append(pairs, f+"="+v)
			}
			sort.Strings(pairs)
			val = strings.Join(pairs, "\x00")
		case "zset":
			var z []redis.Z
			z, err = rdb.ZRangeWithScores(ctx, k, 0, -1).Result()
			parts := make([]string, 0, len(z))
			for _, e := range z {
				parts = append(parts, fmt.Sprint(e.Member)+"="+strconv.FormatFloat(e.Score, 'g', -1, 64))
			}
			val = strings.Join(parts, "\x00")
		case "set":
			var s []string
			s, err = rdb.SMembers(ctx, k).Result()
			sort.Strings(s)
			val = strings.Join(s, "\x00")
		case "list":
			var s []string
			s, err = rdb.LRange(ctx, k, 0, -1).Result()
			val = strings.Join(s, "\x00")
		default:
			val = typ
		}
		if err != nil {
			return "", 0, err
		}
		fmt.Fprintf(h, "%s\x00%s\x00%s\n", k, typ, val)
		count++
	}
	return hex.EncodeToString(h.Sum(nil)), count, nil
}

// ResourceLimits are the per-job bounds enforced by the engine/worker.
type ResourceLimits struct {
	MaxMemoryBytes  int64
	MaxShards       int
	MaxConcurrent   int
	ArtifactDiskMax int64
}

// DefaultLimits returns the Phase 6 defaults.
func DefaultLimits() ResourceLimits {
	return ResourceLimits{
		MaxMemoryBytes:  2 << 30,
		MaxShards:       16,
		MaxConcurrent:   2,
		ArtifactDiskMax: 20 << 30,
	}
}

// CheckMemory samples MEMORY USAGE for a key and returns an error when the
// namespace is above the limit.
func CheckMemory(ctx context.Context, rdb redis.Cmdable, prefix string, limit int64) error {
	if limit <= 0 {
		return nil
	}
	var cursor uint64
	sampled := 0
	var total int64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, prefix+"*", 200).Result()
		if err != nil {
			return err
		}
		for _, k := range keys {
			n, uerr := rdb.MemoryUsage(ctx, k).Result()
			if uerr == nil {
				total += n
				sampled++
			}
		}
		cursor = next
		if cursor == 0 || sampled >= 500 {
			break
		}
	}
	if total > limit {
		return fmt.Errorf("replay: namespace memory %d exceeds limit %d", total, limit)
	}
	return nil
}
