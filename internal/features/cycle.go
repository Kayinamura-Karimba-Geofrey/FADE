package features

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// cycleLength returns 2, 3 or 4 when adding from->to closes a simple directed
// cycle of that length within the 24h latest-edge graph. It returns 0 when no
// cycle is found, the hop/neighbour/visited caps are hit, or the time budget
// expires. Results are cached for the configured TTL unless the event is late.
func (s *Store) cycleLength(ctx context.Context, from, to string, eventMS int64, late bool) (int, error) {
	if from == "" || to == "" || from == to {
		return 0, nil
	}
	cfg := s.cfg.Cycle
	useCache := !late && cfg.CacheTTLS > 0
	var cacheKey string
	if useCache {
		cacheKey = cycleCacheKey(from, to)
		cached, err := s.redis.Get(ctx, cacheKey).Int()
		switch {
		case err == nil:
			return cached, nil
		case err != redis.Nil:
			return 0, fmt.Errorf("cycle cache get: %w", err)
		}
	}

	deadline := time.Now().Add(time.Duration(cfg.TimeBudgetMS) * time.Millisecond)
	minScore := strconv.FormatInt(eventMS-int64(s.cfg.Windows.Cycle24hS)*1000, 10)
	maxScore := strconv.FormatInt(eventMS, 10)

	frontier := []string{to}
	visited := map[string]struct{}{to: {}, from: {}}
	found := 0

	for depth := 0; depth < cfg.MaxHops && found == 0; depth++ {
		if time.Now().After(deadline) {
			break
		}
		cmds := make([]*redis.StringSliceCmd, len(frontier))
		zr := &redis.ZRangeBy{Min: "(" + minScore, Max: maxScore, Offset: 0, Count: int64(cfg.MaxNeighbors)}
		if len(frontier) == 1 {
			edges, err := s.redis.ZRangeByScore(ctx, outEdgesKey(frontier[0]), zr).Result()
			if err != nil {
				return 0, fmt.Errorf("cycle edges %s: %w", frontier[0], err)
			}
			cmds[0] = redis.NewStringSliceResult(edges, nil)
		} else {
			_, err := s.redis.Pipelined(ctx, func(p redis.Pipeliner) error {
				for i, node := range frontier {
					cmds[i] = p.ZRangeByScore(ctx, outEdgesKey(node), zr)
				}
				return nil
			})
			if err != nil {
				return 0, fmt.Errorf("cycle edge read: %w", err)
			}
		}
		next := make([]string, 0, len(frontier)*2)
		for i, node := range frontier {
			edges, err := cmds[i].Result()
			if err != nil {
				return 0, fmt.Errorf("cycle edges %s: %w", node, err)
			}
			for _, neighbour := range edges {
				if neighbour == from {
					found = depth + 2
					break
				}
				if depth+1 >= cfg.MaxHops {
					continue
				}
				if _, ok := visited[neighbour]; ok {
					continue
				}
				visited[neighbour] = struct{}{}
				if len(visited) > cfg.MaxVisited {
					found = 0
					goto done
				}
				next = append(next, neighbour)
			}
			if found != 0 {
				break
			}
			if time.Now().After(deadline) {
				break
			}
		}
		frontier = next
	}

done:
	if useCache {
		if err := s.redis.Set(ctx, cacheKey, found, time.Duration(cfg.CacheTTLS)*time.Second).Err(); err != nil {
			return 0, fmt.Errorf("cycle cache set: %w", err)
		}
	}
	return found, nil
}
