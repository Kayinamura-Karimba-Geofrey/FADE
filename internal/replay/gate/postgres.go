package gate

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresVerdictStore implements VerdictStore over replay_gates.
type PostgresVerdictStore struct {
	pool *pgxpool.Pool
}

func NewPostgresVerdictStore(pool *pgxpool.Pool) *PostgresVerdictStore {
	return &PostgresVerdictStore{pool: pool}
}

func (s *PostgresVerdictStore) Latest(ctx context.Context, rulesHash string) (Verdict, bool, error) {
	if s == nil || s.pool == nil {
		return Verdict{}, false, fmt.Errorf("gate: no pool")
	}
	row := s.pool.QueryRow(ctx, `
		SELECT job_id, passed, created_at
		FROM replay_gates
		WHERE rules_hash = $1
		ORDER BY created_at DESC
		LIMIT 1`, rulesHash)
	var v Verdict
	if err := row.Scan(&v.JobID, &v.Passed, &v.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Verdict{}, false, nil
		}
		return Verdict{}, false, err
	}
	v.RulesHash = rulesHash
	return v, true, nil
}
