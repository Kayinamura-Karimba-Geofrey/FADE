// Package retention plans and applies monthly partition maintenance and data
// expiry for FADE's time-partitioned tables.
package retention

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Policy is one retention rule.
type Policy struct {
	Table      string `json:"table"`
	RetainDays int    `json:"retain_days"`
	// MonthsAhead is how many future monthly partitions to keep created.
	MonthsAhead int `json:"months_ahead"`
}

// DefaultPolicy is the documented Phase 5 policy: 90 days of decisions.
func DefaultPolicy() Policy {
	return Policy{Table: "decisions", RetainDays: 90, MonthsAhead: 3}
}

// Validate rejects policies that would delete recent data or leave the table
// without a future partition.
func (p Policy) Validate() error {
	if p.Table == "" {
		return errors.New("retention: table is required")
	}
	if p.RetainDays < 1 {
		return fmt.Errorf("retention: retain_days must be >= 1, got %d", p.RetainDays)
	}
	if p.MonthsAhead < 1 {
		return fmt.Errorf("retention: months_ahead must be >= 1, got %d", p.MonthsAhead)
	}
	return nil
}

// ActionKind is a partition operation.
type ActionKind string

// Action kinds.
const (
	CreatePartition ActionKind = "CREATE_PARTITION"
	DropPartition   ActionKind = "DROP_PARTITION"
)

// Action is one planned or applied operation.
type Action struct {
	Kind      ActionKind `json:"kind"`
	Table     string     `json:"table"`
	Partition string     `json:"partition"`
	From      time.Time  `json:"from"`
	To        time.Time  `json:"to"`
	SQL       string     `json:"sql"`
}

// Month is the first instant of the calendar month containing t.
func Month(t time.Time) time.Time {
	y, m, _ := t.UTC().Date()
	return time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
}

// PartitionName returns the deterministic partition name for a month.
func PartitionName(table string, month time.Time) string {
	return fmt.Sprintf("%s_%s", table, month.UTC().Format("200601"))
}

// Plan computes the partition actions for a policy at now. The result is
// deterministic: partitions from the current month through months_ahead are
// created, and partitions whose upper bound is older than the retention window
// are dropped.
func Plan(p Policy, now time.Time) ([]Action, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	now = now.UTC()
	current := Month(now)
	cutoff := Month(now.AddDate(0, 0, -p.RetainDays))
	if !cutoff.Before(current) {
		// A retention window under one month would try to drop live data.
		return nil, fmt.Errorf("retention: retain_days %d is shorter than the partition interval", p.RetainDays)
	}

	var create []Action
	for i := 0; i <= p.MonthsAhead; i++ {
		from := current.AddDate(0, i, 0)
		to := from.AddDate(0, 1, 0)
		name := PartitionName(p.Table, from)
		create = append(create, Action{
			Kind: CreatePartition, Table: p.Table, Partition: name, From: from, To: to,
			SQL: fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s');",
				name, p.Table, from.Format("2006-01-02"), to.Format("2006-01-02")),
		})
	}

	// Partitions are considered from the oldest month this policy could have
	// created to the cutoff. Only months strictly before the cutoff are planned,
	// so a partition holding live data can never be dropped.
	var drop []Action
	for m := cutoff.AddDate(0, -p.MonthsAhead-12, 0); m.Before(cutoff); m = m.AddDate(0, 1, 0) {
		name := PartitionName(p.Table, m)
		drop = append(drop, Action{
			Kind: DropPartition, Table: p.Table, Partition: name,
			From: m, To: m.AddDate(0, 1, 0),
			SQL: fmt.Sprintf("DROP TABLE IF EXISTS %s;", name),
		})
	}
	return append(drop, create...), nil
}

// Execer runs one statement.
type Execer interface {
	Exec(ctx context.Context, sql string) (int64, error)
}

// Apply runs the actions in order and returns how many succeeded. A failure
// stops the run and is returned with the action that failed.
func Apply(ctx context.Context, e Execer, actions []Action) (int, error) {
	done := 0
	for _, a := range actions {
		if _, err := e.Exec(ctx, a.SQL); err != nil {
			return done, fmt.Errorf("retention: %s %s: %w", a.Kind, a.Partition, err)
		}
		done++
	}
	return done, nil
}
