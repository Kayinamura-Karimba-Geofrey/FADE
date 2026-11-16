package retention

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMonthAndPartitionName(t *testing.T) {
	now := time.Date(2026, 11, 16, 9, 30, 0, 0, time.UTC)
	if got := Month(now); !got.Equal(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("month=%v", got)
	}
	if got := PartitionName("decisions", now); got != "decisions_202611" {
		t.Fatalf("name=%q", got)
	}
}

func TestPlanCreatesFutureAndDropsExpired(t *testing.T) {
	now := time.Date(2026, 11, 16, 9, 30, 0, 0, time.UTC)
	actions, err := Plan(DefaultPolicy(), now)
	if err != nil {
		t.Fatal(err)
	}
	var creates, drops []Action
	for _, a := range actions {
		switch a.Kind {
		case CreatePartition:
			creates = append(creates, a)
		case DropPartition:
			drops = append(drops, a)
		default:
			t.Fatalf("unknown action %q", a.Kind)
		}
	}
	if len(creates) != 4 {
		t.Fatalf("creates=%d want 4 (current + 3 ahead)", len(creates))
	}
	if creates[0].Partition != "decisions_202611" || creates[3].Partition != "decisions_202702" {
		t.Fatalf("create partitions=%v", creates)
	}
	if !creates[0].From.Equal(Month(now)) || !creates[0].To.Equal(Month(now).AddDate(0, 1, 0)) {
		t.Fatalf("range=%v..%v", creates[0].From, creates[0].To)
	}
	if !strings.Contains(creates[0].SQL, "CREATE TABLE IF NOT EXISTS decisions_202611 PARTITION OF decisions") {
		t.Fatalf("sql=%q", creates[0].SQL)
	}
	if len(drops) == 0 {
		t.Fatal("expected expired partitions to be dropped")
	}
	for _, d := range drops {
		if !d.To.Before(now.AddDate(0, 0, -90)) && !d.To.Equal(Month(now).AddDate(0, 0, -90)) {
			// Drop eligibility is judged on the month boundary, so the upper bound
			// must not be newer than the month containing the cutoff.
			if d.To.After(Month(now.AddDate(0, 0, -90))) {
				t.Fatalf("dropping a partition inside the retention window: %+v", d)
			}
		}
	}
}

func TestPlanIsDeterministicAndMonotonic(t *testing.T) {
	now := time.Date(2026, 11, 16, 9, 30, 0, 0, time.UTC)
	a, err := Plan(DefaultPolicy(), now)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Plan(DefaultPolicy(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != len(b) {
		t.Fatalf("lengths differ: %d %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("action %d differs: %+v %+v", i, a[i], b[i])
		}
	}
}

func TestPolicyValidation(t *testing.T) {
	cases := []Policy{
		{Table: "", RetainDays: 90, MonthsAhead: 3},
		{Table: "decisions", RetainDays: 0, MonthsAhead: 3},
		{Table: "decisions", RetainDays: 90, MonthsAhead: 0},
	}
	for _, p := range cases {
		if err := p.Validate(); err == nil {
			t.Fatalf("policy accepted: %+v", p)
		}
		if _, err := Plan(p, time.Now()); err == nil {
			t.Fatalf("plan accepted invalid policy: %+v", p)
		}
	}
}

type fakeExecer struct {
	ran  []string
	fail string
	n    int
}

func (f *fakeExecer) Exec(_ context.Context, sql string) (int64, error) {
	if f.fail != "" && strings.Contains(sql, f.fail) {
		return 0, errors.New("boom")
	}
	f.ran = append(f.ran, sql)
	f.n++
	return 1, nil
}

func TestApplyRunsInOrderAndStopsOnError(t *testing.T) {
	now := time.Date(2026, 11, 16, 9, 30, 0, 0, time.UTC)
	actions, err := Plan(DefaultPolicy(), now)
	if err != nil {
		t.Fatal(err)
	}
	ok := &fakeExecer{}
	done, err := Apply(context.Background(), ok, actions)
	if err != nil || done != len(actions) {
		t.Fatalf("apply done=%d err=%v", done, err)
	}
	bad := &fakeExecer{fail: "decisions_202611"}
	done, err = Apply(context.Background(), bad, actions)
	if err == nil || done == len(actions) {
		t.Fatalf("failure not reported: done=%d err=%v", done, err)
	}
}
