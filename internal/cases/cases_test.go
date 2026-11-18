package cases

import (
	"testing"
	"time"
)

func TestStateTransitions(t *testing.T) {
	cases := []struct {
		from, to Status
		ok       bool
	}{
		{StatusOpen, StatusInReview, true},
		{StatusInReview, StatusOpen, true},
		{StatusInReview, StatusResolved, true},
		{StatusResolved, StatusReopened, true},
		{StatusReopened, StatusInReview, true},
		{StatusOpen, StatusResolved, false},
		{StatusResolved, StatusOpen, false},
	}
	for _, c := range cases {
		err := ValidateTransition(c.from, c.to)
		if c.ok && err != nil {
			t.Errorf("expected allowed %s->%s: %v", c.from, c.to, err)
		}
		if !c.ok && err == nil {
			t.Errorf("expected rejected %s->%s", c.from, c.to)
		}
	}
}

func TestPriority(t *testing.T) {
	w := DefaultPriorityWeights()
	got := ComputePriority(50, 2_000_000, true, "BLOCK", 2, w)
	if got != 100 {
		t.Fatalf("got %d want 100", got)
	}
	got2 := ComputePriority(10, 50000, false, "HOLD", 0, w)
	if got2 != 10 {
		t.Fatalf("got %d want 10", got2)
	}
}

func TestRedact(t *testing.T) {
	if err := ScanNote("hello world"); err != nil {
		t.Fatal(err)
	}
	if err := ScanNote("call 0781234567"); err == nil {
		t.Fatal("expected redaction error")
	}
	if err := ScanNote("a@b.com"); err == nil {
		t.Fatal("expected redaction error")
	}
	if err := ScanNote("1234567890123456"); err == nil {
		t.Fatal("expected redaction error")
	}
}

func TestSLA(t *testing.T) {
	cfg := DefaultSLAConfig()
	d := SLADuration(SourceDecision, "BLOCK", cfg)
	if d != 30*time.Minute {
		t.Fatalf("got %v", d)
	}
	d2 := SLADuration(SourceQASample, "", cfg)
	if d2 != 24*time.Hour {
		t.Fatalf("got %v", d2)
	}
}
