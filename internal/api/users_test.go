package api

import (
	"net/http"
	"testing"
	"time"
)

func TestLoginThrottleBlocksAfterLimit(t *testing.T) {
	now := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	throttle := newLoginThrottle(3, time.Minute)
	throttle.now = func() time.Time { return now }

	for i := 0; i < 3; i++ {
		if throttle.blocked("10.0.0.1") {
			t.Fatalf("blocked before reaching the limit at attempt %d", i+1)
		}
		throttle.fail("10.0.0.1")
	}
	if !throttle.blocked("10.0.0.1") {
		t.Fatal("expected the client to be blocked after 3 failures")
	}
	if throttle.blocked("10.0.0.2") {
		t.Fatal("one client's failures must not block another")
	}

	// The window expires and the client is allowed again.
	now = now.Add(2 * time.Minute)
	if throttle.blocked("10.0.0.1") {
		t.Fatal("throttle must lapse after the window")
	}
	throttle.fail("10.0.0.1")
	throttle.succeed("10.0.0.1")
	if throttle.blocked("10.0.0.1") {
		t.Fatal("a successful login must clear the counter")
	}
}

func TestRoleScopesSeparateQueueDuties(t *testing.T) {
	// An analyst must not approve their own work or reopen a closed case.
	analyst := scopesFor("analyst")
	for _, forbidden := range []string{"cases:approve", "cases:reopen", "cases:assign", "users:write", "rules:write"} {
		if analyst[forbidden] {
			t.Errorf("analyst must not hold %s", forbidden)
		}
	}
	for _, required := range []string{"cases:read", "cases:write", "cases:resolve"} {
		if !analyst[required] {
			t.Errorf("analyst must hold %s", required)
		}
	}

	lead := scopesFor("lead")
	for _, required := range []string{"cases:approve", "cases:reopen", "cases:assign", "cases:resolve"} {
		if !lead[required] {
			t.Errorf("lead must hold %s", required)
		}
	}
	if lead["users:write"] {
		t.Error("lead must not manage user accounts")
	}

	auditor := scopesFor("auditor")
	if auditor["cases:write"] || !auditor["cases:read"] {
		t.Error("auditor must read but not mutate cases")
	}

	service := scopesFor("service")
	if service["cases:read"] {
		t.Error("service tokens must not read the analyst queue")
	}
	if !service["cases:create"] {
		t.Error("service tokens must be able to report step-up failures")
	}
}

func scopesFor(role string) map[string]bool {
	out := map[string]bool{}
	for _, s := range RoleScopes[role] {
		out[s] = true
	}
	return out
}

func TestClientKeyPrefersForwardedFor(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, "/v1/auth/login", nil)
	r.RemoteAddr = "127.0.0.1:5555"
	if got := clientKey(r); got != "127.0.0.1:5555" {
		t.Fatalf("got %q", got)
	}
	r.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.1")
	if got := clientKey(r); got != "203.0.113.7" {
		t.Fatalf("got %q", got)
	}
}

func TestReadVersionFromIfMatch(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, "/v1/cases/1/claim", nil)
	if got := readVersion(r); got != 0 {
		t.Fatalf("missing header must read as 0, got %d", got)
	}
	r.Header.Set("If-Match", `"7"`)
	if got := readVersion(r); got != 7 {
		t.Fatalf("got %d want 7", got)
	}
	r.Header.Set("If-Match", "junk")
	if got := readVersion(r); got != 0 {
		t.Fatalf("junk must read as 0, got %d", got)
	}
}

func TestShortHashIsStable(t *testing.T) {
	if shortHash("a") != shortHash("a") {
		t.Fatal("hash must be stable")
	}
	if shortHash("a") == shortHash("b") {
		t.Fatal("distinct inputs must not collide")
	}
	if len(shortHash("a")) != 16 {
		t.Fatalf("unexpected length %d", len(shortHash("a")))
	}
}
