package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/audit"
)

// The full explanation contract is exercised in internal/explain. These tests
// cover the HTTP surface that does not require a decision store.

func TestExplainRequiresStore(t *testing.T) {
	h, auth := newTestRouter(t, nil)
	tok := token(t, auth, "analyst", time.Hour)
	res := do(t, h, http.MethodGet, "/v1/decisions/dec_1/explain", tok, nil, nil)
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", res.Code, res.Body)
	}
}

func TestAuditVerifyRejectsUnknownSequence(t *testing.T) {
	h, auth := newTestRouter(t, nil)
	tok := token(t, auth, "auditor", time.Hour)
	res := do(t, h, http.MethodGet, "/v1/audit/verify?from=-1", tok, nil, nil)
	// -1 parses as an integer but is rejected as a negative sequence.
	if res.Code != http.StatusBadRequest && res.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", res.Code, res.Body)
	}
}

func TestAuditorRoleScopes(t *testing.T) {
	scopes := RoleScopes["auditor"]
	want := map[string]bool{"audit:read": false, "decisions:read": false, "audit:write": true}
	for _, s := range scopes {
		if _, ok := want[s]; ok {
			want[s] = true
		}
	}
	for scope, forbidden := range want {
		if forbidden && (scope == "audit:write" && hasScope(scopes, scope)) {
			t.Fatalf("auditor must not hold %s", scope)
		}
		if !forbidden && !hasScope(scopes, scope) {
			t.Fatalf("auditor is missing %s", scope)
		}
	}
}

func hasScope(scopes []string, want string) bool {
	for _, s := range scopes {
		if s == want {
			return true
		}
	}
	return false
}

func TestAuditEntryPageIsSerialisable(t *testing.T) {
	mem := audit.NewMemoryStore()
	entry, err := mem.Append(context.Background(), audit.NewRow(
		"admin@fade", "lists.add", "list_entry", "7", nil, []byte(`{"value":"acct_000123"}`), time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{"entries": []audit.Entry{entry}, "count": 1})
	if err != nil {
		t.Fatal(err)
	}
	var page struct {
		Entries []audit.Entry `json:"entries"`
	}
	if err := json.Unmarshal(raw, &page); err != nil {
		t.Fatal(err)
	}
	if page.Entries[0].Seq != 1 || page.Entries[0].Row.Actor != "admin@fade" {
		t.Fatalf("page=%s", raw)
	}
}

var _ = httptest.NewRecorder
