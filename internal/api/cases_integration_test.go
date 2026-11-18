package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/auth"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/cases"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/obs"
)

// caseTestEnv is a real Postgres-backed router: real migrations, real SQL, real
// JWTs. Only the decision pipeline is absent, because no case endpoint needs it.
type caseTestEnv struct {
	handler http.Handler
	pool    *pgxpool.Pool
	store   *cases.Store
}

func newCaseTestEnv(t *testing.T) *caseTestEnv {
	t.Helper()
	dsn := os.Getenv("FADE_TEST_POSTGRES_DSN")
	cleanup := func() {}
	if dsn == "" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		container, err := postgrescontainer.Run(ctx, "postgres:16.4-alpine3.20",
			postgrescontainer.WithDatabase("fade"), postgrescontainer.WithUsername("fade"),
			postgrescontainer.WithPassword("fade"), postgrescontainer.BasicWaitStrategies())
		cancel()
		if err != nil {
			t.Skipf("start postgres: %v", err)
		}
		cleanup = func() { _ = testcontainers.TerminateContainer(container) }
		ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		dsn, err = container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			cleanup()
			t.Fatal(err)
		}
	}
	t.Cleanup(cleanup)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for _, name := range []string{"0001_init.up.sql", "0002_phase3.up.sql",
		"0003_explain_audit.up.sql", "0004_replay.up.sql", "0005_cases_users.up.sql"} {
		sql, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	jwt := NewAuth(AuthConfig{Secret: "cases-test-secret", Issuer: "fade-test"})
	store := cases.NewStore(pool, cases.DefaultConfig())
	users := NewUserStore(pool)
	handler := NewRouter(Deps{
		Logger: logger, Metrics: obs.NewMetrics("cases-api-test"), Auth: jwt,
		Limiter: NewRateLimiter(nil), Cases: store, Users: users,
		PseudonymKey: "cases-test-pseudonym-key", Version: "test",
	})
	return &caseTestEnv{handler: handler, pool: pool, store: store}
}

func (e *caseTestEnv) seedUsers(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	users := NewUserStore(e.pool)
	if _, _, err := users.BootstrapAdmin(ctx, "admin-password-123"); err != nil {
		t.Fatal(err)
	}
	for _, u := range []struct{ name, role, password string }{
		{"analyst1", auth.RoleAnalyst, "analyst-password-123"},
		{"lead1", auth.RoleLead, "lead-password-123"},
	} {
		if _, err := users.CreateUser(ctx, u.name, u.name, u.role, u.password); err != nil {
			t.Fatal(err)
		}
	}
}

func (e *caseTestEnv) login(t *testing.T, username, password string) (access, refresh string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"username": username, "password": password})
	res := do(t, e.handler, http.MethodPost, "/v1/auth/login", "", body, nil)
	if res.Code != http.StatusOK {
		t.Fatalf("login %s: %d %s", username, res.Code, res.Body)
	}
	var session struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}
	if session.AccessToken == "" || session.RefreshToken == "" {
		t.Fatalf("login %s returned no tokens: %s", username, res.Body)
	}
	return session.AccessToken, session.RefreshToken
}

func TestCaseAPILifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	env := newCaseTestEnv(t)
	env.seedUsers(t)
	adminToken, adminRefresh := env.login(t, "admin", "admin-password-123")

	// The dashboard BFF needs the profile and scopes, not the tokens.
	me := do(t, env.handler, http.MethodGet, "/v1/auth/me", adminToken, nil, nil)
	if me.Code != http.StatusOK || !strings.Contains(me.Body.String(), `"role":"admin"`) {
		t.Fatalf("me: %d %s", me.Code, me.Body)
	}

	// Refresh rotates the token and revokes the old one.
	refreshBody, _ := json.Marshal(map[string]string{"refresh_token": adminRefresh})
	rotated := do(t, env.handler, http.MethodPost, "/v1/auth/refresh", "", refreshBody, nil)
	if rotated.Code != http.StatusOK {
		t.Fatalf("refresh: %d %s", rotated.Code, rotated.Body)
	}
	reused := do(t, env.handler, http.MethodPost, "/v1/auth/refresh", "", refreshBody, nil)
	if reused.Code != http.StatusUnauthorized {
		t.Fatalf("a rotated refresh token must be rejected, got %d", reused.Code)
	}

	analystToken, _ := env.login(t, "analyst1", "analyst-password-123")
	leadToken, _ := env.login(t, "lead1", "lead-password-123")

	// A mutating action without an Idempotency-Key is refused.
	createBody := []byte(`{"account_id":"ACC-SECRET-1","decision_class":"HOLD","risk_score":55,"amount_total":1200000}`)
	missingKey := do(t, env.handler, http.MethodPost, "/v1/cases", analystToken, createBody, nil)
	if missingKey.Code != http.StatusForbidden && missingKey.Code != http.StatusBadRequest {
		t.Fatalf("expected an idempotency or scope rejection, got %d %s", missingKey.Code, missingKey.Body)
	}

	created := do(t, env.handler, http.MethodPost, "/v1/cases", adminToken, createBody,
		map[string]string{"Idempotency-Key": "create-1"})
	if created.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", created.Code, created.Body)
	}
	if strings.Contains(created.Body.String(), "ACC-SECRET-1") {
		t.Fatalf("response leaked a raw account id: %s", created.Body)
	}
	var envelope struct {
		Case struct {
			ID         int64  `json:"id"`
			Version    int    `json:"version"`
			AccountRef string `json:"account_ref"`
			Status     string `json:"status"`
			Priority   int    `json:"priority"`
		} `json:"case"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(envelope.Case.AccountRef, "acct_") || envelope.Case.Status != "OPEN" {
		t.Fatalf("unexpected case: %+v", envelope.Case)
	}
	if envelope.Case.Priority <= 55 {
		t.Fatalf("amount bonus not applied: priority=%d", envelope.Case.Priority)
	}

	// Replaying the same key returns the same body without a second case.
	replay := do(t, env.handler, http.MethodPost, "/v1/cases", adminToken, createBody,
		map[string]string{"Idempotency-Key": "create-1"})
	if replay.Code != http.StatusCreated && replay.Code != http.StatusOK {
		t.Fatalf("replay: %d %s", replay.Code, replay.Body)
	}
	if !sameJSON(t, replay.Body.Bytes(), created.Body.Bytes()) {
		t.Fatalf("replay body differs:\n%s\n%s", replay.Body, created.Body)
	}

	id := envelope.Case.ID
	casePath := "/v1/cases/" + strconv.FormatInt(id, 10)

	// An analyst cannot assign or approve.
	if res := do(t, env.handler, http.MethodPost, casePath+"/assign", analystToken,
		[]byte(`{"assignee":"lead1"}`), map[string]string{"Idempotency-Key": "assign-1"}); res.Code != http.StatusForbidden {
		t.Fatalf("analyst assign: %d %s", res.Code, res.Body)
	}

	claim := do(t, env.handler, http.MethodPost, casePath+"/claim", analystToken, nil,
		map[string]string{"Idempotency-Key": "claim-1", "If-Match": strconv.Itoa(envelope.Case.Version)})
	if claim.Code != http.StatusOK {
		t.Fatalf("claim: %d %s", claim.Code, claim.Body)
	}
	var claimed struct {
		Case struct {
			Version int `json:"version"`
		} `json:"case"`
	}
	_ = json.Unmarshal(claim.Body.Bytes(), &claimed)

	// A stale version is a 409, not a silent overwrite.
	stale := do(t, env.handler, http.MethodPost, casePath+"/release", analystToken, nil,
		map[string]string{"Idempotency-Key": "release-stale", "If-Match": "1"})
	if stale.Code != http.StatusConflict {
		t.Fatalf("stale release: %d %s", stale.Code, stale.Body)
	}

	// Redacted content is rejected before it reaches the timeline.
	badNote := do(t, env.handler, http.MethodPost, casePath+"/notes", analystToken,
		[]byte(`{"text":"call 0788123456"}`), map[string]string{"Idempotency-Key": "note-bad"})
	if badNote.Code != http.StatusUnprocessableEntity {
		t.Fatalf("redacted note: %d %s", badNote.Code, badNote.Body)
	}
	goodNote := do(t, env.handler, http.MethodPost, casePath+"/notes", analystToken,
		[]byte(`{"text":"customer confirmed the transfer"}`), map[string]string{"Idempotency-Key": "note-good"})
	if goodNote.Code != http.StatusOK {
		t.Fatalf("note: %d %s", goodNote.Code, goodNote.Body)
	}

	// Adding a note bumps the version, so re-read it before resolving.
	fresh := do(t, env.handler, http.MethodGet, casePath, analystToken, nil, nil)
	var freshCase struct {
		Case struct {
			Version int `json:"version"`
		} `json:"case"`
	}
	if fresh.Code != http.StatusOK {
		t.Fatalf("reload: %d %s", fresh.Code, fresh.Body)
	}
	if err := json.Unmarshal(fresh.Body.Bytes(), &freshCase); err != nil {
		t.Fatal(err)
	}
	if freshCase.Case.Version <= claimed.Case.Version {
		t.Fatalf("note should have bumped the version, still %d", freshCase.Case.Version)
	}

	// Resolve below the four-eyes threshold: the label is published immediately.
	resolveBody, _ := json.Marshal(map[string]any{
		"outcome": "FRAUD", "reason_code": "confirmed_fraud", "version": freshCase.Case.Version,
	})
	resolved := do(t, env.handler, http.MethodPost, casePath+"/resolve", analystToken, resolveBody,
		map[string]string{"Idempotency-Key": "resolve-1"})
	if resolved.Code != http.StatusOK {
		t.Fatalf("resolve: %d %s", resolved.Code, resolved.Body)
	}
	var resolvedCase struct {
		Case struct {
			Status          string `json:"status"`
			PendingApproval bool   `json:"pending_approval"`
			Outcome         string `json:"outcome"`
		} `json:"case"`
	}
	_ = json.Unmarshal(resolved.Body.Bytes(), &resolvedCase)
	if resolvedCase.Case.Status != "RESOLVED" || resolvedCase.Case.PendingApproval {
		t.Fatalf("unexpected resolution: %+v", resolvedCase.Case)
	}

	// The label reached the feedback outbox.
	var labels int
	if err := env.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE topic='labels.feedback'`).Scan(&labels); err != nil {
		t.Fatal(err)
	}
	if labels != 1 {
		t.Fatalf("labels.feedback rows=%d", labels)
	}

	// Detail includes the timeline with pseudonymous actors.
	detail := do(t, env.handler, http.MethodGet, casePath, leadToken, nil, nil)
	if detail.Code != http.StatusOK {
		t.Fatalf("detail: %d %s", detail.Code, detail.Body)
	}
	if strings.Contains(detail.Body.String(), "analyst1") {
		t.Fatalf("timeline leaked a raw actor: %s", detail.Body)
	}
	for _, typ := range []string{"opened", "claimed", "note", "resolved"} {
		if !bytes.Contains(detail.Body.Bytes(), []byte(`"type":"`+typ+`"`)) {
			t.Fatalf("timeline missing %s: %s", typ, detail.Body)
		}
	}

	// The queue lists the case by pseudonym.
	queue := do(t, env.handler, http.MethodGet, "/v1/cases?status=RESOLVED", leadToken, nil, nil)
	if queue.Code != http.StatusOK || !strings.Contains(queue.Body.String(), envelope.Case.AccountRef) {
		t.Fatalf("queue: %d %s", queue.Code, queue.Body)
	}
}

func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	var left, right any
	if err := json.Unmarshal(a, &left); err != nil {
		t.Fatalf("decode %s: %v", a, err)
	}
	if err := json.Unmarshal(b, &right); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	return reflect.DeepEqual(left, right)
}

func TestCaseAPIRequiresAuthAndScopes(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	env := newCaseTestEnv(t)
	env.seedUsers(t)

	if res := do(t, env.handler, http.MethodGet, "/v1/cases", "", nil, nil); res.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous queue: %d", res.Code)
	}
	serviceToken, _ := env.login(t, "analyst1", "analyst-password-123")
	// An analyst holds no users:read, so user administration is forbidden.
	if res := do(t, env.handler, http.MethodGet, "/v1/users", serviceToken, nil, nil); res.Code != http.StatusForbidden {
		t.Fatalf("analyst users: %d %s", res.Code, res.Body)
	}
}
