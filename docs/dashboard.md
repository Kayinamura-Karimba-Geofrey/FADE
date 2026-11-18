# Analyst dashboard (Phase 7)

The dashboard is a Next.js app in `dashboard/` that gives analysts a queue, a
case workspace and label-quality metrics on top of the case API. It holds no
business logic of its own: every state change goes through
`POST /v1/cases/...` and is audited there.

- Owner: risk operations
- Stack: Node 22, Next.js 15 (App Router), React 19, TypeScript strict,
  Tailwind, TanStack Query v5, Recharts
- Contract: [api/openapi.yaml](../api/openapi.yaml) (version 0.7.0)
- Related: [cases.md](cases.md), [audit.md](audit.md)

## Why a backend-for-frontend

Browsers never receive a JWT. The dashboard runs a small BFF inside Next.js:

```
browser ──cookie──► Next route handler ──Bearer──► decision-api
```

| Piece | File | Responsibility |
| --- | --- | --- |
| Session helpers | `lib/session.ts` | Writes/clears the token cookies, refreshes a rotated pair |
| Login | `app/api/auth/login/route.ts` | Exchanges credentials for tokens, stores them server-side, returns only the profile |
| Logout | `app/api/auth/logout/route.ts` | Revokes the refresh session and clears cookies |
| Profile | `app/api/auth/me/route.ts` | Returns the signed-in profile and scopes |
| Proxy | `app/api/proxy/[...path]/route.ts` | Forwards API calls with the access token |
| Gate | `middleware.ts` | Redirects requests without a session cookie to `/login` |

Cookie rules (`lib/session.ts`):

- `fade_access` and `fade_refresh` are `httpOnly`, `SameSite=Strict`, `path=/`
  and `Secure` whenever `NODE_ENV=production`.
- Access cookies live 15 minutes; refresh cookies 12 hours, matching the API's
  `TokenAccessTTL`/`TokenRefreshTTL` defaults.
- The proxy strips inbound `Cookie` and `Authorization` headers before calling
  the API, so a browser-supplied header can never override the session.

Refresh handling: when the API answers `401`, the proxy rotates the refresh
token once (`/v1/auth/refresh`) and retries the original request. The API
revokes a refresh token on first use, so a stolen token is useless after the
legitimate client refreshes.

## Pseudonymous identifiers

`caseView` in `internal/api/cases.go` blanks `account_id`, `assignee` and
`primary_decision_id` and adds `account_ref`, `assignee_ref`, `decision_ref`
and `linked_refs` (HMAC-derived, e.g. `acct_8af4104602`). Timeline actors are
hashed the same way (`usr_...`). The dashboard only renders those references:
analysts correlate with the core banking system through a separate, audited
lookup.

Searching by raw account number is deliberately unsupported in the UI.

## Screens

| Route | Contents |
| --- | --- |
| `/login` | Username/password form; posts to the BFF, never to the API |
| `/queue` | Filterable queue (status, assignee, limit) with priority, SLA state and age |
| `/cases/[id]` | Case detail, timeline and actions: claim, release, escalate, note, resolve, approve, reopen |
| `/metrics` | Queue counters and outcome mix, polled every 30 s |

`/` redirects to `/queue`.

Every mutating action sends a fresh `Idempotency-Key` (`crypto.randomUUID()`)
and, where the API uses optimistic locking, the case `version` as `If-Match`.
A `VERSION_CONFLICT` response is surfaced as "the case changed, reload" rather
than retried blindly.

## Accessibility

- Skip link in `app/layout.tsx`, one `h1` per page, landmarks (`header`, `nav`,
  `main`).
- Actions are real `<button>`/`<form>` elements, so they work with the keyboard
  and with screen readers; status messages use `role="status"` / `role="alert"`.
- Colour is never the only signal: SLA state and status are rendered as text.

## Running locally

```bash
# API (from the repository root)
make migrate
go run ./cmd/decision-api        # needs DATABASE_URL, AUTH_SECRET, BOOTSTRAP_ADMIN_PASSWORD

# Case worker (opens cases and enforces SLA)
go run ./cmd/case-worker

# Dashboard
cd dashboard
npm ci
API_BASE_URL=http://localhost:8080 npm run dev
```

Environment:

| Variable | Default | Purpose |
| --- | --- | --- |
| `API_BASE_URL` | `http://localhost:8080` | Where the BFF forwards requests |
| `NODE_ENV` | – | `production` turns on `Secure` cookies |

The first sign-in uses the admin created from `BOOTSTRAP_ADMIN_PASSWORD`
(only when the `users` table is empty). Create analyst and lead accounts with
`POST /v1/users`; a lead is required for four-eyes approvals and reopenings.

## Checks

- `cd dashboard && npm run build` — type-check, lint and prerender.
- `make cases-test` — case store, auth, case-worker and case API integration
  tests (testcontainers).
- `make privscan` — fails if a raw account number, phone number or email leaks
  into source or documentation.

## Known limits

- The sidebar shows a sign-out control on `/login` too; it is harmless but
  could be hidden in a future pass.
- `/queue` polls through TanStack Query only on user interaction; there is no
  websocket push, so a new case appears on the next refresh.
- Metrics are computed from `cases` and `label_history`; there is no historical
  time series, so the dashboard does not chart trends yet.
