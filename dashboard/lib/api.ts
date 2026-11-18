/** Typed helpers over the BFF proxy. Never returns raw identifiers. */

export type CaseStatus = "OPEN" | "IN_REVIEW" | "ESCALATED" | "RESOLVED" | "REOPENED";
export type CaseSource = "DECISION" | "STEP_UP_FAILED" | "QA_SAMPLE" | "MANUAL";
export type Outcome = "FRAUD" | "LEGIT" | "INCONCLUSIVE";

export type CaseItem = {
  id: number;
  source: CaseSource;
  decision_class: string | null;	account_ref: string;
	assignee_ref: string;
	decision_ref: string;
  linked_refs: string[];
  risk_score: number;
  amount_total: number;
  priority: number;	status: CaseStatus;
  outcome: Outcome | null;
  reason_code: string | null;
  pending_approval: boolean;
  repeat_account: boolean;
  escalated: boolean;
  sla_due_at: string | null;
  sla_state: "ok" | "breached" | "paused" | "met" | "none";
  age_minutes: number;
  version: number;
  opened_at: string;
  resolved_at: string | null;
};

export type CaseEvent = {
  id: number;
  case_id: number;
  at: string;
  actor: string;
  actor_role: string;
  type: string;
  payload: Record<string, unknown>;
};

export type CaseDetail = { case: CaseItem; timeline: CaseEvent[] };

export type CaseMetrics = {
  open: number;
  in_review: number;
  escalated: number;
  pending_approval: number;
  breached: number;
  resolved_today: number;
  median_resolution_minutes: number;
  by_outcome: Record<string, number>;
  by_reason: Record<string, number>;
};

export class ApiError extends Error {
  status: number;
  code: string;
  constructor(status: number, code: string, detail: string) {
    super(detail);
    this.status = status;
    this.code = code;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api/proxy${path}`, {
    ...init,
    credentials: "include",
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
  });
  if (res.status === 204) return undefined as T;
  if (!res.ok) {
    let code = "REQUEST_FAILED";
    let detail = res.statusText;
    try {
      const body = (await res.json()) as { code?: string; detail?: string };
      code = body.code ?? code;
      detail = body.detail ?? detail;
    } catch {
      // keep the status text
    }
    throw new ApiError(res.status, code, detail);
  }
  return (await res.json()) as T;
}

/** Idempotency keys are generated per analyst action so a retry is safe. */
export function idempotencyKey(): string {
  return crypto.randomUUID();
}

export const api = {
  queue: (params: { status?: string; assignee?: string; limit?: number }) => {
    const q = new URLSearchParams();
    if (params.status) q.set("status", params.status);
    if (params.assignee) q.set("assignee", params.assignee);
    if (params.limit) q.set("limit", String(params.limit));
    return request<{ cases: CaseItem[]; total: number }>(`/v1/cases?${q.toString()}`);
  },
  metrics: () => request<CaseMetrics>("/v1/cases/metrics"),
  case: (id: number) => request<CaseDetail>(`/v1/cases/${id}`),
  claim: (id: number, version: number) =>
    request<{ case: CaseItem }>(`/v1/cases/${id}/claim`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey(), "If-Match": String(version) },
    }),
  release: (id: number, version: number) =>
    request<{ case: CaseItem }>(`/v1/cases/${id}/release`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey(), "If-Match": String(version) },
    }),
  escalate: (id: number, version: number, reason: string) =>
    request<{ case: CaseItem }>(`/v1/cases/${id}/escalate`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey() },
      body: JSON.stringify({ reason, version }),
    }),
  note: (id: number, text: string) =>
    request<{ event: CaseEvent }>(`/v1/cases/${id}/notes`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey() },
      body: JSON.stringify({ text }),
    }),
  resolve: (
    id: number,
    version: number,
    resolution: { outcome: Outcome; reason_code: string; confidence?: string; follow_up_at?: string },
  ) =>
    request<{ case: CaseItem }>(`/v1/cases/${id}/resolve`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey() },
      body: JSON.stringify({ ...resolution, version }),
    }),
  approve: (id: number, version: number) =>
    request<{ case: CaseItem }>(`/v1/cases/${id}/approve`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey(), "If-Match": String(version) },
    }),
  reopen: (id: number, version: number, reason: string) =>
    request<{ case: CaseItem }>(`/v1/cases/${id}/reopen`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey() },
      body: JSON.stringify({ reason, version }),
    }),
};
