"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useParams } from "next/navigation";
import { useState } from "react";
import { ApiError, api, type Outcome } from "@/lib/api";

const REASONS = [
  "CONFIRMED_FRAUD",
  "CUSTOMER_CONTACTED",
  "FALSE_POSITIVE",
  "INSUFFICIENT_EVIDENCE",
  "DUPLICATE_CASE",
];

export default function CasePage() {
  const params = useParams<{ id: string }>();
  const id = Number(params.id);
  const queryClient = useQueryClient();
  const [note, setNote] = useState("");
  const [outcome, setOutcome] = useState<Outcome>("FRAUD");
  const [reason, setReason] = useState(REASONS[0]);
  const [followUp, setFollowUp] = useState("");
  const [message, setMessage] = useState<string | null>(null);

  const { data, isPending, error } = useQuery({
    queryKey: ["case", id],
    queryFn: () => api.case(id),
    enabled: Number.isFinite(id),
  });

  function onSuccess(text: string) {
    setMessage(text);
    setNote("");
    queryClient.invalidateQueries({ queryKey: ["case", id] });
    queryClient.invalidateQueries({ queryKey: ["cases"] });
  }

  function onFailure(err: unknown) {
    setMessage(err instanceof ApiError ? `${err.code}: ${err.message}` : "Action failed");
  }

  const claim = useMutation({
    mutationFn: () => api.claim(id, data?.case.version ?? 0),
    onSuccess: () => onSuccess("Case claimed."),
    onError: onFailure,
  });
  const release = useMutation({
    mutationFn: () => api.release(id, data?.case.version ?? 0),
    onSuccess: () => onSuccess("Case returned to the queue."),
    onError: onFailure,
  });
  const escalate = useMutation({
    mutationFn: () => api.escalate(id, data?.case.version ?? 0, "ANALYST_ESCALATION"),
    onSuccess: () => onSuccess("Case escalated."),
    onError: onFailure,
  });
  const addNote = useMutation({
    mutationFn: () => api.note(id, note),
    onSuccess: () => onSuccess("Note added."),
    onError: onFailure,
  });
  const resolve = useMutation({
    mutationFn: () =>
      api.resolve(id, data?.case.version ?? 0, {
        outcome,
        reason_code: reason,
        confidence: "MEDIUM",
        follow_up_at: followUp ? new Date(followUp).toISOString() : undefined,
      }),
    onSuccess: (res) =>
      onSuccess(
        res.case.pending_approval
          ? "Resolution recorded and awaiting a second reviewer."
          : "Case resolved and the label was published.",
      ),
    onError: onFailure,
  });
  const approve = useMutation({
    mutationFn: () => api.approve(id, data?.case.version ?? 0),
    onSuccess: () => onSuccess("Resolution approved."),
    onError: onFailure,
  });
  const reopen = useMutation({
    mutationFn: () => api.reopen(id, data?.case.version ?? 0, "REOPENED_BY_LEAD"),
    onSuccess: () => onSuccess("Case reopened."),
    onError: onFailure,
  });

  if (isPending) return <p role="status">Loading case…</p>;
  if (error || !data) {
    return (
      <p role="alert" className="rounded border border-red-300 bg-red-50 p-3 text-sm text-red-800">
        Could not load this case.
      </p>
    );
  }

  const c = data.case;
  const resolved = c.status === "RESOLVED";

  return (
    <section className="grid gap-6 lg:grid-cols-3" aria-labelledby="case-heading">
      <div className="space-y-4 lg:col-span-2">
        <header>
          <h1 id="case-heading" className="text-2xl font-semibold">
            Case #{c.id}
          </h1>
          <p className="text-sm text-gray-600">
            {c.account_ref} · {c.decision_class ?? c.source} · priority {c.priority} ·{" "}
            {c.status.replace("_", " ").toLowerCase()}
          </p>
        </header>

        {message && (
          <p role="status" className="rounded border bg-white p-3 text-sm">
            {message}
          </p>
        )}

        <dl className="grid grid-cols-2 gap-3 rounded border bg-white p-4 text-sm sm:grid-cols-4">
          <div>
            <dt className="text-gray-500">Amount</dt>
            <dd>{c.amount_total.toLocaleString()} RWF</dd>
          </div>
          <div>
            <dt className="text-gray-500">Risk score</dt>
            <dd>{c.risk_score}</dd>
          </div>
          <div>
            <dt className="text-gray-500">Linked decisions</dt>
            <dd>{c.linked_refs.length}</dd>
          </div>
          <div>
            <dt className="text-gray-500">Repeat account</dt>
            <dd>{c.repeat_account ? "Yes" : "No"}</dd>
          </div>
          <div>
            <dt className="text-gray-500">SLA</dt>
            <dd>{c.sla_state}</dd>
          </div>
          <div>
            <dt className="text-gray-500">Opened</dt>
            <dd>{new Date(c.opened_at).toLocaleString()}</dd>
          </div>
          <div>
            <dt className="text-gray-500">Assignee</dt>
            <dd className="font-mono text-xs">{c.assignee_ref || "Unassigned"}</dd>
          </div>
          <div>
            <dt className="text-gray-500">Version</dt>
            <dd>{c.version}</dd>
          </div>
        </dl>

        <div className="space-y-2 rounded border bg-white p-4">
          <h2 className="text-lg font-medium">Resolution</h2>
          <div className="flex flex-wrap items-end gap-3">
            <label className="text-sm">
              <span className="block text-gray-700">Outcome</span>
              <select
                className="rounded border px-2 py-1"
                value={outcome}
                onChange={(e) => setOutcome(e.target.value as Outcome)}
                disabled={resolved}
              >
                <option value="FRAUD">Fraud</option>
                <option value="LEGIT">Legitimate</option>
                <option value="INCONCLUSIVE">Inconclusive</option>
              </select>
            </label>
            <label className="text-sm">
              <span className="block text-gray-700">Reason</span>
              <select
                className="rounded border px-2 py-1"
                value={reason}
                onChange={(e) => setReason(e.target.value)}
                disabled={resolved}
              >
                {REASONS.map((r) => (
                  <option key={r} value={r}>
                    {r}
                  </option>
                ))}
              </select>
            </label>
            {outcome === "INCONCLUSIVE" && (
              <label className="text-sm">
                <span className="block text-gray-700">Follow up by</span>
                <input
                  type="date"
                  className="rounded border px-2 py-1"
                  value={followUp}
                  onChange={(e) => setFollowUp(e.target.value)}
                  required
                />
              </label>
            )}
            <button
              type="button"
              className="rounded bg-blue-600 px-3 py-1.5 text-sm text-white disabled:opacity-50"
              onClick={() => resolve.mutate()}
              disabled={resolved || resolve.isPending}
            >
              {resolve.isPending ? "Saving…" : "Record resolution"}
            </button>
          </div>
          {c.pending_approval && (
            <div className="rounded border border-amber-300 bg-amber-50 p-3 text-sm">
              <p>
                A second reviewer must approve this resolution before the label reaches the feedback
                loop.
              </p>
              <button
                type="button"
                className="mt-2 rounded border px-3 py-1 text-sm"
                onClick={() => approve.mutate()}
                disabled={approve.isPending}
              >
                Approve resolution
              </button>
            </div>
          )}
        </div>

        <div className="space-y-2 rounded border bg-white p-4">
          <h2 className="text-lg font-medium">Note</h2>
          <label className="block text-sm">
            <span className="sr-only">Case note</span>
            <textarea
              className="w-full rounded border px-2 py-1"
              rows={3}
              maxLength={2000}
              value={note}
              onChange={(e) => setNote(e.target.value)}
              placeholder="Notes must not contain phone numbers, emails or card numbers."
            />
          </label>
          <button
            type="button"
            className="rounded border px-3 py-1 text-sm disabled:opacity-50"
            onClick={() => addNote.mutate()}
            disabled={note.trim().length === 0 || addNote.isPending}
          >
            Add note
          </button>
        </div>

        <div className="rounded border bg-white p-4">
          <h2 className="mb-2 text-lg font-medium">Timeline</h2>
          <ol className="space-y-2 text-sm">
            {data.timeline.map((ev) => (
              <li key={ev.id} className="border-l-2 border-gray-200 pl-3">
                <p className="font-medium">{ev.type.replace(/_/g, " ")}</p>
                <p className="text-xs text-gray-600">
                  {new Date(ev.at).toLocaleString()} · {ev.actor} ({ev.actor_role})
                </p>
                {typeof ev.payload.text === "string" && <p className="mt-1">{ev.payload.text}</p>}
              </li>
            ))}
          </ol>
        </div>
      </div>

      <aside className="space-y-2 rounded border bg-white p-4 h-fit">
        <h2 className="text-lg font-medium">Actions</h2>
        <button
          type="button"
          className="w-full rounded border px-3 py-1.5 text-sm"
          onClick={() => claim.mutate()}
          disabled={(c.status !== "OPEN" && c.status !== "ESCALATED") || claim.isPending}
        >
          Claim case
        </button>
        <button
          type="button"
          className="w-full rounded border px-3 py-1.5 text-sm"
          onClick={() => release.mutate()}
          disabled={c.status !== "IN_REVIEW" || release.isPending}
        >
          Return to queue
        </button>
        <button
          type="button"
          className="w-full rounded border px-3 py-1.5 text-sm"
          onClick={() => escalate.mutate()}
          disabled={c.status !== "IN_REVIEW" || escalate.isPending}
        >
          Escalate
        </button>
        <button
          type="button"
          className="w-full rounded border px-3 py-1.5 text-sm"
          onClick={() => reopen.mutate()}
          disabled={!resolved || reopen.isPending}
        >
          Reopen case
        </button>
      </aside>
    </section>
  );
}
