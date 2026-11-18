"use client";

import { useQuery } from "@tanstack/react-query";
import Link from "next/link";
import { useState } from "react";
import { api, type CaseItem } from "@/lib/api";
import { useCaseEvents } from "@/lib/sse";

const STATUS_FILTERS = [
  { label: "All open", value: "OPEN,IN_REVIEW,ESCALATED,REOPENED" },
  { label: "Unclaimed", value: "OPEN" },
  { label: "In review", value: "IN_REVIEW" },
  { label: "Escalated", value: "ESCALATED" },
];

function slaBadge(c: CaseItem) {
  if (c.sla_state === "breached") {
    return <span className="rounded bg-red-100 px-2 py-0.5 text-xs text-red-800">Breached</span>;
  }
  if (c.sla_state === "paused") {
    return <span className="rounded bg-amber-100 px-2 py-0.5 text-xs text-amber-800">Paused</span>;
  }
  return <span className="rounded bg-gray-100 px-2 py-0.5 text-xs text-gray-700">On track</span>;
}

export default function QueuePage() {
  useCaseEvents();
  const [status, setStatus] = useState(STATUS_FILTERS[0].value);
  const { data, isPending, error } = useQuery({
    queryKey: ["cases", status],
    queryFn: () => api.queue({ status, limit: 100 }),
  });

  return (
    <section aria-labelledby="queue-heading" className="space-y-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 id="queue-heading" className="text-2xl font-semibold">
            Review queue
          </h1>
          <p className="text-sm text-gray-600">
            Ordered by priority, then oldest first. Account and decision references are pseudonyms.
          </p>
        </div>
        <label className="text-sm">
          <span className="mr-2 text-gray-700">Filter</span>
          <select
            className="rounded border px-2 py-1"
            value={status}
            onChange={(e) => setStatus(e.target.value)}
          >
            {STATUS_FILTERS.map((f) => (
              <option key={f.value} value={f.value}>
                {f.label}
              </option>
            ))}
          </select>
        </label>
      </div>

      {isPending && <p role="status">Loading queue…</p>}
      {error && (
        <p role="alert" className="rounded border border-red-300 bg-red-50 p-3 text-sm text-red-800">
          Could not load the queue: {error.message}
        </p>
      )}

      {data && data.cases.length === 0 && (
        <p className="rounded border bg-white p-6 text-center text-sm text-gray-600">
          No cases match this filter.
        </p>
      )}

      {data && data.cases.length > 0 && (
        <div className="overflow-x-auto rounded border bg-white">
          <table className="w-full text-sm">
            <caption className="sr-only">Cases awaiting analyst review</caption>
            <thead className="border-b bg-gray-50 text-left">
              <tr>
                <th scope="col" className="p-3">Priority</th>
                <th scope="col" className="p-3">Account</th>
                <th scope="col" className="p-3">Decision</th>
                <th scope="col" className="p-3">Amount</th>
                <th scope="col" className="p-3">Age</th>
                <th scope="col" className="p-3">Assignee</th>
                <th scope="col" className="p-3">SLA</th>
                <th scope="col" className="p-3">Source</th>
              </tr>
            </thead>
            <tbody>
              {data.cases.map((c) => (
                <tr key={c.id} className="border-b last:border-0 hover:bg-gray-50">
                  <td className="p-3 font-medium">
                    <Link className="hover:underline" href={`/cases/${c.id}`}>
                      {c.priority}
                    </Link>
                  </td>
                  <td className="p-3 font-mono text-xs">{c.account_ref}</td>
                  <td className="p-3">
                    <span className="rounded bg-slate-100 px-2 py-0.5 text-xs">
                      {c.decision_class ?? c.source}
                    </span>
                  </td>
                  <td className="p-3">{c.amount_total.toLocaleString()} RWF</td>
                  <td className="p-3">{c.age_minutes}m</td>				  <td className="p-3 font-mono text-xs">{c.assignee_ref || "—"}</td>
                  <td className="p-3">{slaBadge(c)}</td>
                  <td className="p-3 text-xs text-gray-600">{c.source}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
