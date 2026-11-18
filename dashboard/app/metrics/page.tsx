"use client";

import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";

export default function MetricsPage() {
  const { data, isPending, error } = useQuery({
    queryKey: ["case-metrics"],
    queryFn: api.metrics,
    refetchInterval: 30_000,
  });

  if (isPending) return <p role="status">Loading metrics…</p>;
  if (error || !data) {
    return (
      <p role="alert" className="rounded border border-red-300 bg-red-50 p-3 text-sm text-red-800">
        Could not load metrics.
      </p>
    );
  }

  const cards = [
    { label: "Open", value: data.open },
    { label: "In review", value: data.in_review },
    { label: "Escalated", value: data.escalated },
    { label: "Awaiting approval", value: data.pending_approval },
    { label: "SLA breached", value: data.breached },
    { label: "Resolved today", value: data.resolved_today },
  ];

  return (
    <section aria-labelledby="metrics-heading" className="space-y-6">
      <h1 id="metrics-heading" className="text-2xl font-semibold">
        Queue metrics
      </h1>
      <ul className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        {cards.map((c) => (
          <li key={c.label} className="rounded border bg-white p-4">
            <p className="text-xs uppercase tracking-wide text-gray-500">{c.label}</p>
            <p className="mt-1 text-2xl font-semibold">{c.value}</p>
          </li>
        ))}
      </ul>
      <div className="grid gap-4 sm:grid-cols-2">
        <div className="rounded border bg-white p-4">
          <h2 className="mb-2 font-medium">Median time to resolution</h2>
          <p className="text-2xl font-semibold">
            {Math.round(data.median_resolution_minutes)} min
          </p>
          <p className="text-xs text-gray-500">Last 7 days of resolved cases.</p>
        </div>
        <div className="rounded border bg-white p-4">
          <h2 className="mb-2 font-medium">Outcomes</h2>
          <ul className="space-y-1 text-sm">
            {Object.entries(data.by_outcome).length === 0 && <li>No resolutions yet.</li>}
            {Object.entries(data.by_outcome).map(([outcome, n]) => (
              <li key={outcome} className="flex justify-between">
                <span>{outcome}</span>
                <span className="font-medium">{n}</span>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </section>
  );
}
