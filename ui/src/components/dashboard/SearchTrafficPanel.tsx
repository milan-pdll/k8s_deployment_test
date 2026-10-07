import { Panel } from "@/components/dashboard/Section";
import { formatFullDate, formatNumber, formatRate } from "@/components/dashboard/format";
import type { SearchTraffic } from "@/lib/api/types";

// How people are using search, over the window the API reports (since → until).
export function SearchTrafficPanel({ traffic }: { traffic: SearchTraffic }) {
  const ms = (value: number | null) => (value === null ? "–" : `${Math.round(value)} ms`);
  const metrics = [
    { label: "Searches", value: formatNumber(traffic.searches), hint: `${traffic.queries_per_second.toFixed(2)} per second` },
    { label: "Average response time", value: ms(traffic.avg_latency_ms), hint: "How long a search takes" },
    { label: "Slowest 5% (p95)", value: ms(traffic.p95_latency_ms), hint: "95% of searches are faster" },
    {
      label: "No results",
      value: formatRate(traffic.zero_result_rate),
      hint: "Searches that found nothing",
      warn: traffic.zero_result_rate >= 0.2,
    },
    { label: "Clicked a result", value: formatRate(traffic.click_through_rate), hint: "Searches followed by a click" },
  ];

  return (
    <Panel className="p-4 sm:p-5">
      <p className="text-xs text-slate-500 dark:text-slate-400">
        From {formatFullDate(traffic.since)} to {formatFullDate(traffic.until)}
      </p>
      <dl className="mt-3 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
        {metrics.map((m) => (
          <div key={m.label} className="rounded-lg bg-slate-50 px-3 py-2 dark:bg-slate-800/60">
            <dt className="text-xs text-slate-600 dark:text-slate-400">{m.label}</dt>
            <dd
              className={`mt-0.5 text-lg font-semibold tabular-nums ${
                m.warn ? "text-amber-700 dark:text-amber-400" : "text-slate-900 dark:text-slate-100"
              }`}
            >
              {m.value}
            </dd>
            <dd className="text-[11px] text-slate-500 dark:text-slate-400">{m.hint}</dd>
          </div>
        ))}
      </dl>
    </Panel>
  );
}
