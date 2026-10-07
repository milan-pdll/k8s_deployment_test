import { Panel } from "@/components/dashboard/Section";
import { formatNumber, percent } from "@/components/dashboard/format";
import type { AdminSummary } from "@/lib/api/types";

// How the registered websites are doing, as one bar and a legend with counts.
export function WebsiteStatus({ domains }: { domains: AdminSummary["domains"] }) {
  const rows = [
    { label: "Crawling now", value: domains.active_crawling, className: "bg-blue-500", hint: "Being downloaded right now" },
    { label: "Done", value: domains.completed, className: "bg-emerald-500", hint: "Crawled successfully" },
    { label: "Waiting", value: domains.pending, className: "bg-slate-400", hint: "Queued for the next crawl" },
    { label: "Paused", value: domains.paused, className: "bg-amber-400", hint: "Paused by an admin" },
    { label: "Failed or blocked", value: domains.failed_or_blocked, className: "bg-rose-500", hint: "Couldn't be crawled" },
  ];
  const total = domains.total_registered;

  return (
    <Panel className="p-4 sm:p-5">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <p className="text-sm font-medium text-slate-700 dark:text-slate-300">
          {formatNumber(total)} registered websites
        </p>
        {domains.failed_or_blocked > 0 && (
          <p className="text-sm text-rose-700 dark:text-rose-400">
            {percent(domains.failed_or_blocked, total)}% failed or blocked
          </p>
        )}
      </div>

      {total > 0 && (
        <div
          className="mt-3 flex h-3 overflow-hidden rounded-full bg-slate-100 dark:bg-slate-800"
          role="img"
          aria-label={rows.map((r) => `${r.label}: ${formatNumber(r.value)}`).join(", ")}
        >
          {rows.map((r) => (
            <div key={r.label} className={r.className} style={{ width: `${(r.value / total) * 100}%`, minWidth: r.value > 0 ? 4 : 0 }} />
          ))}
        </div>
      )}

      <dl className="mt-4 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
        {rows.map((r) => (
          <div key={r.label} className="rounded-lg bg-slate-50 px-3 py-2 dark:bg-slate-800/60">
            <dt className="flex items-center gap-1.5 text-xs text-slate-600 dark:text-slate-400">
              <span className={`h-2 w-2 shrink-0 rounded-full ${r.className}`} aria-hidden />
              {r.label}
            </dt>
            <dd className="mt-0.5 text-lg font-semibold tabular-nums text-slate-900 dark:text-slate-100">
              {formatNumber(r.value)}
              <span className="ml-1 text-xs font-normal text-slate-500 dark:text-slate-400">{percent(r.value, total)}%</span>
            </dd>
            <dd className="text-[11px] text-slate-500 dark:text-slate-400">{r.hint}</dd>
          </div>
        ))}
      </dl>
    </Panel>
  );
}
