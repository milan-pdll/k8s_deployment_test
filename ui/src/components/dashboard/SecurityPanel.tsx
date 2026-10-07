import { ShieldAlert, ShieldCheck } from "lucide-react";
import { Panel } from "@/components/dashboard/Section";
import { formatFullDate, formatNumber, timeAgo } from "@/components/dashboard/format";
import type { AdminSummary } from "@/lib/api/types";

// Errors the services reported in the last 24 hours, and files the virus scanner blocked.
export function SecurityPanel({
  errors,
  quarantine,
  now,
}: {
  errors: AdminSummary["errors_last_24h"];
  quarantine: AdminSummary["quarantine"];
  now: number;
}) {
  const levels = [
    { label: "Warnings", value: errors.WARN, className: "text-amber-700 dark:text-amber-400", hint: "Worth a look" },
    { label: "Errors", value: errors.ERROR, className: "text-rose-700 dark:text-rose-400", hint: "Something failed" },
    { label: "Fatal", value: errors.FATAL, className: "text-rose-800 dark:text-rose-300", hint: "A service stopped" },
  ];

  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <Panel className="p-4 sm:p-5">
        <p className="text-sm font-medium text-slate-700 dark:text-slate-300">Errors reported in the last 24 hours</p>
        <dl className="mt-3 grid grid-cols-3 gap-3">
          {levels.map((l) => (
            <div key={l.label} className="rounded-lg bg-slate-50 px-3 py-2 dark:bg-slate-800/60">
              <dt className="text-xs text-slate-600 dark:text-slate-400">{l.label}</dt>
              <dd className={`mt-0.5 text-2xl font-semibold tabular-nums ${l.value > 0 ? l.className : "text-slate-900 dark:text-slate-100"}`}>
                {formatNumber(l.value)}
              </dd>
              <dd className="text-[11px] text-slate-500 dark:text-slate-400">{l.hint}</dd>
            </div>
          ))}
        </dl>
      </Panel>

      <Panel className="p-4 sm:p-5">
        <div className="flex items-center gap-2 text-sm font-medium">
          {quarantine.count > 0 ? (
            <>
              <ShieldAlert className="h-4 w-4 text-rose-600 dark:text-rose-400" aria-hidden />
              <span className="text-slate-700 dark:text-slate-300">Unsafe files blocked</span>
            </>
          ) : (
            <>
              <ShieldCheck className="h-4 w-4 text-emerald-600 dark:text-emerald-400" aria-hidden />
              <span className="text-slate-700 dark:text-slate-300">No unsafe files found</span>
            </>
          )}
        </div>
        <p className="mt-2 text-3xl font-semibold tabular-nums text-slate-900 dark:text-slate-50">
          {formatNumber(quarantine.count)}
          <span className="ml-2 text-sm font-normal text-slate-500 dark:text-slate-400">
            files in quarantine ({quarantine.size_mb.toFixed(1)} MB)
          </span>
        </p>
        <dl className="mt-3 space-y-1 text-xs text-slate-600 dark:text-slate-400">
          <div className="flex flex-wrap gap-1">
            <dt>Latest threat:</dt>
            <dd className="font-mono text-slate-800 dark:text-slate-200">{quarantine.latest_threat_detected ?? "none"}</dd>
          </div>
          <div className="flex flex-wrap gap-1">
            <dt>Last scan:</dt>
            <dd>
              {quarantine.latest_scanned_at ? (
                <time dateTime={quarantine.latest_scanned_at} title={formatFullDate(quarantine.latest_scanned_at)}>
                  {timeAgo(quarantine.latest_scanned_at, now)}
                </time>
              ) : (
                "no scans yet"
              )}
            </dd>
          </div>
        </dl>
        <p className="mt-3 text-xs leading-relaxed text-slate-500 dark:text-slate-400">
          ClamAV scans every downloaded file before it is processed.
        </p>
      </Panel>
    </div>
  );
}
