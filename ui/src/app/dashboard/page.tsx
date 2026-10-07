import { AlertTriangle, Database, Globe2, Search, ShieldAlert, Timer } from "lucide-react";
import { StatCard } from "@/components/dashboard/StatCard";
import { apiFetch } from "@/lib/api/server";
import { isAdminSummary } from "@/lib/api/types";
import { getSessionToken } from "@/lib/auth/session";

function formatBytes(bytes: number): string {
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3) return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
}

export default async function DashboardPage() {
  const token = await getSessionToken();
  const result = await apiFetch("/api/v1/admin/summary", isAdminSummary, { token: token ?? undefined });

  if (!result.ok) {
    return (
      <p role="alert" className="rounded-lg bg-rose-50 px-4 py-3 text-sm text-rose-700 dark:bg-rose-500/10 dark:text-rose-300">
        The dashboard summary is unavailable: {result.error}
      </p>
    );
  }

  const summary = result.data;
  const traffic = summary.search_traffic;
  const errors = summary.errors_last_24h;
  return (
    <div className="space-y-6">
      <p className="text-xs text-slate-500 dark:text-slate-400">
        As of {new Date(summary.timestamp).toISOString().replace("T", " ").slice(0, 19)} UTC
      </p>
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-3">
        <StatCard label="Websites registered" value={summary.domains.total_registered} icon={Globe2} tone="blue" />
        <StatCard label="Pages stored" value={summary.storage.total_raw_files} icon={Database} tone="emerald" />
        <StatCard label="Pages waiting for the ETL" value={summary.storage.unprocessed_files} icon={Timer} tone="amber" />
        <StatCard label="Searches (last 24 h)" value={traffic.searches} icon={Search} tone="blue" />
        <StatCard
          label="Search p95 latency"
          value={traffic.p95_latency_ms === null ? "–" : Math.round(traffic.p95_latency_ms)}
          unit={traffic.p95_latency_ms === null ? undefined : "ms"}
          icon={Timer}
          tone="emerald"
        />
        <StatCard label="Quarantined files" value={summary.quarantine.count} icon={ShieldAlert} tone="rose" />
      </div>

      <section className="rounded-xl border border-slate-200 bg-white p-4 text-sm dark:border-slate-800 dark:bg-slate-900">
        <h2 className="mb-3 flex items-center gap-2 font-semibold text-slate-900 dark:text-slate-100">
          <AlertTriangle className="h-4 w-4 text-amber-500" />
          Errors reported in the last 24 hours
        </h2>
        <dl className="grid grid-cols-3 gap-4 text-slate-600 dark:text-slate-300">
          <div><dt className="text-xs">Warnings</dt><dd className="text-lg">{errors.WARN}</dd></div>
          <div><dt className="text-xs">Errors</dt><dd className="text-lg">{errors.ERROR}</dd></div>
          <div><dt className="text-xs">Fatal</dt><dd className="text-lg">{errors.FATAL}</dd></div>
        </dl>
        <p className="mt-3 text-xs text-slate-500 dark:text-slate-400">
          Storage used: {formatBytes(summary.storage.total_storage_used_bytes)} · zero-result
          searches: {(traffic.zero_result_rate * 100).toFixed(1)}%
        </p>
      </section>
    </div>
  );
}
