import { ChevronRight, FileSearch, Globe, Link2, Search } from "lucide-react";
import { Panel } from "@/components/dashboard/Section";
import { formatBytes, formatCompact, formatNumber, percent, timeAgo } from "@/components/dashboard/format";
import type { AdminSummary } from "@/lib/api/types";

// The data pipeline as four plain steps: find links → download pages → scan and
// process → searchable. Each step shows how much is done and how much is waiting.
export function PipelineFlow({ summary, now }: { summary: AdminSummary; now: number }) {
  const { links, storage, domains } = summary;
  const waiting = storage.unprocessed_files + storage.processing_files;

  const steps = [
    {
      icon: Link2,
      title: "Find links",
      value: formatCompact(links.total_child_links_discovered),
      unit: "links found",
      details: [`On ${formatNumber(domains.total_registered)} registered websites`],
    },
    {
      icon: Globe,
      title: "Download pages",
      value: formatCompact(storage.total_raw_files),
      unit: "pages stored",
      details: [
        `${formatNumber(domains.active_crawling)} websites crawling now`,
        `${formatBytes(storage.total_storage_used_bytes)} used`,
      ],
    },
    {
      icon: FileSearch,
      title: "Scan & process",
      value: formatCompact(waiting),
      unit: "in the queue",
      details: [
        `${formatNumber(storage.processing_files)} processing now`,
        storage.oldest_unprocessed_at
          ? `Oldest waiting: ${timeAgo(storage.oldest_unprocessed_at, now)}`
          : "Nothing waiting",
      ],
    },
    {
      icon: Search,
      title: "Searchable",
      value: formatCompact(storage.processed_files),
      unit: "pages processed",
      details: [`${percent(storage.processed_files, storage.total_raw_files)}% of downloaded pages`],
    },
  ];

  const total = storage.total_raw_files;
  const bar = [
    { label: "Processed", value: storage.processed_files, className: "bg-emerald-500" },
    { label: "Processing", value: storage.processing_files, className: "bg-blue-500" },
    { label: "Waiting", value: storage.unprocessed_files, className: "bg-amber-400" },
    { label: "Failed", value: storage.failed_files, className: "bg-rose-500" },
    { label: "Quarantined", value: storage.quarantined_files, className: "bg-violet-500" },
  ];

  return (
    <Panel className="overflow-hidden">
      {/* 1px gaps over a grey background draw the lines between steps. */}
      <ol className="grid gap-px bg-slate-200 dark:bg-slate-800 sm:grid-cols-2 xl:grid-cols-4">
        {steps.map((step, i) => (
          <li key={step.title} className="relative bg-white p-4 dark:bg-slate-900 sm:p-5">
            <div className="flex items-center gap-2 text-sm font-medium text-slate-600 dark:text-slate-400">
              <span className="flex h-6 w-6 items-center justify-center rounded-full bg-slate-100 text-xs font-semibold text-slate-700 dark:bg-slate-800 dark:text-slate-300">
                {i + 1}
              </span>
              <step.icon className="h-4 w-4" aria-hidden />
              {step.title}
            </div>
            <p className="mt-3 text-2xl font-semibold tabular-nums tracking-tight text-slate-900 dark:text-slate-50">
              {step.value}
              <span className="ml-1.5 text-sm font-normal text-slate-500 dark:text-slate-400">{step.unit}</span>
            </p>
            <ul className="mt-2 space-y-1 text-xs text-slate-500 dark:text-slate-400">
              {step.details.map((detail) => (
                <li key={detail}>{detail}</li>
              ))}
            </ul>
            {i > 0 && (
              <ChevronRight
                aria-hidden
                className="absolute -left-3 top-1/2 z-10 hidden h-6 w-6 -translate-y-1/2 rounded-full border border-slate-200 bg-white p-1 text-slate-400 dark:border-slate-700 dark:bg-slate-900 xl:block"
              />
            )}
          </li>
        ))}
      </ol>

      <div className="border-t border-slate-200 p-4 dark:border-slate-800 sm:px-5">
        <div className="flex flex-wrap items-baseline justify-between gap-2 text-sm">
          <p className="font-medium text-slate-700 dark:text-slate-300">Stored pages by stage</p>
          <p className="text-slate-500 dark:text-slate-400">{formatNumber(total)} total</p>
        </div>
        {total > 0 ? (
          <div
            className="mt-2 flex h-2.5 overflow-hidden rounded-full bg-slate-100 dark:bg-slate-800"
            role="img"
            aria-label={bar.map((b) => `${b.label}: ${formatNumber(b.value)}`).join(", ")}
          >
            {bar.map((b) => (
              <div
                key={b.label}
                className={b.className}
                style={{ width: `${(b.value / total) * 100}%`, minWidth: b.value > 0 ? 4 : 0 }}
              />
            ))}
          </div>
        ) : (
          <p className="mt-2 text-sm text-slate-500 dark:text-slate-400">No pages stored yet.</p>
        )}
        <ul className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-600 dark:text-slate-400">
          {bar.map((b) => (
            <li key={b.label} className="flex items-center gap-1.5">
              <span className={`h-2 w-2 rounded-full ${b.className}`} aria-hidden />
              {b.label} <span className="tabular-nums text-slate-900 dark:text-slate-200">{formatNumber(b.value)}</span>
            </li>
          ))}
        </ul>
      </div>
    </Panel>
  );
}
