import { cn } from "@/lib/cn";
import type { LucideIcon } from "lucide-react";

const TONES = {
  blue: "bg-blue-50 text-blue-600 dark:bg-blue-500/10 dark:text-blue-400",
  emerald: "bg-emerald-50 text-emerald-600 dark:bg-emerald-500/10 dark:text-emerald-400",
  amber: "bg-amber-50 text-amber-600 dark:bg-amber-500/10 dark:text-amber-400",
  violet: "bg-violet-50 text-violet-600 dark:bg-violet-500/10 dark:text-violet-400",
  rose: "bg-rose-50 text-rose-600 dark:bg-rose-500/10 dark:text-rose-400",
};

/** One headline number, with a plain-language line explaining it. */
export function StatCard({
  label,
  value,
  hint,
  icon: Icon,
  tone = "blue",
}: {
  label: string;
  value: string;
  hint?: string;
  icon: LucideIcon;
  tone?: keyof typeof TONES;
}) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-3 dark:border-slate-800 dark:bg-slate-900 sm:p-4">
      <div className="flex items-start justify-between gap-3">
        <p className="text-sm font-medium text-slate-600 dark:text-slate-400">{label}</p>
        <span className={cn("hidden h-8 w-8 shrink-0 items-center justify-center rounded-lg min-[420px]:flex", TONES[tone])}>
          <Icon className="h-4 w-4" aria-hidden />
        </span>
      </div>
      <p className="mt-1 text-xl font-semibold tabular-nums tracking-tight text-slate-900 dark:text-slate-50 sm:text-2xl">{value}</p>
      {hint && <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">{hint}</p>}
    </div>
  );
}
