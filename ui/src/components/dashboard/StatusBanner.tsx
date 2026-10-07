import { CircleAlert, CircleCheck, CircleX } from "lucide-react";
import { cn } from "@/lib/cn";
import type { ApiResult } from "@/lib/api/server";
import type { ReadinessReport } from "@/lib/api/types";

// One sentence anyone can read, from the API's readiness check (GET /health/ready):
// is the system OK, and if not, what does it mean for people using search?
export function StatusBanner({ readiness }: { readiness: ApiResult<ReadinessReport> }) {
  let state: "ok" | "degraded" | "down";
  let title: string;
  let body: string;

  if (readiness.ok && readiness.data.status === "ok") {
    state = "ok";
    title = "All systems are running normally";
    body = "The database and the search engine are both healthy.";
  } else if (readiness.ok) {
    // "degraded": the API works but search is limited (e.g. the search engine is down).
    state = "degraded";
    title = "Search is running with reduced quality";
    body = readiness.data.checks.search.detail || "The search engine reported a problem.";
  } else if (readiness.status === 503) {
    state = "down";
    title = "Some system checks are failing";
    body = "The API is up, but the database or search engine is not ready. New pages may not appear in results.";
  } else {
    state = "down";
    title = "The health check could not be reached";
    body = readiness.error;
  }

  const look = {
    ok: {
      icon: CircleCheck,
      box: "border-emerald-200 bg-emerald-50 text-emerald-900 dark:border-emerald-500/30 dark:bg-emerald-500/10 dark:text-emerald-100",
      iconClass: "text-emerald-600 dark:text-emerald-400",
    },
    degraded: {
      icon: CircleAlert,
      box: "border-amber-200 bg-amber-50 text-amber-950 dark:border-amber-500/30 dark:bg-amber-500/10 dark:text-amber-100",
      iconClass: "text-amber-600 dark:text-amber-400",
    },
    down: {
      icon: CircleX,
      box: "border-rose-200 bg-rose-50 text-rose-950 dark:border-rose-500/30 dark:bg-rose-500/10 dark:text-rose-100",
      iconClass: "text-rose-600 dark:text-rose-400",
    },
  }[state];
  const Icon = look.icon;

  return (
    <div role="status" className={cn("flex items-start gap-3 rounded-xl border p-4", look.box)}>
      <Icon className={cn("mt-0.5 h-5 w-5 shrink-0", look.iconClass)} aria-hidden />
      <div className="min-w-0">
        <p className="font-medium">{title}</p>
        <p className="mt-0.5 text-sm opacity-80">{body}</p>
      </div>
    </div>
  );
}
