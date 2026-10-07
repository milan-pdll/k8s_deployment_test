// Shown while the dashboard waits for the API (Next.js route loading UI).
export default function DashboardLoading() {
  return (
    <div className="animate-pulse space-y-10" role="status" aria-label="Loading dashboard">
      <div className="space-y-4">
        <div className="h-6 w-56 rounded bg-slate-200 dark:bg-slate-800" />
        <div className="h-16 rounded-xl bg-slate-200 dark:bg-slate-800" />
        <div className="grid grid-cols-2 gap-3 sm:gap-4 xl:grid-cols-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="h-28 rounded-xl bg-slate-200 dark:bg-slate-800" />
          ))}
        </div>
      </div>
      <div className="h-56 rounded-xl bg-slate-200 dark:bg-slate-800" />
      <div className="h-40 rounded-xl bg-slate-200 dark:bg-slate-800" />
    </div>
  );
}
