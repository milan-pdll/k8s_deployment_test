import { CircleX, Database, FileStack, Globe, HardDrive } from "lucide-react";
import { Section } from "@/components/dashboard/Section";
import { StatusBanner } from "@/components/dashboard/StatusBanner";
import { StatCard } from "@/components/dashboard/StatCard";
import { RefreshButton } from "@/components/dashboard/RefreshButton";
import { PipelineFlow } from "@/components/dashboard/PipelineFlow";
import { WebsiteStatus } from "@/components/dashboard/WebsiteStatus";
import { SearchTrafficPanel } from "@/components/dashboard/SearchTrafficPanel";
import { SecurityPanel } from "@/components/dashboard/SecurityPanel";
import { formatBytes, formatClock, formatCompact, formatNumber, timeAgo } from "@/components/dashboard/format";
import { apiFetch } from "@/lib/api/server";
import { isAdminSummary, isReadinessReport } from "@/lib/api/types";
import { getSessionToken } from "@/lib/auth/session";

// Everything on this page comes from the API: GET /api/v1/admin/summary for the
// numbers and GET /health/ready for the status banner, fetched together.
async function loadDashboard() {
  const token = await getSessionToken();
  const [summary, readiness] = await Promise.all([
    apiFetch("/api/v1/admin/summary", isAdminSummary, { token: token ?? undefined }),
    apiFetch("/health/ready", isReadinessReport),
  ]);
  return { summary, readiness, now: Date.now() };
}

export default async function DashboardPage() {
  const { summary: result, readiness, now } = await loadDashboard();

  if (!result.ok) {
    return (
      <div className="mx-auto max-w-xl py-12 text-center">
        <CircleX className="mx-auto h-10 w-10 text-rose-500" aria-hidden />
        <h1 className="mt-4 text-xl font-semibold text-slate-900 dark:text-slate-50">The dashboard couldn&rsquo;t load</h1>
        <p role="alert" className="mt-2 text-sm text-slate-600 dark:text-slate-400">
          {result.error}
        </p>
        <div className="mt-6 flex justify-center">
          <RefreshButton />
        </div>
      </div>
    );
  }

  const summary = result.data;
  const { domains, storage } = summary;
  const waiting = storage.unprocessed_files + storage.processing_files;

  return (
    <div className="space-y-10">
      <Section
        id="overview"
        title="Overview"
        description={`How the search engine is doing. Updated ${formatClock(summary.timestamp)} NPT.`}
        action={<RefreshButton />}
      >
        <div className="space-y-4">
          <StatusBanner readiness={readiness} />
          <div className="grid grid-cols-2 gap-3 sm:gap-4 xl:grid-cols-4">
            <StatCard
              label="Websites tracked"
              value={formatNumber(domains.total_registered)}
              hint={`${formatNumber(domains.active_crawling)} crawling now · ${formatNumber(domains.failed_or_blocked)} failed or blocked`}
              icon={Globe}
              tone="blue"
            />
            <StatCard
              label="Pages processed"
              value={formatCompact(storage.processed_files)}
              hint="Ready for search"
              icon={FileStack}
              tone="emerald"
            />
            <StatCard
              label="Waiting to process"
              value={formatCompact(waiting)}
              hint={
                storage.oldest_unprocessed_at
                  ? `Oldest waiting ${timeAgo(storage.oldest_unprocessed_at, now)}`
                  : "The queue is empty"
              }
              icon={Database}
              tone="amber"
            />
            <StatCard
              label="Storage used"
              value={formatBytes(storage.total_storage_used_bytes)}
              hint={`${formatCompact(storage.total_raw_files)} pages stored`}
              icon={HardDrive}
              tone="violet"
            />
          </div>
        </div>
      </Section>

      <Section id="pipeline" title="Pipeline" description="How a web page becomes a search result, and how much is waiting at each step.">
        <PipelineFlow summary={summary} now={now} />
      </Section>

      <Section id="websites" title="Websites" description="Where each registered website is in the crawl.">
        <WebsiteStatus domains={domains} />
      </Section>

      <Section id="search" title="Search traffic" description="How people are using search.">
        <SearchTrafficPanel traffic={summary.search_traffic} />
      </Section>

      <Section id="security" title="Errors & security" description="Problems the services reported, and files the virus scanner blocked.">
        <SecurityPanel errors={summary.errors_last_24h} quarantine={summary.quarantine} now={now} />
      </Section>
    </div>
  );
}
