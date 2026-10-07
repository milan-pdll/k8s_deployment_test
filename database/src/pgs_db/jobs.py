"""Scheduled maintenance: `python -m pgs_db.jobs <job> [...]`.

Run from cron, Airflow or a Kubernetes CronJob, as the `pgs_jobs` role. Each job runs
in its own transaction under a Postgres advisory lock, so two schedulers overlapping
never run the same job twice at once (the second one skips and says so). Prints one
JSON line per job; exits non-zero if any job failed.

| Job | What it does | Suggested schedule |
|---|---|---|
| `ingest` | load the scraper's new S3 objects into Bronze (`pgs_db.ingest`) | every 5 min |
| `stats` | rebuild `domain_stats` and `geo_content_stats` | every 15 min |
| `scores` | rebuild `page_scores` and domain authority | hourly |
| `reference` | link domains to local bodies, fill missing municipality contacts | daily |
| `release-stale` | free claims stuck longer than `--stale-minutes` | every 10 min |
| `purge` | drop error logs older than `--error-days`, searches older than `--search-days` | daily |
| `all` | every job above, in that order | -- |
"""

import argparse
import json
import sys
import time
import zlib
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from .ingest import S3Source, ingest
from .repositories import (
    OpsRepository,
    RankingRepository,
    ReferenceRepository,
    SearchLogRepository,
    SearchRepository,
    SilverRepository,
    StatsRepository,
)
from .session import make_session_factory

JobFn = Callable[[Session, argparse.Namespace], dict[str, Any]]


def _ingest(session: Session, _: argparse.Namespace) -> dict[str, Any]:
    source = S3Source.from_env()
    if source is None:
        return {"skipped": "PGS_S3_BUCKET not set"}
    # The loader commits per chunk, on its own sessions bound to this job's engine.
    report = ingest(sessionmaker(bind=session.get_bind(), expire_on_commit=False), source)
    return {
        "source": source.name,
        "runs_seen": report.runs_seen,
        "runs_finished": report.runs_finished,
        "documents_loaded": report.documents_loaded,
        "documents_failed": report.documents_failed,
    }


def _stats(session: Session, _: argparse.Namespace) -> dict[str, Any]:
    stats = StatsRepository(session)
    return {
        "domain_stats": stats.refresh_domain_stats(),
        "geo_content_stats": stats.refresh_geo_content_stats(),
    }


def _scores(session: Session, _: argparse.Namespace) -> dict[str, Any]:
    return RankingRepository(session).refresh_scores()


def _reference(session: Session, _: argparse.Namespace) -> dict[str, Any]:
    reference = ReferenceRepository(session)
    return {
        "domains_linked": reference.link_domains_to_local_bodies(),
        "contacts_filled": reference.fill_local_body_contacts(),
    }


def _release_stale(session: Session, args: argparse.Namespace) -> dict[str, Any]:
    window = timedelta(minutes=args.stale_minutes)
    return {
        "bronze_released": SilverRepository(session).release_stale(window),
        "indexing_released": SearchRepository(session).release_stale_indexing(window),
    }


def _purge(session: Session, args: argparse.Namespace) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "error_logs_deleted": OpsRepository(session).purge_errors(
            now - timedelta(days=args.error_days)
        ),
        "searches_deleted": SearchLogRepository(session).purge_before(
            now - timedelta(days=args.search_days)
        ),
    }


JOBS: dict[str, JobFn] = {
    "ingest": _ingest,
    "stats": _stats,
    "scores": _scores,
    "reference": _reference,
    "release-stale": _release_stale,
    "purge": _purge,
}


def run_job(session: Session, name: str, args: argparse.Namespace) -> dict[str, Any]:
    """Run one job in the caller's transaction, guarded by a transaction advisory lock."""
    lock_key = zlib.crc32(f"pgs_db.jobs.{name}".encode())
    if not session.scalar(select(func.pg_try_advisory_xact_lock(lock_key))):
        return {"job": name, "status": "skipped", "reason": "already running elsewhere"}
    started = time.monotonic()
    result = JOBS[name](session, args)
    return {
        "job": name,
        "status": "ok",
        "seconds": round(time.monotonic() - started, 3),
        "result": result,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m pgs_db.jobs", description=__doc__.split("\n")[0]
    )
    parser.add_argument("job", choices=[*JOBS, "all"])
    parser.add_argument("--stale-minutes", type=int, default=30)
    parser.add_argument("--error-days", type=int, default=90)
    parser.add_argument("--search-days", type=int, default=365)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    names = list(JOBS) if args.job == "all" else [args.job]
    session_factory = make_session_factory()
    failed = False
    for name in names:
        try:
            with session_factory.begin() as session:
                report = run_job(session, name, args)
        except Exception as exc:  # report and carry on with the next job
            failed = True
            report = {"job": name, "status": "failed", "error": f"{type(exc).__name__}: {exc}"}
        print(json.dumps(report, default=str), flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
