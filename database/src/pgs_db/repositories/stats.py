"""Gold summaries (`domain_stats`, `geo_content_stats`) and the admin dashboard reads.

`refresh_domain_stats` and `refresh_geo_content_stats` rebuild Gold from Bronze and
Silver; `python -m pgs_db.jobs` runs them on a schedule. Everything else here is a read
for the API:

- `GET /api/v1/user/geo/...` map counts -> `geo_content`
- `GET /api/v1/admin/domains` -> `list_domains`
- `POST /api/v1/admin/domains/{domain}/action` -> `set_domain_status`
- `GET /api/v1/admin/storage/metrics` -> `storage_metrics`
- `GET /api/v1/admin/dashboard/summary` -> `dashboard_summary`

The dashboard's infrastructure block (k8s nodes, Spark executors, Kafka brokers) and
the scraper's queue counts (`queued_for_crawl`, `filtered_by_bloom`) are not in the
database; the API gets those from the services themselves.
"""

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import BigInteger, bindparam, case, func, literal, or_, select, text, union_all
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Session

from ..enums import DomainPriority, DomainStatus, ProcessingStatus
from ..models import (
    CrawledDocument,
    District,
    Domain,
    DomainStats,
    GeoContentStats,
    LocalBody,
    StoredFile,
)
from .ops import OpsRepository
from .quarantine import QuarantineRepository

# One statement rebuilds every requested row. A URL's outcome is its latest fetch.
_REFRESH_DOMAIN_STATS = text(
    """
    WITH scope AS (
        SELECT id AS domain_id FROM domains
        WHERE CAST(:domain_ids AS bigint[]) IS NULL OR id = ANY(CAST(:domain_ids AS bigint[]))
    ),
    latest AS (
        SELECT DISTINCT ON (cd.normalized_url)
               cd.domain_id, cd.status_code, cd.error, cd.fetched_at
        FROM crawled_documents cd JOIN scope s ON s.domain_id = cd.domain_id
        ORDER BY cd.normalized_url, cd.fetched_at DESC, cd.id DESC
    ),
    fetches AS (
        SELECT domain_id,
               count(*) FILTER (WHERE error IS NULL AND coalesce(status_code, 0) < 400) AS scraped,
               count(*) FILTER (WHERE error IS NOT NULL OR status_code >= 400) AS failed,
               max(fetched_at) AS last_fetched_at
        FROM latest GROUP BY domain_id
    ),
    links AS (
        -- internal_links when the scraper splits them out; otherwise the same-host
        -- entries of links (the Temporal scraper sends only links).
        SELECT cd.domain_id, count(DISTINCT link) AS n
        FROM crawled_documents cd
        JOIN scope s ON s.domain_id = cd.domain_id
        CROSS JOIN LATERAL unnest(
            CASE WHEN cardinality(cd.internal_links) > 0 THEN cd.internal_links
            ELSE ARRAY(
                SELECT l FROM unnest(cd.links) AS l
                WHERE lower(regexp_replace(
                          substring(l from '^[a-zA-Z][a-zA-Z0-9+.-]*://([^/?#]+)'),
                          '^www\\.|:[0-9]+$', '', 'g'))
                      = lower(regexp_replace(cd.host, '^www\\.', ''))
            ) END
        ) AS link
        GROUP BY cd.domain_id
    ),
    silver AS (
        SELECT p.domain_id, count(*) AS n
        FROM pages p JOIN scope s ON s.domain_id = p.domain_id
        WHERE p.duplicate_of_id IS NULL
        GROUP BY p.domain_id
    ),
    quarantine AS (
        SELECT q.domain_id, count(*) AS n
        FROM quarantined_files q JOIN scope s ON s.domain_id = q.domain_id
        WHERE q.status = 'QUARANTINED'
        GROUP BY q.domain_id
    )
    INSERT INTO domain_stats (
        domain_id, discovered_links, scraped_pages, failed_pages, page_count,
        quarantined_count, last_fetched_at, computed_at
    )
    SELECT s.domain_id,
           coalesce(links.n, 0), coalesce(fetches.scraped, 0), coalesce(fetches.failed, 0),
           coalesce(silver.n, 0), coalesce(quarantine.n, 0),
           fetches.last_fetched_at, :computed_at
    FROM scope s
    LEFT JOIN fetches ON fetches.domain_id = s.domain_id
    LEFT JOIN links ON links.domain_id = s.domain_id
    LEFT JOIN silver ON silver.domain_id = s.domain_id
    LEFT JOIN quarantine ON quarantine.domain_id = s.domain_id
    ON CONFLICT (domain_id) DO UPDATE SET
        discovered_links = EXCLUDED.discovered_links,
        scraped_pages = EXCLUDED.scraped_pages,
        failed_pages = EXCLUDED.failed_pages,
        page_count = EXCLUDED.page_count,
        quarantined_count = EXCLUDED.quarantined_count,
        last_fetched_at = EXCLUDED.last_fetched_at,
        computed_at = EXCLUDED.computed_at,
        updated_at = now()
    """
).bindparams(bindparam("domain_ids", type_=ARRAY(BigInteger)))


# Full rebuild of all 837 regions. A page counts in every region any of its tags falls
# in (page_geo_codes carries the full chain), once per region.
_REFRESH_GEO_CONTENT_STATS = text(
    """
    WITH tagged AS (
        SELECT page_id, 'province' AS level, province_code AS code
        FROM page_geo_codes WHERE province_code IS NOT NULL
        UNION
        SELECT page_id, 'district', district_code
        FROM page_geo_codes WHERE district_code IS NOT NULL
        UNION
        SELECT page_id, 'local_body', local_body_code
        FROM page_geo_codes WHERE local_body_code IS NOT NULL
    ),
    tagged_pages AS (
        SELECT t.level, t.code, p.id, p.domain_id, p.content_type, p.language,
               coalesce(p.category, 'UNCATEGORIZED') AS category, p.published_at,
               p.stored_file_id IS NOT NULL AS is_document
        FROM tagged t JOIN pages p ON p.id = t.page_id
        WHERE p.duplicate_of_id IS NULL
    ),
    totals AS (
        SELECT level, code, count(*) AS page_count,
               count(*) FILTER (WHERE is_document) AS document_count,
               count(DISTINCT domain_id) AS domain_count,
               max(published_at) AS latest_published_at
        FROM tagged_pages GROUP BY level, code
    ),
    by_content_type AS (
        SELECT level, code, jsonb_object_agg(content_type, n) AS counts
        FROM (SELECT level, code, content_type, count(*) AS n
              FROM tagged_pages GROUP BY level, code, content_type) x
        GROUP BY level, code
    ),
    by_language AS (
        SELECT level, code, jsonb_object_agg(language, n) AS counts
        FROM (SELECT level, code, language, count(*) AS n
              FROM tagged_pages GROUP BY level, code, language) x
        GROUP BY level, code
    ),
    by_category AS (
        SELECT level, code, jsonb_object_agg(category, n) AS counts
        FROM (SELECT level, code, category, count(*) AS n
              FROM tagged_pages GROUP BY level, code, category) x
        GROUP BY level, code
    ),
    regions AS (
        SELECT 'province' AS level, code FROM provinces
        UNION ALL SELECT 'district', code FROM districts
        UNION ALL SELECT 'local_body', code FROM local_bodies
    )
    INSERT INTO geo_content_stats (
        province_code, district_code, local_body_code, page_count, document_count,
        domain_count, latest_published_at, by_content_type, by_language, by_category,
        computed_at
    )
    SELECT CASE WHEN r.level = 'province' THEN r.code END,
           CASE WHEN r.level = 'district' THEN r.code END,
           CASE WHEN r.level = 'local_body' THEN r.code END,
           coalesce(t.page_count, 0), coalesce(t.document_count, 0),
           coalesce(t.domain_count, 0), t.latest_published_at,
           coalesce(ct.counts, '{}'), coalesce(lg.counts, '{}'), coalesce(cg.counts, '{}'),
           :computed_at
    FROM regions r
    LEFT JOIN totals t ON (t.level, t.code) = (r.level, r.code)
    LEFT JOIN by_content_type ct ON (ct.level, ct.code) = (r.level, r.code)
    LEFT JOIN by_language lg ON (lg.level, lg.code) = (r.level, r.code)
    LEFT JOIN by_category cg ON (cg.level, cg.code) = (r.level, r.code)
    ON CONFLICT ON CONSTRAINT uq_geo_content_stats_region DO UPDATE SET
        page_count = EXCLUDED.page_count,
        document_count = EXCLUDED.document_count,
        domain_count = EXCLUDED.domain_count,
        latest_published_at = EXCLUDED.latest_published_at,
        by_content_type = EXCLUDED.by_content_type,
        by_language = EXCLUDED.by_language,
        by_category = EXCLUDED.by_category,
        computed_at = EXCLUDED.computed_at,
        updated_at = now()
    """
)

_GEO_LEVEL_COLUMN = {
    "province": ("province_code", None),
    "district": ("district_code", "province_code"),
    "local_body": ("local_body_code", "district_code"),
}


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


class StatsRepository:
    """Gold refresh and admin dashboard reads for one SQLAlchemy session."""

    def __init__(self, session: Session) -> None:
        self.session = session

    # ------------------------------------------------------------- domain stats

    def refresh_domain_stats(
        self, domain_ids: Iterable[int] | None = None, *, when: datetime | None = None
    ) -> int:
        """Recompute `domain_stats` for the given domains (all when None). Returns rows written.

        Every domain in scope gets a row, zeros included, so the admin list never
        has to treat a missing row as a special case.
        """
        ids = None if domain_ids is None else sorted(set(domain_ids))
        if ids == []:
            return 0
        result = self.session.execute(
            _REFRESH_DOMAIN_STATS,
            {"domain_ids": ids, "computed_at": when or datetime.now(UTC)},
        )
        self.session.expire_all()  # loaded DomainStats objects are now stale
        return int(result.rowcount or 0)  # type: ignore[attr-defined]

    # -------------------------------------------------------- geo content stats

    def refresh_geo_content_stats(self, *, when: datetime | None = None) -> int:
        """Rebuild `geo_content_stats` for every region. Returns rows written (837).

        Every region gets a row, zeros included, so the map colours empty regions
        without a special case.
        """
        result = self.session.execute(
            _REFRESH_GEO_CONTENT_STATS, {"computed_at": when or datetime.now(UTC)}
        )
        self.session.expire_all()
        return int(result.rowcount or 0)  # type: ignore[attr-defined]

    def geo_content(self, level: str, *, within: str | None = None) -> list[dict[str, Any]]:
        """Content counts for every region of one level, for colouring the map.

        `level` is "province", "district" or "local_body"; `within` limits it to one
        parent (`P4` -> its districts, `D38` -> its local bodies). Ordered by code.
        """
        if level not in _GEO_LEVEL_COLUMN:
            raise ValueError(f"level must be province, district or local_body, not {level!r}")
        own, parent = _GEO_LEVEL_COLUMN[level]
        code = getattr(GeoContentStats, own)
        stmt = select(GeoContentStats, code.label("code")).where(code.is_not(None))
        if within is not None:
            if parent is None:
                raise ValueError("provinces have no parent to filter by")
            region = District if level == "district" else LocalBody
            stmt = stmt.join(region, region.code == code).where(
                getattr(region, parent) == within
            )
        return [
            {
                "level": level,
                "code": region_code,
                "page_count": row.page_count,
                "document_count": row.document_count,
                "domain_count": row.domain_count,
                "latest_published_at": row.latest_published_at,
                "by_content_type": row.by_content_type,
                "by_language": row.by_language,
                "by_category": row.by_category,
                "computed_at": row.computed_at,
            }
            for row, region_code in self.session.execute(stmt.order_by(code)).all()
        ]

    def list_domains(
        self,
        *,
        status: DomainStatus | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """One page of domains with their stats (the `DomainOverview` shape), plus the total.

        `search` matches the hostname or website name, case-insensitively. Ordered by
        priority (HIGH first) then hostname. Counts are 0 until the first refresh.
        """
        stmt = select(Domain, DomainStats).outerjoin(
            DomainStats, DomainStats.domain_id == Domain.id
        )
        if status is not None:
            stmt = stmt.where(Domain.status == status)
        if search and search.strip():
            pattern = f"%{_escape_like(search.strip())}%"
            stmt = stmt.where(
                or_(
                    Domain.domain.ilike(pattern, escape="\\"),
                    Domain.website_name.ilike(pattern, escape="\\"),
                )
            )
        total = self.session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
        priority_rank = case(
            {DomainPriority.HIGH: 0, DomainPriority.NORMAL: 1}, value=Domain.priority, else_=2
        )
        rows = self.session.execute(
            stmt.order_by(priority_rank, Domain.domain).limit(limit).offset(offset)
        ).all()
        return [self._overview(domain, stats) for domain, stats in rows], int(total)

    @staticmethod
    def _overview(domain: Domain, stats: DomainStats | None) -> dict[str, Any]:
        return {
            "id": domain.id,
            "domain": domain.domain,
            "website_name": domain.website_name,
            "category": domain.category,
            "status": domain.status,
            "priority": domain.priority,
            "rate_limit_per_sec": domain.rate_limit_per_sec,
            "last_crawled_at": domain.last_crawled_at,
            "discovered_links": stats.discovered_links if stats else 0,
            "scraped_pages": stats.scraped_pages if stats else 0,
            "failed_pages": stats.failed_pages if stats else 0,
            "page_count": stats.page_count if stats else 0,
            "quarantined_count": stats.quarantined_count if stats else 0,
            "last_fetched_at": stats.last_fetched_at if stats else None,
            "stats_computed_at": stats.computed_at if stats else None,
        }

    def set_domain_status(self, domain_id: int, status: DomainStatus) -> Domain:
        """The admin's PAUSE / RESUME / RE_CRAWL actions, as a status the scraper reads.

        The API maps PAUSE -> PAUSED and RESUME / RE_CRAWL -> PENDING.
        """
        domain = self.session.get(Domain, domain_id)
        if domain is None:
            raise LookupError(f"domain {domain_id} does not exist")
        domain.status = status
        return domain

    # ---------------------------------------------------------------- dashboard

    def storage_metrics(self) -> dict[str, Any]:
        """Bronze work-queue counts across crawled pages and stored files, plus bytes stored.

        Sizes cover stored files only; the size of a crawled page is not recorded.
        """
        queue = union_all(
            select(
                CrawledDocument.processing_status.label("status"),
                CrawledDocument.fetched_at.label("at"),
                literal(0, BigInteger).label("size_bytes"),
            ),
            select(
                StoredFile.processing_status.label("status"),
                StoredFile.stored_at.label("at"),
                StoredFile.size_bytes.label("size_bytes"),
            ),
        ).subquery()
        counts = {status.value: 0 for status in ProcessingStatus}
        sizes = dict.fromkeys(counts, 0)
        oldest: dict[str, datetime | None] = dict.fromkeys(counts)
        for status, count, size, first_at in self.session.execute(
            select(
                queue.c.status,
                func.count(),
                func.coalesce(func.sum(queue.c.size_bytes), 0),
                func.min(queue.c.at),
            ).group_by(queue.c.status)
        ).all():
            key = ProcessingStatus(status).value
            counts[key], sizes[key], oldest[key] = int(count), int(size), first_at
        total_bytes = self.session.scalar(
            select(func.coalesce(func.sum(StoredFile.size_bytes), 0))
        )
        unprocessed = ProcessingStatus.UNPROCESSED.value
        return {
            "total_raw_files": sum(counts.values()),
            "unprocessed_files": counts[unprocessed],
            "processing_files": counts[ProcessingStatus.PROCESSING.value],
            "processed_files": counts[ProcessingStatus.PROCESSED.value],
            "failed_files": counts[ProcessingStatus.FAILED.value],
            "quarantined_files": counts[ProcessingStatus.QUARANTINED.value],
            "unprocessed_size_bytes": sizes[unprocessed],
            "oldest_unprocessed_at": oldest[unprocessed],
            "total_storage_used_bytes": int(total_bytes or 0),
        }

    def dashboard_summary(self, *, now: datetime | None = None) -> dict[str, Any]:
        """The database's part of `GET /api/v1/admin/dashboard/summary` (`DashboardSummary`)."""
        now = now or datetime.now(UTC)
        by_status = {status.value: 0 for status in DomainStatus}
        for status, count in self.session.execute(
            select(Domain.status, func.count()).group_by(Domain.status)
        ).all():
            by_status[DomainStatus(status).value] = int(count)
        discovered = self.session.scalar(
            select(func.coalesce(func.sum(DomainStats.discovered_links), 0))
        )
        return {
            "timestamp": now,
            "domains": {
                "total_registered": sum(by_status.values()),
                "pending": by_status[DomainStatus.PENDING.value],
                "active_crawling": by_status[DomainStatus.CRAWLING.value],
                "paused": by_status[DomainStatus.PAUSED.value],
                "completed": by_status[DomainStatus.COMPLETED.value],
                "failed_or_blocked": by_status[DomainStatus.FAILED.value],
            },
            "links": {"total_child_links_discovered": int(discovered or 0)},
            "storage": self.storage_metrics(),
            "quarantine": QuarantineRepository(self.session).summary(),
            "errors_last_24h": OpsRepository(self.session).count_errors_since(
                now - timedelta(hours=24)
            ),
        }
