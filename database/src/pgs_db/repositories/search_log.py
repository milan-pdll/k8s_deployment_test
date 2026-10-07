"""The search log and relevance labels: `search_queries`, `search_clicks`, `relevance_judgments`.

The API writes a query row for every search it answers (`log_query`) and a click row
when a user opens a result (`log_click`). Admins grade results (`judge`), and the
reranker trains on `training_examples`. `traffic` feeds the dashboard's queries/sec and
latency cards; `top_queries` / `zero_result_queries` show what users look for and miss.

Privacy: only an opaque session id is kept, never an IP, user agent or account. Old
rows go with `purge_before` (clicks with their query); labels are kept.
"""

import re
import unicodedata
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from ..enums import JudgmentSource
from ..models import Page, RelevanceJudgment, SearchClick, SearchQuery
from .ranking import RankingRepository

_SPACE = re.compile(r"\s+")


def normalize_query(text: str) -> str:
    """NFC, lowercase, whitespace collapsed: the key judgments and analytics group by.

    NFC matters for Nepali: the same word can arrive composed or decomposed.
    """
    return _SPACE.sub(" ", unicodedata.normalize("NFC", text)).strip().lower()


class SearchLogRepository:
    """Search logging, labels and search analytics for one SQLAlchemy session."""

    def __init__(self, session: Session) -> None:
        self.session = session

    # ------------------------------------------------------------------ logging

    def log_query(
        self,
        query_text: str,
        *,
        result_count: int,
        session_id: str | None = None,
        query_language: str | None = None,
        translated_text: str | None = None,
        filters: dict[str, Any] | None = None,
        page_number: int = 1,
        latency_ms: int | None = None,
        searched_at: datetime | None = None,
    ) -> SearchQuery:
        """Record one answered search. An empty query (a pure map click) is allowed."""
        row = SearchQuery(
            query_text=query_text,
            normalized_query=normalize_query(query_text),
            result_count=result_count,
            session_id=session_id,
            query_language=query_language,
            translated_text=translated_text,
            filters=filters or None,
            page_number=page_number,
            latency_ms=latency_ms,
        )
        if searched_at is not None:
            row.searched_at = searched_at
        self.session.add(row)
        self.session.flush()
        return row

    def log_click(
        self,
        search_query_id: int,
        *,
        url: str,
        rank_position: int,
        page_id: int | None = None,
        dwell_ms: int | None = None,
        clicked_at: datetime | None = None,
    ) -> SearchClick:
        """Record a result the user opened. `page_id` is looked up from `url` when not given."""
        if self.session.get(SearchQuery, search_query_id) is None:
            raise LookupError(f"search query {search_query_id} does not exist")
        if page_id is None:
            page_id = self.session.scalar(select(Page.id).where(Page.canonical_url == url))
        click = SearchClick(
            search_query_id=search_query_id,
            url=url,
            rank_position=rank_position,
            page_id=page_id,
            dwell_ms=dwell_ms,
        )
        if clicked_at is not None:
            click.clicked_at = clicked_at
        self.session.add(click)
        self.session.flush()
        return click

    def purge_before(self, when: datetime) -> int:
        """Delete searches (and their clicks) older than `when`. Returns searches deleted."""
        result = self.session.execute(delete(SearchQuery).where(SearchQuery.searched_at < when))
        return int(result.rowcount or 0)  # type: ignore[attr-defined]

    # ------------------------------------------------------------------- labels

    def judge(
        self,
        query: str,
        page_id: int,
        grade: int,
        *,
        judged_by: str,
        source: JudgmentSource = JudgmentSource.HUMAN,
        notes: str | None = None,
    ) -> RelevanceJudgment:
        """Label how relevant `page_id` is to `query` (0..3). Re-labelling updates it."""
        if not 0 <= grade <= 3:
            raise ValueError("grade must be 0 (irrelevant) .. 3 (perfect)")
        if not judged_by.strip():
            raise ValueError("judged_by must name the judge")
        normalized = normalize_query(query)
        if not normalized:
            raise ValueError("query must not be empty")
        stmt = insert(RelevanceJudgment).values(
            normalized_query=normalized,
            page_id=page_id,
            grade=grade,
            source=source,
            judged_by=judged_by.strip(),
            notes=notes,
        )
        judgment_id = self.session.scalar(
            stmt.on_conflict_do_update(
                constraint="uq_relevance_judgments_label",
                set_={
                    "grade": stmt.excluded.grade,
                    "source": stmt.excluded.source,
                    "notes": stmt.excluded.notes,
                    "updated_at": func.now(),
                },
            ).returning(RelevanceJudgment.id)
        )
        judgment = self.session.get(RelevanceJudgment, judgment_id)
        if judgment is None:  # pragma: no cover - RETURNING always yields the row
            raise RuntimeError("relevance judgment vanished after upsert")
        self.session.refresh(judgment)
        return judgment

    def training_examples(self, *, min_labels: int = 1) -> list[dict[str, Any]]:
        """One row per labelled (query, page): the mean grade, rounded, plus page features.

        The reranker adds its query-dependent features (BM25, vector score, title
        match) itself; this supplies the label and the query-independent ones.
        Ordered by query, so rows of one query are adjacent (LightGBM's `group`).
        """
        rows = self.session.execute(
            select(
                RelevanceJudgment.normalized_query,
                RelevanceJudgment.page_id,
                func.round(func.avg(RelevanceJudgment.grade)).label("grade"),
                func.count().label("labels"),
            )
            .group_by(RelevanceJudgment.normalized_query, RelevanceJudgment.page_id)
            .having(func.count() >= min_labels)
            .order_by(RelevanceJudgment.normalized_query, RelevanceJudgment.page_id)
        ).all()
        features = RankingRepository(self.session).ranking_features([r.page_id for r in rows])
        return [
            {
                "query": row.normalized_query,
                "page_id": row.page_id,
                "label": int(row.grade),
                "labels": row.labels,
                **features.get(row.page_id, {}),
            }
            for row in rows
            if row.page_id in features  # skip labels on pages that became duplicates
        ]

    # ---------------------------------------------------------------- analytics

    def traffic(self, since: datetime, until: datetime) -> dict[str, Any]:
        """Searches, queries per second and latency over a window, for the dashboard."""
        if until <= since:
            raise ValueError("until must be after since")
        count, avg_ms, p95_ms, zero = self.session.execute(
            select(
                func.count(),
                func.avg(SearchQuery.latency_ms),
                func.percentile_cont(0.95).within_group(SearchQuery.latency_ms),
                func.count().filter(SearchQuery.result_count == 0),
            ).where(SearchQuery.searched_at >= since, SearchQuery.searched_at < until)
        ).one()
        clicks = self.session.scalar(
            select(func.count())
            .select_from(SearchClick)
            .join(SearchQuery, SearchQuery.id == SearchClick.search_query_id)
            .where(SearchQuery.searched_at >= since, SearchQuery.searched_at < until)
        )
        seconds = (until - since).total_seconds()
        return {
            "searches": int(count),
            "queries_per_second": int(count) / seconds,
            "avg_latency_ms": float(avg_ms) if avg_ms is not None else None,
            "p95_latency_ms": float(p95_ms) if p95_ms is not None else None,
            "zero_result_rate": int(zero) / int(count) if count else 0.0,
            "click_through_rate": int(clicks or 0) / int(count) if count else 0.0,
        }

    def top_queries(self, since: datetime, *, limit: int = 20) -> list[dict[str, Any]]:
        """The most searched (normalized) queries since `since`, with how often they
        found nothing."""
        return self._grouped(since, limit, zero_only=False)

    def zero_result_queries(self, since: datetime, *, limit: int = 20) -> list[dict[str, Any]]:
        """Queries that returned nothing, most frequent first: content the index is missing."""
        return self._grouped(since, limit, zero_only=True)

    def _grouped(self, since: datetime, limit: int, *, zero_only: bool) -> list[dict[str, Any]]:
        stmt = (
            select(
                SearchQuery.normalized_query,
                func.count().label("searches"),
                func.count().filter(SearchQuery.result_count == 0).label("zero_results"),
            )
            .where(SearchQuery.searched_at >= since, SearchQuery.normalized_query != "")
            .group_by(SearchQuery.normalized_query)
            .order_by(func.count().desc(), SearchQuery.normalized_query)
            .limit(limit)
        )
        if zero_only:
            stmt = stmt.where(SearchQuery.result_count == 0)
        return [
            {"query": q, "searches": int(n), "zero_results": int(z)}
            for q, n, z in self.session.execute(stmt).all()
        ]

    def clicks_for(self, search_query_ids: Sequence[int]) -> list[SearchClick]:
        """Clicks of the given searches, in click order."""
        if not search_query_ids:
            return []
        return list(
            self.session.scalars(
                select(SearchClick)
                .where(SearchClick.search_query_id.in_(search_query_ids))
                .order_by(SearchClick.clicked_at, SearchClick.id)
            )
        )
