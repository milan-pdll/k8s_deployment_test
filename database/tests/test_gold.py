"""Gold: geo_content_stats, page_scores / domain authority, and the search log."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from pgs_db import (
    BronzeRepository,
    RankingRepository,
    SearchLogRepository,
    SilverRepository,
    StatsRepository,
)
from pgs_db.enums import JudgmentSource
from pgs_db.models import DomainStats, Page, PageScore, SearchClick, SearchQuery
from pgs_db.repositories.ranking import freshness, pagerank, quality
from pgs_db.repositories.search_log import normalize_query
from pgs_db.schemas import GeoContentCount, PageScoreRead, SearchTraffic

_ANCIENT = datetime(1975, 1, 1, tzinfo=UTC)
NOW = datetime(2026, 9, 29, tzinfo=UTC)
POKHARA, KASKI, GANDAKI = "MUN414", "D38", "P4"


@pytest.fixture()
def bronze(session: Session) -> BronzeRepository:
    return BronzeRepository(session)


@pytest.fixture()
def silver(session: Session) -> SilverRepository:
    return SilverRepository(session)


def page(
    bronze: BronzeRepository,
    silver: SilverRepository,
    host: str,
    path: str,
    n: int,
    *,
    links: list[str] | None = None,
    geo: list[dict[str, Any]] | None = None,
    stored: bool = False,
    **payload: Any,
) -> int:
    url = f"https://{host}/{path}"
    if stored:
        source = bronze.save_stored_file(
            {
                "source_page_url": f"https://{host}/",
                "document_url": url,
                "storage_path": f"raw/{n}.pdf",
                "content_type": "application/pdf",
                "sha256": f"{n:064x}",
                "size": 100,
                "stored_at": _ANCIENT.isoformat(),
            }
        ).id
        kw: dict[str, Any] = {"stored_file_id": source}
    else:
        source = bronze.save_document(
            {
                "url": url,
                "normalized_url": url,
                "host": host,
                "content_hash": f"{n:064x}",
                "fetched_at": (_ANCIENT + timedelta(days=n)).isoformat(),
                "links": links or [],
            },
            register_unknown_domains=True,
        ).id
        kw = {"crawled_document_id": source}
    body: dict[str, Any] = {"searchable_text": f"text {n}", "content_hash": f"{n:064x}"}
    body.update(payload)
    if geo is not None:
        body["geo_location"] = geo
    return silver.save_page(body, domain_id=bronze.domain_id_for_host(host), **kw).id


def tag(code_key: str, code: str) -> dict[str, Any]:
    return {code_key: code, "method": "GAZETTEER", "confidence": 0.9}


# ------------------------------------------------------------------ geo content


class TestGeoContentStats:
    def test_a_page_counts_in_every_region_containing_its_tags(
        self, session: Session, bronze: BronzeRepository, silver: SilverRepository
    ) -> None:
        stats = StatsRepository(session)
        stats.refresh_geo_content_stats()
        before = {r["code"]: r for r in stats.geo_content("local_body", within=KASKI)}
        district_before = {r["code"]: r for r in stats.geo_content("district", within=GANDAKI)}

        page(bronze, silver, "geo-a.gov.np", "1", 1, geo=[tag("municipality_id", POKHARA)],
             language_detected="ne", category="notice")
        page(bronze, silver, "geo-b.gov.np", "2", 2, geo=[tag("district_code", KASKI)],
             language_detected="en", published_at="2026-01-02T00:00:00Z")
        page(bronze, silver, "geo-b.gov.np", "3.pdf", 3, geo=[tag("municipality_id", POKHARA)],
             stored=True, content_type="document")

        assert stats.refresh_geo_content_stats(when=NOW) == 7 + 77 + 753
        pokhara = {r["code"]: r for r in stats.geo_content("local_body", within=KASKI)}[POKHARA]
        kaski = {r["code"]: r for r in stats.geo_content("district", within=GANDAKI)}[KASKI]

        assert pokhara["page_count"] == before[POKHARA]["page_count"] + 2
        assert pokhara["document_count"] == before[POKHARA]["document_count"] + 1
        # Kaski gets the Pokhara pages too, plus the one tagged only with Kaski.
        assert kaski["page_count"] == district_before[KASKI]["page_count"] + 3
        assert kaski["domain_count"] >= 2
        assert kaski["by_language"].get("EN", 0) >= 1
        assert pokhara["by_category"].get("notice", 0) >= 1
        GeoContentCount.model_validate(kaski)

    def test_every_region_has_a_row_and_levels_are_filtered(self, session: Session) -> None:
        stats = StatsRepository(session)
        stats.refresh_geo_content_stats()
        assert len(stats.geo_content("province")) == 7
        assert len(stats.geo_content("district")) == 77
        assert len(stats.geo_content("local_body")) == 753
        assert {r["code"] for r in stats.geo_content("district", within=GANDAKI)} >= {KASKI}
        with pytest.raises(ValueError):
            stats.geo_content("ward")
        with pytest.raises(ValueError):
            stats.geo_content("province", within="P1")


# ---------------------------------------------------------------------- ranking


class TestScoringFunctions:
    def test_pagerank_rewards_the_most_linked_page(self) -> None:
        # 1, 2 and 3 all link to 4; 4 links back to 1.
        ranks = pagerank([1, 2, 3, 4], [(1, 4), (2, 4), (3, 4), (4, 1)])
        assert sum(ranks.values()) == pytest.approx(1.0)
        assert max(ranks, key=lambda k: ranks[k]) == 4
        assert ranks[1] > ranks[2] == pytest.approx(ranks[3])
        assert pagerank([], []) == {}

    def test_dangling_and_unknown_nodes(self) -> None:
        ranks = pagerank([1, 2], [(1, 2), (2, 99)])  # 2 is dangling once 99 is dropped
        assert sum(ranks.values()) == pytest.approx(1.0)
        assert ranks[2] > ranks[1]

    def test_freshness_halves_every_half_life(self) -> None:
        assert freshness(NOW, NOW) == 1.0
        assert freshness(NOW - timedelta(days=180), NOW) == pytest.approx(0.5)
        assert freshness(NOW + timedelta(days=3), NOW) == 1.0  # future dates are "now"
        assert freshness(None, NOW) == 0.0

    def test_quality_grows_with_length_and_drops_per_flag(self) -> None:
        assert quality(0, None) == 0.0
        assert quality(1000, None) == pytest.approx(1.0)
        assert quality(100, None) < quality(1000, None)
        assert quality(1000, ["thin_content"]) == pytest.approx(0.75)
        assert quality(10, ["a", "b", "c", "d"]) == 0.0


class TestRefreshScores:
    def test_link_graph_scores_and_domain_authority(
        self, session: Session, bronze: BronzeRepository, silver: SilverRepository
    ) -> None:
        hub = "https://rank-hub.gov.np/index"
        # Two other sites link to the hub; the hub links to one of them.
        page(bronze, silver, "rank-hub.gov.np", "index", 101,
             links=["https://rank-a.gov.np/a"], word_count=1000,
             published_at=NOW.isoformat())
        a = page(bronze, silver, "rank-a.gov.np", "a", 102, links=[hub + "/"])  # trailing slash
        b = page(bronze, silver, "rank-b.gov.np", "b", 103, links=[hub, "https://nowhere.np/x"])
        hub_id = session.scalar(select(Page.id).where(Page.canonical_url == hub))
        assert hub_id is not None

        result = RankingRepository(session).refresh_scores(when=NOW)
        assert result["pages"] >= 3 and result["edges"] >= 3

        scores = {
            s.page_id: s
            for s in session.scalars(
                select(PageScore).where(PageScore.page_id.in_([hub_id, a, b]))
            )
        }
        assert scores[hub_id].inbound_links == 2
        assert scores[hub_id].pagerank > scores[a].pagerank > scores[b].pagerank
        assert scores[hub_id].freshness_score == pytest.approx(1.0)
        assert scores[hub_id].quality_score == pytest.approx(1.0)
        assert scores[hub_id].static_rank > scores[b].static_rank
        PageScoreRead.model_validate(scores[hub_id])

        hub_domain = session.scalar(
            select(DomainStats).where(
                DomainStats.domain_id == bronze.domain_id_for_host("rank-hub.gov.np")
            )
        )
        assert hub_domain is not None
        assert hub_domain.inbound_link_count == 2
        assert 0 < hub_domain.authority_score <= 1

    def test_duplicates_lose_their_scores_and_features_are_exposed(
        self, session: Session, bronze: BronzeRepository, silver: SilverRepository
    ) -> None:
        ranking = RankingRepository(session)
        keep = page(bronze, silver, "dup.gov.np", "keep", 201)
        dup = page(bronze, silver, "dup.gov.np", "dup", 202)
        ranking.refresh_scores(when=NOW)
        assert session.scalar(select(PageScore).where(PageScore.page_id == dup)) is not None

        silver.mark_duplicate_of(dup, keep)
        session.flush()
        ranking.refresh_scores(when=NOW)
        assert session.scalar(select(PageScore).where(PageScore.page_id == dup)) is None

        features = ranking.ranking_features([keep, dup])
        assert set(features) == {keep}
        assert {"freshness", "source_authority", "content_length", "static_rank"} <= set(
            features[keep]
        )

    def test_scores_survive_a_domain_stats_refresh(
        self, session: Session, bronze: BronzeRepository, silver: SilverRepository
    ) -> None:
        page(bronze, silver, "keep-auth.gov.np", "x", 301)
        page(bronze, silver, "keep-auth-2.gov.np", "y", 302, links=["https://keep-auth.gov.np/x"])
        RankingRepository(session).refresh_scores(when=NOW)
        domain_id = bronze.domain_id_for_host("keep-auth.gov.np")
        assert domain_id is not None
        StatsRepository(session).refresh_domain_stats([domain_id])
        row = session.scalar(select(DomainStats).where(DomainStats.domain_id == domain_id))
        assert row is not None and row.inbound_link_count == 1


# ------------------------------------------------------------------- search log


class TestSearchLog:
    def test_queries_and_clicks(
        self, session: Session, bronze: BronzeRepository, silver: SilverRepository
    ) -> None:
        log = SearchLogRepository(session)
        page_id = page(bronze, silver, "log.gov.np", "budget", 401)
        q = log.log_query("  Pokhara   BUDGET ", result_count=12, session_id="s1",
                          latency_ms=40, filters={"district_code": KASKI})
        assert q.normalized_query == "pokhara budget"

        click = log.log_click(q.id, url="https://log.gov.np/budget", rank_position=1)
        assert click.page_id == page_id  # resolved from the URL
        assert [c.id for c in log.clicks_for([q.id])] == [click.id]
        with pytest.raises(LookupError):
            log.log_click(9_999_999, url="https://x", rank_position=1)

        session.add(SearchClick(search_query_id=q.id, url="https://x", rank_position=0))
        with pytest.raises(IntegrityError, match="ck_search_clicks_rank_position_positive"):
            session.flush()

    def test_nepali_queries_normalize_to_nfc(self) -> None:
        # क़ can arrive as one code point (U+0958) or as क + nukta; NFC makes them equal.
        assert normalize_query("क़ानून") == normalize_query(
            "क़ानून"
        )
        assert normalize_query(" A  B ") == "a b"

    def test_traffic_and_query_reports(self, session: Session) -> None:
        log = SearchLogRepository(session)
        start = _ANCIENT
        for i, (text, results, ms) in enumerate(
            [("lok sewa", 10, 20), ("lok sewa", 0, 40), ("tender", 5, 60), ("xyz", 0, 80)]
        ):
            q = log.log_query(text, result_count=results, latency_ms=ms,
                              searched_at=start + timedelta(seconds=i))
            if text == "tender":
                log.log_click(q.id, url="https://t", rank_position=2)

        traffic = SearchTraffic.model_validate(
            log.traffic(start, start + timedelta(seconds=10))
        )
        assert traffic.searches == 4
        assert traffic.queries_per_second == pytest.approx(0.4)
        assert traffic.avg_latency_ms == pytest.approx(50)
        assert traffic.zero_result_rate == pytest.approx(0.5)
        assert traffic.click_through_rate == pytest.approx(0.25)

        window_top = [r for r in log.top_queries(start) if r["query"] in {"lok sewa", "tender"}]
        assert window_top[0] == {"query": "lok sewa", "searches": 2, "zero_results": 1}
        zero = {r["query"] for r in log.zero_result_queries(start)}
        assert {"xyz", "lok sewa"} <= zero and "tender" not in zero
        with pytest.raises(ValueError):
            log.traffic(start, start)

    def test_purge_removes_old_searches_with_their_clicks(self, session: Session) -> None:
        log = SearchLogRepository(session)
        query_id = log.log_query(
            "old", result_count=1, searched_at=_ANCIENT - timedelta(days=900)
        ).id
        click_id = log.log_click(query_id, url="https://o", rank_position=1).id
        assert log.purge_before(_ANCIENT - timedelta(days=800)) >= 1
        session.expire_all()
        assert session.get(SearchQuery, query_id) is None
        assert session.get(SearchClick, click_id) is None  # cascaded


class TestJudgments:
    def test_labels_upsert_and_feed_training(
        self, session: Session, bronze: BronzeRepository, silver: SilverRepository
    ) -> None:
        log = SearchLogRepository(session)
        good = page(bronze, silver, "judge.gov.np", "good", 501)
        bad = page(bronze, silver, "judge.gov.np", "bad", 502)
        RankingRepository(session).refresh_scores(when=NOW)

        first = log.judge("Zz Budget", good, 2, judged_by="alice")
        again = log.judge("zz  budget", good, 3, judged_by="alice", notes="exact doc")
        assert again.id == first.id and again.grade == 3
        log.judge("zz budget", good, 2, judged_by="bob")
        log.judge("zz budget", bad, 0, judged_by="alice", source=JudgmentSource.HUMAN)

        rows = [r for r in log.training_examples() if r["query"] == "zz budget"]
        # good: mean of 3 (alice) and 2 (bob) = 2.5; Postgres rounds numerics half away from 0.
        assert [(r["page_id"], r["label"], r["labels"]) for r in rows] == [
            (good, 3, 2),
            (bad, 0, 1),
        ]
        assert rows[0]["freshness"] is not None and "source_authority" in rows[0]
        assert next(r for r in log.training_examples(min_labels=2) if r["query"] == "zz budget")[
            "page_id"
        ] == good

    @pytest.mark.parametrize(
        "query, grade, judge", [("q", 4, "a"), ("q", -1, "a"), ("  ", 1, "a"), ("q", 1, " ")]
    )
    def test_bad_labels(
        self,
        session: Session,
        bronze: BronzeRepository,
        silver: SilverRepository,
        query: str,
        grade: int,
        judge: str,
    ) -> None:
        page_id = page(bronze, silver, "judge2.gov.np", "p", 601)
        with pytest.raises(ValueError):
            SearchLogRepository(session).judge(query, page_id, grade, judged_by=judge)

    def test_search_documents_carry_the_scores(
        self, session: Session, bronze: BronzeRepository, silver: SilverRepository
    ) -> None:
        from pgs_db.models import search_documents

        page_id = page(bronze, silver, "view.gov.np", "p", 701)
        RankingRepository(session).refresh_scores(when=NOW)
        session.execute(update(Page).where(Page.id == page_id).values(title="scored"))
        session.flush()
        row = session.execute(
            select(search_documents).where(search_documents.c.page_id == page_id)
        ).one()
        assert row.static_rank is not None and row.domain_authority is not None
