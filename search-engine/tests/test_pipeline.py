from __future__ import annotations

import time
from typing import Any

import pytest
from opensearchpy.exceptions import ConnectionError as OpenSearchConnectionError
from opensearchpy.exceptions import ConnectionTimeout

from pgs_search.config import settings
from pgs_search.pipeline import (
    FinalSearchPipeline,
    InvalidSearchRequest,
    SearchBackendUnavailable,
    SearchInput,
    SearchTimeout,
    build_filter_clauses,
)
from pgs_search.query.normalizer import QueryExpansion


def source(document_id: str, **extra: Any) -> dict[str, Any]:
    return {
        "document_id": document_id,
        "title": f"Title {document_id}",
        "description": "",
        "summary": f"Summary of {document_id}",
        "source_url": f"https://site.gov.np/{document_id}",
        "domain": "site.gov.np",
        "language": "ne",
        "content_type": "web_page",
        "geo": {"province_code": "P4", "province_name_en": "Gandaki", "district_code": "D38"},
        "published_at": "2026-09-30T08:00:00+00:00",
        "content_length": 1200,
        **extra,
    }


class FakeOpenSearch:
    def __init__(self, hits: list[dict[str, Any]], stored: dict[str, dict[str, Any]] | None = None):
        self.hits = hits
        self.stored = stored or {}
        self.search_calls: list[dict[str, Any]] = []
        self.mget_calls: list[dict[str, Any]] = []
        self.error: Exception | None = None

    def search(self, **kwargs: Any) -> dict[str, Any]:
        self.search_calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return {"hits": {"hits": self.hits}}

    def mget(self, **kwargs: Any) -> dict[str, Any]:
        self.mget_calls.append(kwargs)
        docs = [
            {"_id": doc_id, "found": doc_id in self.stored, "_source": self.stored.get(doc_id, {})}
            for doc_id in kwargs["body"]["ids"]
        ]
        return {"docs": docs}


def hit(document_id: str, score: float, highlight: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"_id": document_id, "_score": score, "_source": source(document_id)}
    if highlight:
        result["highlight"] = {"searchable_text": [highlight]}
    return result


def expansion(query: str) -> QueryExpansion:
    return QueryExpansion(query.lower(), "en", [query.lower(), "पोखरा"], False)


def pipeline(client: FakeOpenSearch, dense=None, reranker=None, **config: Any):
    return FinalSearchPipeline(
        opensearch_client=client,
        dense_search=dense,
        reranker=reranker,
        expander=expansion,
        config=settings.model_copy(update=config),
    )


def test_hybrid_results_are_fused_hydrated_and_paged() -> None:
    client = FakeOpenSearch(
        hits=[hit("1", 12.0, "budget of pokhara"), hit("2", 8.0)],
        stored={"3": source("3")},
    )

    def dense(query, search_input, size):
        assert query == "pokhara budget"
        return [
            {"document_id": "3", "vector_score": 0.9},
            {"document_id": "1", "vector_score": 0.8},
        ]

    output = pipeline(client, dense).search(SearchInput(query="Pokhara budget", limit=2))

    assert [result.id for result in output.results] == ["1", "3"]  # 1 is in both lists
    assert output.total_hits == 3
    assert output.degraded is False
    first = output.results[0]
    assert first.snippet == "budget of pokhara"
    assert first.geo is not None and first.geo.province_name == "Gandaki"
    # Only the dense-only document is hydrated, without its full text.
    assert client.mget_calls[0]["body"] == {"ids": ["3"]}
    assert client.mget_calls[0]["_source_excludes"] == ["searchable_text"]
    # Every query variant is searched.
    should = client.search_calls[0]["body"]["query"]["bool"]["should"]
    assert len(should) == 2


def test_dense_failure_degrades_instead_of_failing() -> None:
    def broken(query, search_input, size):
        raise RuntimeError("postgres down")

    output = pipeline(FakeOpenSearch([hit("1", 5.0)]), broken).search(SearchInput(query="q"))
    assert [result.id for result in output.results] == ["1"]
    assert output.degraded is True
    assert output.stages["dense"].startswith("failed")


def test_rerank_failure_keeps_the_fusion_order() -> None:
    def broken(candidates, top_k):
        raise ValueError("bad model")

    client = FakeOpenSearch([hit("1", 5.0), hit("2", 4.0)])
    output = pipeline(client, reranker=broken).search(SearchInput(query="q"))
    assert [result.id for result in output.results] == ["1", "2"]
    assert output.degraded is True


def test_opensearch_outage_and_timeout_are_reported_as_such() -> None:
    client = FakeOpenSearch([])
    client.error = OpenSearchConnectionError("N/A", "refused", Exception("refused"))
    with pytest.raises(SearchBackendUnavailable):
        pipeline(client).search(SearchInput(query="q"))
    client.error = ConnectionTimeout("TIMEOUT", "timed out", Exception("slow"))
    with pytest.raises(SearchTimeout):
        pipeline(client).search(SearchInput(query="q"))


def test_an_expired_deadline_stops_before_any_backend_call() -> None:
    client = FakeOpenSearch([hit("1", 1.0)])
    with pytest.raises(SearchTimeout):
        pipeline(client).search(SearchInput(query="q"), deadline=time.monotonic() - 1)
    assert client.search_calls == []


@pytest.mark.parametrize(
    "search_input",
    [
        SearchInput(query=" "),
        SearchInput(query="x" * 513),
        SearchInput(query="q", page=0),
        SearchInput(query="q", limit=101),
        SearchInput(query="q", page=6, limit=100),
    ],
)
def test_requests_outside_the_limits_are_refused(search_input) -> None:
    client = FakeOpenSearch([])
    with pytest.raises(InvalidSearchRequest):
        pipeline(client).search(search_input)
    assert client.search_calls == []


def test_candidate_pool_is_bounded_by_the_window() -> None:
    client = FakeOpenSearch([])
    sizes = []

    def dense(query, search_input, size):
        sizes.append(size)
        return []

    pipeline(client, dense, candidate_pool=50).search(SearchInput(query="q", page=1, limit=10))
    pipeline(client, dense, candidate_pool=50).search(SearchInput(query="q", page=5, limit=100))
    assert [call["body"]["size"] for call in client.search_calls] == [50, 500]
    assert sizes == [50, 500]


def test_dense_only_pages_missing_from_the_index_are_left_out() -> None:
    def dense(query, search_input, size):
        return [{"document_id": "99", "vector_score": 0.99}]

    output = pipeline(FakeOpenSearch([hit("1", 1.0)]), dense).search(SearchInput(query="q"))
    assert [result.id for result in output.results] == ["1"]


def test_filters_match_any_geo_tag_and_ignore_sentinels() -> None:
    filters = build_filter_clauses(
        SearchInput(
            query="q", district_code="D38", ward_number=4, content_type="all", language="NE"
        )
    )
    assert filters == [
        {"term": {"geo_tags.district_code": "D38"}},
        {"term": {"geo_tags.ward_number": 4}},
        {"term": {"language": "ne"}},
    ]
    assert build_filter_clauses(SearchInput(query="q", language="auto")) == []


def _base_expansion(query: str) -> QueryExpansion:
    return QueryExpansion(query.lower(), "en", [query.lower()], False)


def _translating_pipeline(client: FakeOpenSearch, translator, **config: Any):
    return FinalSearchPipeline(
        opensearch_client=client,
        expander=_base_expansion,
        translator=translator,
        config=settings.model_copy(update=config),
    )


def _variants(client: FakeOpenSearch) -> int:
    return len(client.search_calls[0]["body"]["query"]["bool"]["should"])


def test_translated_variant_is_searched_when_ready() -> None:
    client = FakeOpenSearch(hits=[hit("1", 5.0)])
    output = _translating_pipeline(client, lambda query, language: ("पोखरा", False)).search(
        SearchInput(query="Pokhara")
    )

    assert _variants(client) == 2
    assert output.stages["translation"] == "ok"


def test_slow_translation_does_not_delay_the_search() -> None:
    client = FakeOpenSearch(hits=[hit("1", 5.0)])

    def slow(query: str, language: str) -> tuple[str, bool]:
        time.sleep(1.0)
        return "पोखरा", False

    started = time.perf_counter()
    output = _translating_pipeline(client, slow, translation_wait_seconds=0.05).search(
        SearchInput(query="Pokhara")
    )

    assert time.perf_counter() - started < 0.5
    assert _variants(client) == 1
    assert output.stages["translation"] == "skipped: too slow"
    assert output.degraded is False


def test_failed_translation_degrades_but_still_searches() -> None:
    client = FakeOpenSearch(hits=[hit("1", 5.0)])

    def broken(query: str, language: str) -> tuple[str, bool]:
        raise RuntimeError("model crashed")

    output = _translating_pipeline(client, broken).search(SearchInput(query="Pokhara"))

    assert _variants(client) == 1
    assert output.degraded is True
