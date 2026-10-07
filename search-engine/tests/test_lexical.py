from __future__ import annotations

from typing import Any

from pgs_search.config import settings
from pgs_search.retrieval.lexical import build_query, search_bm25


class FakeClient:
    def __init__(self, hits: list[dict[str, Any]] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.hits = hits or []

    def search(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {"hits": {"hits": self.hits}}


def test_one_should_clause_per_variant_and_filters_are_exact() -> None:
    filters = [{"term": {"geo_tags.district_code": "D38"}}]
    body = build_query(["pokhara budget", "पोखरा बजेट"], 50, filters, 2.5)

    should = body["query"]["bool"]["should"]
    assert [clause["multi_match"]["query"] for clause in should] == [
        "pokhara budget",
        "पोखरा बजेट",
    ]
    assert body["query"]["bool"]["minimum_should_match"] == 1
    assert body["query"]["bool"]["filter"] == filters
    assert body["size"] == 50
    assert body["timeout"] == "2500ms"


def test_full_text_is_not_shipped_back_per_hit() -> None:
    body = build_query(["q"], 10)
    assert body["_source"] == {"excludes": ["searchable_text"]}
    assert "searchable_text" in body["highlight"]["fields"]
    assert body["track_total_hits"] is False


def test_search_uses_the_alias_and_a_request_timeout() -> None:
    client = FakeClient(hits=[{"_id": "1"}])
    hits = search_bm25(client, ["q"], 10, None, 3.0)
    assert hits == [{"_id": "1"}]
    call = client.calls[0]
    assert call["index"] == settings.opensearch_index
    assert call["request_timeout"] == 3.0


def test_blank_variants_or_no_room_mean_no_request() -> None:
    client = FakeClient()
    assert search_bm25(client, ["", "  "], 10) == []
    assert search_bm25(client, ["q"], 0) == []
    assert client.calls == []
