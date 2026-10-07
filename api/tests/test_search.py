from __future__ import annotations

import time
from typing import Any

import pytest

from pgs_api.db.database import SearchLogEntry
from pgs_api.db.search_log import SearchLogWriter
from pgs_api.grpc.client import SearchError
from pgs_search.grpc.generated.search_pb2 import GeoRef, SearchResponse, SearchResultItem

from .conftest import FakeDatabase, Harness


def _response() -> SearchResponse:
    return SearchResponse(
        status_code=200,
        total_hits=2,
        execution_time_ms=12,
        query_language="ne",
        degraded=True,
        results=[
            SearchResultItem(
                id="17",
                result_type="web_page",
                title="पोखरा महानगरपालिका",
                url="https://pokharamun.gov.np/",
                domain="pokharamun.gov.np",
                snippet="Notice",
                relevance_score=0.75,
                language="ne",
                published_at="2026-09-01T00:00:00Z",
                geo=GeoRef(
                    province_code="P4",
                    province_name="Gandaki",
                    district_code="D38",
                    district_name="Kaski",
                    municipality_id="MUN414",
                    municipality_name="Pokhara",
                    ward_number=0,
                ),
            ),
            SearchResultItem(id="18", title="No geo", url="https://example.gov.np/x"),
        ],
    )


def _errors(body: dict[str, Any]) -> list[str]:
    return [".".join(str(part) for part in error["loc"]) for error in body["detail"]]


def test_search_maps_the_engine_response(harness: Harness) -> None:
    harness.search.response = _response()

    response = harness.client.get("/api/v1/search", params={"q": "  pokhara  "})

    assert response.status_code == 200
    body = response.json()
    assert body["query"] == "pokhara"
    assert (body["page"], body["limit"], body["total_hits"]) == (1, 10, 2)
    assert body["query_language"] == "ne"
    assert body["degraded"] is True
    assert isinstance(body["took_ms"], int)
    first, second = body["results"]
    assert first == {
        "id": "17",
        "title": "पोखरा महानगरपालिका",
        "url": "https://pokharamun.gov.np/",
        "domain": "pokharamun.gov.np",
        "snippet": "Notice",
        "result_type": "web_page",
        "language": "ne",
        "published_at": "2026-09-01T00:00:00Z",
        "relevance_score": 0.75,
        "geo": {
            "province_code": "P4",
            "province_name": "Gandaki",
            "district_code": "D38",
            "district_name": "Kaski",
            "municipality_id": "MUN414",
            "municipality_name": "Pokhara",
            "ward_number": None,
        },
    }
    # Unset proto fields: no geo, unknown language, no date.
    assert second["geo"] is None
    assert second["language"] == "unknown"
    assert second["published_at"] is None


def test_search_forwards_filters_to_the_engine(harness: Harness) -> None:
    response = harness.client.get(
        "/api/v1/search",
        params={
            "q": "budget",
            "page": "2",
            "limit": "20",
            "province_code": "p4",
            "district_code": "D38",
            "municipality_id": "mun414",
            "ward_number": "7",
            "language": "NE",
            "content_type": "document",
        },
    )

    assert response.status_code == 200
    (request,) = harness.search.requests
    assert request.query == "budget"
    assert (request.page, request.limit) == (2, 20)
    assert (request.province_code, request.district_code, request.municipality_id) == (
        "P4",
        "D38",
        "MUN414",
    )
    assert request.ward_number == 7
    assert (request.language, request.content_type) == ("ne", "document")


def test_empty_filters_mean_no_filter(harness: Harness) -> None:
    response = harness.client.get(
        "/api/v1/search?q=x&page=1&limit=10&province_code=&district_code=&municipality_id="
        "&ward_number=&language=&content_type="
    )

    assert response.status_code == 200
    (request,) = harness.search.requests
    assert (request.province_code, request.district_code, request.municipality_id) == ("", "", "")
    assert request.ward_number == 0
    assert (request.language, request.content_type) == ("auto", "all")


@pytest.mark.parametrize(
    ("query", "location"),
    [
        ("", "query.q"),
        ("q=", "query.q"),
        ("q=%20%20%20", "query.q"),
        ("q=" + "a" * 513, "query.q"),
        ("q=x&limit=0", "query.limit"),
        ("q=x&limit=101", "query.limit"),
        ("q=x&page=0", "query.page"),
        ("q=x&page=abc", "query.page"),
        ("q=x&page=6&limit=100", "query"),
        ("q=x&page=51", "query"),
        ("q=x&province_code=P9", "query.province_code"),
        ("q=x&district_code=D78", "query.district_code"),
        ("q=x&municipality_id=Pokhara", "query.municipality_id"),
        ("q=x&ward_number=0", "query.ward_number"),
        ("q=x&ward_number=100", "query.ward_number"),
        ("q=x&language=fr", "query.language"),
        ("q=x&content_type=web%20page", "query.content_type"),
    ],
)
def test_invalid_parameters_are_400(harness: Harness, query: str, location: str) -> None:
    response = harness.client.get(f"/api/v1/search?{query}")

    assert response.status_code == 400
    assert location in _errors(response.json())
    assert harness.search.requests == []
    assert harness.search_log.entries == []


def test_limits_at_their_bounds_are_accepted(harness: Harness) -> None:
    for query in ("q=" + "a" * 512, "q=x&page=50&limit=10", "q=x&page=5&limit=100"):
        assert harness.client.get(f"/api/v1/search?{query}").status_code == 200


def test_validation_errors_do_not_echo_input(harness: Harness) -> None:
    response = harness.client.get("/api/v1/search?q=x&limit=secret-looking-value")

    assert response.status_code == 400
    assert "secret-looking-value" not in response.text
    assert set(response.json()["detail"][0]) == {"loc", "msg", "type"}


@pytest.mark.parametrize(
    ("status", "grpc_code"),
    [
        (400, "INVALID_ARGUMENT"),
        (503, "UNAVAILABLE"),
        (504, "DEADLINE_EXCEEDED"),
        (502, "INTERNAL"),
    ],
)
def test_engine_errors_map_to_http(harness: Harness, status: int, grpc_code: str) -> None:
    harness.search.error = SearchError(status, f"engine said {grpc_code}", grpc_code)

    response = harness.client.get("/api/v1/search", params={"q": "x"})

    assert response.status_code == status
    assert response.json() == {"detail": f"engine said {grpc_code}"}
    assert (response.headers.get("retry-after") == "5") is (status == 503)
    assert harness.search_log.entries == []


def test_answered_search_is_logged(harness: Harness) -> None:
    harness.search.response = _response()

    harness.client.get(
        "/api/v1/search", params={"q": "Pokhara ", "page": 2, "district_code": "D38"}
    )

    (entry,) = harness.search_log.entries
    assert entry.query_text == "Pokhara"
    assert entry.result_count == 2
    assert entry.query_language == "ne"
    assert entry.page_number == 2
    assert entry.filters == {"district_code": "D38"}
    assert entry.latency_ms >= 0


def test_failed_search_log_never_fails_the_request(harness: Harness) -> None:
    broken = FakeDatabase(log_error=RuntimeError("database is down"))
    writer = SearchLogWriter(broken)
    harness.services.search_log = writer

    response = harness.client.get("/api/v1/search", params={"q": "x"})

    writer.close()
    assert response.status_code == 200
    assert broken.logged == []


def test_search_log_writer_drops_entries_beyond_its_backlog() -> None:
    class SlowDatabase(FakeDatabase):
        def log_search(self, entry: SearchLogEntry) -> None:
            time.sleep(0.2)
            super().log_search(entry)

    db = SlowDatabase()
    writer = SearchLogWriter(db, workers=1, max_pending=2)
    entry = SearchLogEntry("x", 0, None, {}, 1, 5)

    accepted = [writer.submit(entry) for _ in range(5)]
    writer.close()

    assert accepted == [True, True, False, False, False]
    assert len(db.logged) == 2
