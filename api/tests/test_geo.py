from __future__ import annotations

import pytest

from .conftest import Harness


def test_hierarchy(harness: Harness) -> None:
    response = harness.client.get("/api/v1/geo/hierarchy")

    assert response.status_code == 200
    (province,) = response.json()
    assert province["code"] == "P4"
    assert province["districts"][0]["local_bodies"][0] == {
        "code": "MUN414",
        "name_en": "Pokhara",
        "name_ne": "पोखरा",
        "type": "METROPOLITAN_CITY",
    }
    assert response.headers["cache-control"] == "public, max-age=3600"


def test_content_stats_defaults_to_provinces(harness: Harness) -> None:
    response = harness.client.get("/api/v1/geo/content-stats")

    assert response.status_code == 200
    assert response.json()[0]["page_count"] == 5
    assert harness.db.geo_calls == [("province", None)]


@pytest.mark.parametrize(
    ("query", "call"),
    [
        ("level=district&within=p4", ("district", "P4")),
        ("level=local_body&within=D38", ("local_body", "D38")),
        ("level=district&within=", ("district", None)),
    ],
)
def test_content_stats_within_a_parent(
    harness: Harness, query: str, call: tuple[str, str | None]
) -> None:
    assert harness.client.get(f"/api/v1/geo/content-stats?{query}").status_code == 200
    assert harness.db.geo_calls == [call]


@pytest.mark.parametrize(
    "query",
    [
        "level=ward",
        "level=province&within=P4",
        "level=district&within=D38",
        "level=local_body&within=P4",
        "level=district&within=" + "P" * 20,
    ],
)
def test_content_stats_rejects_bad_combinations(harness: Harness, query: str) -> None:
    response = harness.client.get(f"/api/v1/geo/content-stats?{query}")

    assert response.status_code == 400
    assert harness.db.geo_calls == []
