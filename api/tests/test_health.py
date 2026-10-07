from __future__ import annotations

from sqlalchemy.exc import OperationalError

from pgs_api.grpc.client import SearchHealth
from pgs_api.health.readiness import ReadinessProbe

from .conftest import FakeDatabase, FakeSearch, Harness


def test_live(harness: Harness) -> None:
    response = harness.client.get("/health/live")

    assert (response.status_code, response.json()) == (200, {"status": "ok"})


def test_ready_when_everything_is_ok(harness: Harness) -> None:
    response = harness.client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "checks": {
            "database": {"status": "ok", "problems": []},
            "search": {"status": "ok", "detail": "SERVING"},
        },
    }


def test_search_engine_down_degrades_but_stays_ready(harness: Harness) -> None:
    harness.search.health_status = SearchHealth("degraded", "health check failed: UNAVAILABLE")

    response = harness.client.get("/health/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "degraded"
    assert body["checks"]["search"]["status"] == "degraded"


def test_degraded_database_stays_ready(harness: Harness) -> None:
    harness.db.health_report = {"status": "degraded", "problems": ["page_scores never computed"]}

    response = harness.client.get("/health/ready")

    assert (response.status_code, response.json()["status"]) == (200, "degraded")


def test_failing_database_is_503(harness: Harness) -> None:
    harness.db.health_report = {"status": "failing", "problems": ["schema at x"]}

    response = harness.client.get("/health/ready")

    assert (response.status_code, response.json()["status"]) == (503, "failing")


def test_unreachable_database_is_503_without_details(harness: Harness) -> None:
    harness.db.health_error = OperationalError(
        "SELECT 1", {}, Exception("password authentication failed for user pgs_api")
    )

    response = harness.client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["database"] == {
        "status": "failing",
        "problems": ["database unreachable or check failed"],
    }
    assert "password" not in response.text


def test_readiness_is_cached_briefly() -> None:
    db = FakeDatabase()
    now = [100.0]
    probe = ReadinessProbe(db, FakeSearch(), cache_seconds=5, clock=lambda: now[0])

    probe.check()
    probe.check()
    now[0] += 6
    probe.check()

    assert db.health_calls == 2
