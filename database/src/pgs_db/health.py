"""Database health for the API's readiness probe and the admin dashboard.

`check(session)` answers "can the services run against this database right now?":
the schema is at the revision this package expects, the extensions are installed,
the reference data is seeded, and the Gold summaries are not stale. It never raises
for an unhealthy database; it reports. `status` is "ok", "degraded" (serving works,
something needs attention) or "failing" (services should not start).
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from .models import District, DomainStats, GeoContentStats, LocalBody, PageScore, Province

# The Alembic head this package's models match. tests/test_hardening.py fails if a
# migration is added without updating it.
EXPECTED_REVISION = "b8c9d0e1f2a3"

EXPECTED_COUNTS = {"provinces": 7, "districts": 77, "local_bodies": 753}
STALE_AFTER = timedelta(hours=6)


def check(session: Session, *, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    failing: list[str] = []
    degraded: list[str] = []

    revision = session.scalar(text("SELECT version_num FROM alembic_version"))
    if revision != EXPECTED_REVISION:
        failing.append(f"schema at {revision}, package expects {EXPECTED_REVISION}")

    extensions = set(session.scalars(text("SELECT extname FROM pg_extension")))
    for name in ("vector", "postgis"):
        if name not in extensions:
            failing.append(f"extension {name} is not installed")

    counts = {
        "provinces": session.scalar(select(func.count()).select_from(Province)),
        "districts": session.scalar(select(func.count()).select_from(District)),
        "local_bodies": session.scalar(select(func.count()).select_from(LocalBody)),
    }
    for table, expected in EXPECTED_COUNTS.items():
        if counts[table] != expected:
            failing.append(
                f"{table} has {counts[table]} rows, expected {expected}: seed_geography.py"
            )
    if not session.scalar(select(func.count()).where(LocalBody.boundary.is_not(None))):
        degraded.append("boundaries not loaded: seed_boundaries.py")

    freshness: dict[str, datetime | None] = {}
    for label, column in (
        ("domain_stats", DomainStats.computed_at),
        ("geo_content_stats", GeoContentStats.computed_at),
        ("page_scores", PageScore.computed_at),
    ):
        latest = session.scalar(select(func.max(column)))
        freshness[label] = latest
        if latest is None:
            degraded.append(f"{label} never computed: python -m pgs_db.jobs all")
        elif now - latest > STALE_AFTER:
            degraded.append(f"{label} last computed {latest.isoformat()}: is the job scheduled?")

    status = "failing" if failing else "degraded" if degraded else "ok"
    return {
        "status": status,
        "revision": revision,
        "expected_revision": EXPECTED_REVISION,
        "reference_counts": counts,
        "gold_computed_at": freshness,
        "problems": failing + degraded,
    }
