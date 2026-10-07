"""`GET /api/v1/admin/summary`: the dashboard's database-backed figures."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Response

from ..auth.dependencies import CurrentAdmin
from ..core.services import ServicesDep
from ..schemas.admin import AdminSummary

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])

TRAFFIC_WINDOW = timedelta(hours=24)


@router.get(
    "/summary",
    response_model=AdminSummary,
    summary="Domains, Bronze queue, quarantine, errors (24 h) and search traffic (24 h)",
    responses={
        401: {"description": "Missing, invalid or expired token"},
        503: {"description": "API_AUTH_SECRET is not set on the server"},
    },
)
def summary(_admin: CurrentAdmin, services: ServicesDep, response: Response) -> AdminSummary:
    response.headers["Cache-Control"] = "no-store"
    until = datetime.now(UTC)
    since = until - TRAFFIC_WINDOW
    traffic = services.db.search_traffic(since, until)
    return AdminSummary.model_validate(
        {
            **services.db.dashboard_summary(),
            "search_traffic": {**traffic, "since": since, "until": until},
        }
    )
