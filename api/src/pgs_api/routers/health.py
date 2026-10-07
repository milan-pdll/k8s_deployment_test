"""Liveness and readiness probes."""

from __future__ import annotations

from fastapi import APIRouter, Response, status

from ..core.services import ServicesDep
from ..schemas.health import Liveness, ReadinessChecks, ReadinessReport, SearchCheck

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live", response_model=Liveness, summary="The process is up")
def live() -> Liveness:
    return Liveness()


@router.get(
    "/ready",
    response_model=ReadinessReport,
    summary="The API can serve: database ok or degraded (search is reported, never fatal)",
    responses={503: {"model": ReadinessReport, "description": "Database failing"}},
)
def ready(services: ServicesDep, response: Response) -> ReadinessReport:
    report = services.readiness.check()
    response.headers["Cache-Control"] = "no-store"
    if report.status == "failing":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessReport(
        status=report.status,
        checks=ReadinessChecks(
            database=report.database,
            search=SearchCheck(status=report.search.status, detail=report.search.detail),
        ),
    )
