"""`/health/live` and `/health/ready` responses."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class Liveness(BaseModel):
    status: Literal["ok"] = "ok"


class SearchCheck(BaseModel):
    status: Literal["ok", "degraded"]
    detail: str


class ReadinessChecks(BaseModel):
    database: dict[str, Any] = Field(description="pgs_db.health.check")
    search: SearchCheck


class ReadinessReport(BaseModel):
    status: Literal["ok", "degraded", "failing"]
    checks: ReadinessChecks
