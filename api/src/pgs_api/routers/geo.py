"""Gazetteer reads for the map: the region tree and per-region content counts."""

from __future__ import annotations

import re
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Query, Response

from pgs_db.schemas import GeoContentCount, ProvinceNode

from ..core.errors import invalid_parameter
from ..core.services import ServicesDep
from ..schemas.geo import DISTRICT_CODE, PROVINCE_CODE

router = APIRouter(prefix="/api/v1/geo", tags=["geo"])

GeoLevel = Literal["province", "district", "local_body"]

# level -> (pattern its `within` must match, what that parent is)
_PARENT: dict[str, tuple[str, str] | None] = {
    "province": None,
    "district": (PROVINCE_CODE, "a province code (P1-P7)"),
    "local_body": (DISTRICT_CODE, "a district code (D01-D77)"),
}


@router.get(
    "/hierarchy",
    response_model=list[ProvinceNode],
    summary="Provinces -> districts -> local bodies, ordered by code",
)
def hierarchy(services: ServicesDep, response: Response) -> list[dict[str, Any]]:
    response.headers["Cache-Control"] = "public, max-age=3600"
    return services.db.hierarchy()


@router.get(
    "/content-stats",
    response_model=list[GeoContentCount],
    summary="Indexed content per region of one level, for colouring the map",
    description=(
        "One row per region of `level` (every region, zeros included), ordered by code. "
        "`within` limits districts to one province (`P4`) or local bodies to one district "
        "(`D38`). Counts are the Gold table `geo_content_stats`, refreshed every 15 minutes."
    ),
    responses={400: {"description": "Invalid level/within combination"}},
)
def content_stats(
    services: ServicesDep,
    response: Response,
    level: Annotated[GeoLevel, Query(description="Region level")] = "province",
    within: Annotated[
        str | None, Query(max_length=16, description="Parent region code", examples=["P4"])
    ] = None,
) -> list[dict[str, Any]]:
    parent_code = within.strip().upper() if within and within.strip() else None
    parent = _PARENT[level]
    if parent_code is not None:
        if parent is None:
            raise invalid_parameter("query", "within", "provinces have no parent region")
        pattern, expected = parent
        if not re.match(pattern, parent_code):
            raise invalid_parameter("query", "within", f"for level={level}, use {expected}")
    response.headers["Cache-Control"] = "public, max-age=60"
    return services.db.geo_content(level, parent_code)
