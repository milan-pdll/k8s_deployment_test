"""`GET /api/v1/search`: validate, call the search engine over gRPC, map, log."""

from __future__ import annotations

import logging
import time
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query

from pgs_search.grpc.generated.search_pb2 import SearchRequest as GrpcSearchRequest
from pgs_search.grpc.generated.search_pb2 import SearchResponse as GrpcSearchResponse
from pgs_search.grpc.generated.search_pb2 import SearchResultItem

from ..core.services import ServicesDep
from ..db.database import SearchLogEntry
from ..grpc.client import SearchError
from ..schemas.search import GeoRef, SearchParams, SearchResponse, SearchResult

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["search"])

_ERRORS: dict[int | str, dict[str, Any]] = {
    400: {"description": "Invalid parameters (or rejected by the search engine)"},
    502: {"description": "The search engine failed"},
    503: {"description": "The search engine is unavailable"},
    504: {"description": "The search engine did not answer in time"},
}


def to_grpc_request(params: SearchParams) -> GrpcSearchRequest:
    return GrpcSearchRequest(
        query=params.q,
        province_code=params.province_code or "",
        district_code=params.district_code or "",
        municipality_id=params.municipality_id or "",
        ward_number=params.ward_number or 0,
        content_type=params.content_type,
        language=params.language,
        page=params.page,
        limit=params.limit,
    )


def _geo(item: SearchResultItem) -> GeoRef | None:
    if not item.HasField("geo"):
        return None
    ref = item.geo
    geo = GeoRef(
        province_code=ref.province_code or None,
        province_name=ref.province_name or None,
        district_code=ref.district_code or None,
        district_name=ref.district_name or None,
        municipality_id=ref.municipality_id or None,
        municipality_name=ref.municipality_name or None,
        ward_number=ref.ward_number or None,
    )
    return geo if geo.model_dump(exclude_none=True) else None


def to_result(item: SearchResultItem) -> SearchResult:
    return SearchResult(
        id=item.id,
        title=item.title,
        url=item.url,
        domain=item.domain,
        snippet=item.snippet,
        result_type=item.result_type,
        language=item.language or "unknown",
        published_at=item.published_at or None,
        relevance_score=item.relevance_score,
        geo=_geo(item),
    )


def to_response(params: SearchParams, response: GrpcSearchResponse, took_ms: int) -> SearchResponse:
    return SearchResponse(
        query=params.q,
        page=params.page,
        limit=params.limit,
        total_hits=response.total_hits,
        took_ms=took_ms,
        query_language=response.query_language or "unknown",
        degraded=response.degraded,
        results=[to_result(item) for item in response.results],
    )


@router.get(
    "/search",
    response_model=SearchResponse,
    summary="Full-text and semantic search with optional geo, language and type filters",
    responses=_ERRORS,
)
def search(
    params: Annotated[SearchParams, Query()],
    services: ServicesDep,
) -> SearchResponse:
    started = time.perf_counter()
    try:
        response = services.search.search(to_grpc_request(params))
    except SearchError as exc:
        logger.warning(
            "search failed",
            extra={"grpc_code": exc.grpc_code, "http_status": exc.http_status},
        )
        headers = {"Retry-After": "5"} if exc.http_status == 503 else None
        raise HTTPException(exc.http_status, detail=exc.detail, headers=headers) from None
    took_ms = int((time.perf_counter() - started) * 1000)
    body = to_response(params, response, took_ms)
    services.search_log.submit(
        SearchLogEntry(
            query_text=params.q,
            result_count=body.total_hits,
            query_language=body.query_language,
            filters=params.filters(),
            page_number=params.page,
            latency_ms=took_ms,
        )
    )
    return body
