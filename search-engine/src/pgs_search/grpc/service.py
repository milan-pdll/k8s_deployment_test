"""gRPC SearchService: request validation, deadlines and error mapping around the pipeline."""

from __future__ import annotations

import logging
import time
from typing import Protocol

import grpc

from pgs_search.config import settings
from pgs_search.grpc.generated import search_pb2, search_pb2_grpc
from pgs_search.pipeline import (
    InvalidSearchRequest,
    PipelineHit,
    PipelineOutput,
    SearchBackendUnavailable,
    SearchInput,
    SearchTimeout,
)

logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 10


class SearchPipeline(Protocol):
    def search(
        self, search_input: SearchInput, deadline: float | None = None
    ) -> PipelineOutput: ...


def _to_item(hit: PipelineHit) -> search_pb2.SearchResultItem:
    item = search_pb2.SearchResultItem(
        id=hit.id,
        result_type=hit.result_type,
        title=hit.title,
        url=hit.url,
        domain=hit.domain,
        snippet=hit.snippet,
        download_url=hit.download_url,
        file_size_bytes=hit.file_size_bytes,
        relevance_score=hit.relevance_score,
        language=hit.language,
        published_at=hit.published_at,
    )
    if hit.geo is not None:
        item.geo.CopyFrom(
            search_pb2.GeoRef(
                province_code=hit.geo.province_code,
                province_name=hit.geo.province_name,
                district_code=hit.geo.district_code,
                district_name=hit.geo.district_name,
                municipality_id=hit.geo.municipality_id,
                municipality_name=hit.geo.municipality_name,
                ward_number=hit.geo.ward_number,
            )
        )
    return item


def search_input_from_request(request: search_pb2.SearchRequest) -> SearchInput:
    """The request as pipeline input: 0/negative limit -> the default, limits above the
    maximum are clamped (as before); page < 1 -> 1. Range checks are the pipeline's."""
    limit = request.limit if request.limit > 0 else DEFAULT_LIMIT
    return SearchInput(
        query=request.query.strip(),
        province_code=request.province_code.strip(),
        district_code=request.district_code.strip(),
        municipality_id=request.municipality_id.strip(),
        ward_number=max(request.ward_number, 0),
        content_type=request.content_type.strip(),
        language=request.language.strip(),
        page=max(request.page, 1),
        limit=min(limit, settings.max_limit),
    )


class SearchService(search_pb2_grpc.SearchServiceServicer):
    def __init__(self, pipeline: SearchPipeline) -> None:
        self._pipeline = pipeline

    def ExecuteSearch(
        self,
        request: search_pb2.SearchRequest,
        context: grpc.ServicerContext,
    ) -> search_pb2.SearchResponse:
        search_input = search_input_from_request(request)
        remaining = context.time_remaining()
        deadline = time.monotonic() + remaining if remaining is not None else None
        started = time.perf_counter()
        try:
            output = self._pipeline.search(search_input, deadline=deadline)
        except InvalidSearchRequest as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except SearchTimeout as exc:
            context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, str(exc))
        except SearchBackendUnavailable as exc:
            logger.warning("search unavailable: %s", exc)
            context.abort(grpc.StatusCode.UNAVAILABLE, "the search index is unavailable")
        except Exception:
            logger.exception("unexpected error while searching")
            context.abort(grpc.StatusCode.INTERNAL, "search failed")
        took_ms = int((time.perf_counter() - started) * 1000)
        logger.info(
            "search lang=%s results=%d/%d degraded=%s took_ms=%d stages=%s",
            output.query_language,
            len(output.results),
            output.total_hits,
            output.degraded,
            took_ms,
            output.stages,
        )
        return search_pb2.SearchResponse(
            status_code=200,
            total_hits=output.total_hits,
            execution_time_ms=took_ms,
            results=[_to_item(hit) for hit in output.results],
            query_language=output.query_language,
            degraded=output.degraded,
        )
