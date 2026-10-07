"""The gRPC contract, over a real in-process server: fields, status codes, health."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import grpc
import pytest
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from pgs_search.grpc import server as server_module
from pgs_search.grpc.generated import search_pb2, search_pb2_grpc
from pgs_search.grpc.service import search_input_from_request
from pgs_search.pipeline import (
    GeoRef,
    InvalidSearchRequest,
    PipelineHit,
    PipelineOutput,
    SearchBackendUnavailable,
    SearchInput,
    SearchTimeout,
)


class FakePipeline:
    def __init__(self) -> None:
        self.error: Exception | None = None
        self.calls: list[tuple[SearchInput, float | None]] = []

    def search(self, search_input: SearchInput, deadline: float | None = None) -> PipelineOutput:
        self.calls.append((search_input, deadline))
        if self.error is not None:
            raise self.error
        hit = PipelineHit(
            id="42",
            result_type="web_page",
            title="Pokhara budget",
            url="https://pokharamun.gov.np/budget",
            domain="pokharamun.gov.np",
            snippet="Budget for the year",
            download_url="",
            file_size_bytes=0,
            relevance_score=0.75,
            language="ne",
            published_at="2026-09-30T08:00:00+00:00",
            geo=GeoRef(province_code="P4", district_code="D38", municipality_id="MUN414"),
        )
        return PipelineOutput(total_hits=1, results=[hit], query_language="en", degraded=True)


@pytest.fixture
def grpc_stack() -> Iterator[tuple[Any, Any, FakePipeline]]:
    pipeline = FakePipeline()
    health_servicer = health.HealthServicer()
    server = server_module.build_server(pipeline, health_servicer)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    health_servicer.set(server_module.SERVICE_NAME, health_pb2.HealthCheckResponse.SERVING)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        yield (
            search_pb2_grpc.SearchServiceStub(channel),
            health_pb2_grpc.HealthStub(channel),
            pipeline,
        )
    finally:
        channel.close()
        server.stop(grace=None)


def test_results_carry_every_contract_field(grpc_stack) -> None:
    stub, _, pipeline = grpc_stack
    response = stub.ExecuteSearch(search_pb2.SearchRequest(query=" budget ", limit=5), timeout=5)

    assert response.status_code == 200
    assert response.total_hits == 1
    assert response.query_language == "en"
    assert response.degraded is True
    item = response.results[0]
    assert (item.id, item.title, item.language) == ("42", "Pokhara budget", "ne")
    assert item.published_at == "2026-09-30T08:00:00+00:00"
    assert (item.geo.province_code, item.geo.district_code) == ("P4", "D38")
    search_input, deadline = pipeline.calls[0]
    assert search_input.query == "budget"
    assert deadline is not None  # the client deadline reaches the pipeline


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (InvalidSearchRequest("page * limit may not exceed 500"), grpc.StatusCode.INVALID_ARGUMENT),
        (SearchTimeout("late"), grpc.StatusCode.DEADLINE_EXCEEDED),
        (SearchBackendUnavailable("down"), grpc.StatusCode.UNAVAILABLE),
        (RuntimeError("bug"), grpc.StatusCode.INTERNAL),
    ],
)
def test_failures_are_grpc_status_codes(grpc_stack, error, code) -> None:
    stub, _, pipeline = grpc_stack
    pipeline.error = error
    with pytest.raises(grpc.RpcError) as caught:
        stub.ExecuteSearch(search_pb2.SearchRequest(query="budget"), timeout=5)
    assert caught.value.code() == code
    if code == grpc.StatusCode.INTERNAL:
        assert "bug" not in caught.value.details()  # internals are not leaked


def test_health_service_reports_serving(grpc_stack) -> None:
    _, health_stub, _ = grpc_stack
    response = health_stub.Check(
        health_pb2.HealthCheckRequest(service=server_module.SERVICE_NAME), timeout=5
    )
    assert response.status == health_pb2.HealthCheckResponse.SERVING


def test_request_normalization() -> None:
    request = search_pb2.SearchRequest(query="  q  ", page=0, limit=0, ward_number=-3)
    search_input = search_input_from_request(request)
    assert (search_input.query, search_input.page, search_input.limit) == ("q", 1, 10)
    assert search_input.ward_number == 0
    capped = search_input_from_request(search_pb2.SearchRequest(query="q", limit=1000))
    assert capped.limit == 100
