"""The real gRPC client against an in-process gRPC server with scripted behaviour."""

from __future__ import annotations

import socket
import time
from collections.abc import Callable, Iterator
from concurrent import futures

import grpc
import pytest
from grpc_health.v1.health_pb2 import HealthCheckRequest, HealthCheckResponse

from pgs_api.grpc.client import SEARCH_SERVICE, GrpcSearchClient, SearchError
from pgs_search.grpc.generated.search_pb2 import SearchRequest, SearchResponse

Behaviour = Callable[[SearchRequest, grpc.ServicerContext], SearchResponse]


class FakeEngine:
    """ExecuteSearch runs `behaviour`; Health/Check answers `health` (None: not served)."""

    def __init__(self) -> None:
        self.behaviour: Behaviour = lambda request, context: SearchResponse(total_hits=1)
        self.health: HealthCheckResponse.ServingStatus | None = HealthCheckResponse.SERVING
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        self.server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    SEARCH_SERVICE,
                    {
                        "ExecuteSearch": grpc.unary_unary_rpc_method_handler(
                            self._execute,
                            request_deserializer=SearchRequest.FromString,
                            response_serializer=SearchResponse.SerializeToString,
                        )
                    },
                ),
                grpc.method_handlers_generic_handler(
                    "grpc.health.v1.Health",
                    {
                        "Check": grpc.unary_unary_rpc_method_handler(
                            self._check,
                            request_deserializer=HealthCheckRequest.FromString,
                            response_serializer=HealthCheckResponse.SerializeToString,
                        )
                    },
                ),
            )
        )
        self.port = self.server.add_insecure_port("127.0.0.1:0")
        self.server.start()

    def _execute(self, request: SearchRequest, context: grpc.ServicerContext) -> SearchResponse:
        return self.behaviour(request, context)

    def _check(
        self, request: HealthCheckRequest, context: grpc.ServicerContext
    ) -> HealthCheckResponse:
        if self.health is None:
            context.abort(grpc.StatusCode.UNIMPLEMENTED, "no health service")
        assert request.service == SEARCH_SERVICE
        return HealthCheckResponse(status=self.health)


@pytest.fixture
def engine() -> Iterator[FakeEngine]:
    fake = FakeEngine()
    yield fake
    fake.server.stop(grace=None)


@pytest.fixture
def client(engine: FakeEngine) -> Iterator[GrpcSearchClient]:
    search = GrpcSearchClient(f"127.0.0.1:{engine.port}", timeout_seconds=0.5)
    yield search
    search.close()


def _abort(code: grpc.StatusCode, details: str) -> Behaviour:
    def behaviour(request: SearchRequest, context: grpc.ServicerContext) -> SearchResponse:
        context.abort(code, details)

    return behaviour


def test_search_returns_the_engine_response(engine: FakeEngine, client: GrpcSearchClient) -> None:
    seen: list[SearchRequest] = []

    def behaviour(request: SearchRequest, context: grpc.ServicerContext) -> SearchResponse:
        seen.append(request)
        return SearchResponse(total_hits=3, query_language="en")

    engine.behaviour = behaviour

    response = client.search(SearchRequest(query="kathmandu", limit=5))

    assert (response.total_hits, response.query_language) == (3, "en")
    assert seen[0].query == "kathmandu"


@pytest.mark.parametrize(
    ("code", "status", "detail"),
    [
        (grpc.StatusCode.INVALID_ARGUMENT, 400, "Query must not be empty."),
        (grpc.StatusCode.UNAVAILABLE, 503, "The search engine is unavailable. Try again shortly."),
        (grpc.StatusCode.INTERNAL, 502, "The search engine failed to answer the request."),
        (
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            502,
            "The search engine failed to answer the request.",
        ),
    ],
)
def test_status_codes_map_to_http(
    engine: FakeEngine,
    client: GrpcSearchClient,
    code: grpc.StatusCode,
    status: int,
    detail: str,
) -> None:
    engine.behaviour = _abort(code, "Query must not be empty.")

    with pytest.raises(SearchError) as caught:
        client.search(SearchRequest(query="x"))

    assert (caught.value.http_status, caught.value.detail) == (status, detail)
    assert caught.value.grpc_code == code.name


def test_every_call_has_a_deadline(engine: FakeEngine, client: GrpcSearchClient) -> None:
    def slow(request: SearchRequest, context: grpc.ServicerContext) -> SearchResponse:
        time.sleep(2)
        return SearchResponse()

    engine.behaviour = slow
    started = time.monotonic()

    with pytest.raises(SearchError) as caught:
        client.search(SearchRequest(query="x"))

    assert caught.value.http_status == 504
    assert caught.value.detail == "The search engine did not answer within 0.5 s."
    assert time.monotonic() - started < 1.5


def _closed_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_unreachable_engine_is_503() -> None:
    search = GrpcSearchClient(f"127.0.0.1:{_closed_port()}", timeout_seconds=2)
    try:
        with pytest.raises(SearchError) as caught:
            search.search(SearchRequest(query="x"))
        assert caught.value.http_status == 503
        assert search.health().status == "degraded"
    finally:
        search.close()


def test_health_reports_serving(client: GrpcSearchClient) -> None:
    health = client.health()

    assert (health.status, health.detail) == ("ok", "SERVING")


def test_health_reports_not_serving_as_degraded(
    engine: FakeEngine, client: GrpcSearchClient
) -> None:
    engine.health = HealthCheckResponse.NOT_SERVING

    assert (client.health().status, client.health().detail) == ("degraded", "NOT_SERVING")


def test_health_without_health_service_is_degraded(
    engine: FakeEngine, client: GrpcSearchClient
) -> None:
    engine.health = None

    health = client.health()

    assert health.status == "degraded"
    assert "grpc.health.v1" in health.detail
