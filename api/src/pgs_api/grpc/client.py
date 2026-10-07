"""gRPC client for the search engine (`search.engine.v1.SearchService`).

One channel per process, created at startup and closed at shutdown. Every call carries
a deadline. gRPC failures become `SearchError` with the HTTP status the gateway answers:

| gRPC status       | HTTP |
|-------------------|------|
| INVALID_ARGUMENT  | 400  |
| UNAVAILABLE       | 503  |
| DEADLINE_EXCEEDED | 504  |
| anything else     | 502  |

`health()` asks the standard `grpc.health.v1.Health` service with a short deadline and
never raises: the search engine being down makes the API degraded, not unready.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Literal, Protocol

import grpc
from grpc_health.v1.health_pb2 import HealthCheckRequest, HealthCheckResponse

from pgs_search.grpc.generated.search_pb2 import SearchRequest, SearchResponse

SEARCH_SERVICE = "search.engine.v1.SearchService"
_EXECUTE_SEARCH = f"/{SEARCH_SERVICE}/ExecuteSearch"
_HEALTH_CHECK = "/grpc.health.v1.Health/Check"
HEALTH_TIMEOUT_SECONDS = 1.0

# One transparent retry of a search that failed with UNAVAILABLE (a connection blip),
# throttled so an outage does not double the load. The call's deadline still bounds it.
_SERVICE_CONFIG = {
    "methodConfig": [
        {
            "name": [{"service": SEARCH_SERVICE}],
            "retryPolicy": {
                "maxAttempts": 2,
                "initialBackoff": "0.1s",
                "maxBackoff": "0.5s",
                "backoffMultiplier": 2,
                "retryableStatusCodes": ["UNAVAILABLE"],
            },
        }
    ],
    "retryThrottling": {"maxTokens": 10, "tokenRatio": 0.1},
}

CHANNEL_OPTIONS: tuple[tuple[str, int | str], ...] = (
    # Ping only while calls are in flight; idle pings would trip the server's
    # too_many_pings protection.
    ("grpc.keepalive_time_ms", 60_000),
    ("grpc.keepalive_timeout_ms", 10_000),
    ("grpc.keepalive_permit_without_calls", 0),
    # Reconnect within seconds once a restarted search engine is back (default: up to 120 s).
    ("grpc.initial_reconnect_backoff_ms", 500),
    ("grpc.max_reconnect_backoff_ms", 5_000),
    ("grpc.enable_retries", 1),
    ("grpc.service_config", json.dumps(_SERVICE_CONFIG)),
)

_WHITESPACE = re.compile(r"\s+")


class SearchError(Exception):
    """A failed search, with the HTTP status and message the gateway answers with."""

    def __init__(self, http_status: int, detail: str, grpc_code: str) -> None:
        super().__init__(detail)
        self.http_status = http_status
        self.detail = detail
        self.grpc_code = grpc_code


@dataclass(frozen=True)
class SearchHealth:
    status: Literal["ok", "degraded"]
    detail: str


class SearchBackend(Protocol):
    """What the routes need from the search engine (tests provide fakes)."""

    def search(self, request: SearchRequest) -> SearchResponse: ...

    def health(self) -> SearchHealth: ...

    def close(self) -> None: ...


def _status(exc: grpc.RpcError) -> tuple[grpc.StatusCode, str]:
    if isinstance(exc, grpc.Call):
        return exc.code(), exc.details() or ""
    return grpc.StatusCode.UNKNOWN, ""


def _clean(details: str, limit: int = 300) -> str:
    text = _WHITESPACE.sub(" ", details).strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


def to_search_error(exc: grpc.RpcError, timeout_seconds: float) -> SearchError:
    code, details = _status(exc)
    if code is grpc.StatusCode.INVALID_ARGUMENT:
        # The engine's own validation message ("Query must not be empty.") is meant for users.
        return SearchError(400, _clean(details) or "Invalid search request.", code.name)
    if code is grpc.StatusCode.UNAVAILABLE:
        return SearchError(503, "The search engine is unavailable. Try again shortly.", code.name)
    if code is grpc.StatusCode.DEADLINE_EXCEEDED:
        return SearchError(
            504, f"The search engine did not answer within {timeout_seconds:g} s.", code.name
        )
    return SearchError(502, "The search engine failed to answer the request.", code.name)


class GrpcSearchClient:
    """`SearchBackend` over one channel: TLS when a CA file is given, else plaintext (private network)."""

    def __init__(
        self,
        target: str,
        *,
        timeout_seconds: float,
        health_timeout_seconds: float = HEALTH_TIMEOUT_SECONDS,
        tls_ca_file: str | None = None,
    ) -> None:
        self.target = target
        self.timeout_seconds = timeout_seconds
        self.health_timeout_seconds = health_timeout_seconds
        # Connects lazily: the API starts (and serves geo/admin) while the engine is down.
        if tls_ca_file:
            with open(tls_ca_file, "rb") as ca:
                credentials = grpc.ssl_channel_credentials(root_certificates=ca.read())
            self._channel = grpc.secure_channel(target, credentials, options=CHANNEL_OPTIONS)
        else:
            self._channel = grpc.insecure_channel(target, options=CHANNEL_OPTIONS)
        self._execute_search = self._channel.unary_unary(
            _EXECUTE_SEARCH,
            request_serializer=SearchRequest.SerializeToString,
            response_deserializer=SearchResponse.FromString,
        )
        self._health_check = self._channel.unary_unary(
            _HEALTH_CHECK,
            request_serializer=HealthCheckRequest.SerializeToString,
            response_deserializer=HealthCheckResponse.FromString,
        )

    def search(self, request: SearchRequest) -> SearchResponse:
        try:
            return self._execute_search(request, timeout=self.timeout_seconds)
        except grpc.RpcError as exc:
            raise to_search_error(exc, self.timeout_seconds) from exc

    def health(self) -> SearchHealth:
        try:
            response = self._health_check(
                HealthCheckRequest(service=SEARCH_SERVICE), timeout=self.health_timeout_seconds
            )
        except grpc.RpcError as exc:
            code, _ = _status(exc)
            if code is grpc.StatusCode.UNIMPLEMENTED:
                return SearchHealth("degraded", "the search engine has no grpc.health.v1 service")
            return SearchHealth("degraded", f"health check failed: {code.name}")
        if response.status == HealthCheckResponse.SERVING:
            return SearchHealth("ok", "SERVING")
        return SearchHealth("degraded", HealthCheckResponse.ServingStatus.Name(response.status))

    def close(self) -> None:
        self._channel.close()
