"""Structured (JSON lines) logging and per-request ids.

Every log line is one JSON object on stdout with `ts`, `level`, `logger`, `msg`, the
current request's `request_id` (when there is one) and any `extra=` fields.

`RequestContextMiddleware` gives every HTTP request an id -- the caller's `X-Request-ID`
when it is a sane token (nginx sends its `$request_id`), else a fresh one -- returns it in
the `X-Request-ID` response header, logs one access line per request (method, path without
the query string, status, duration) and turns an unhandled exception into a JSON 500.
Nothing secret is logged: no headers, no query strings, no bodies.
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
import uuid
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)

REQUEST_ID_HEADER = "X-Request-ID"
_VALID_REQUEST_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")

# Attributes every LogRecord has; anything else on a record came from `extra=`
# (uvicorn adds `color_message`, the message with terminal colour codes).
_STANDARD_ATTRS = frozenset(
    vars(logging.LogRecord("", logging.INFO, "", 0, "", None, None)).keys()
    | {"message", "asctime", "request_id", "taskName", "color_message"}
)

access_logger = logging.getLogger("api.access")
logger = logging.getLogger(__name__)


class RequestIdFilter(logging.Filter):
    """Stamps the current request id on every record that passes the handler."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        request_id = getattr(record, "request_id", None)
        if request_id:
            payload["request_id"] = request_id
        for key, value in vars(record).items():
            if key not in _STANDARD_ATTRS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(level: str = "INFO") -> None:
    """Route every logger (uvicorn's included) through one JSON handler on stdout."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(RequestIdFilter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    # uvicorn installs its own text handlers before importing the app; send its records
    # through ours instead.
    for name in ("uvicorn", "uvicorn.error"):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers.clear()
        uvicorn_logger.propagate = True
    # uvicorn's access log would repeat every request with its query string (search
    # text); RequestContextMiddleware logs requests instead, without it.
    uvicorn_access = logging.getLogger("uvicorn.access")
    uvicorn_access.handlers.clear()
    uvicorn_access.propagate = False


def _incoming_request_id(scope: Scope) -> str | None:
    for name, value in scope.get("headers", []):
        if name == b"x-request-id":
            candidate = value.decode("latin-1").strip()
            return candidate if _VALID_REQUEST_ID.match(candidate) else None
    return None


class RequestContextMiddleware:
    """Request id, access log and last-resort JSON 500 for every HTTP request."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = _incoming_request_id(scope) or uuid.uuid4().hex
        token = request_id_var.set(request_id)
        started = time.perf_counter()
        status = 500
        response_started = False
        # The response is complete once its last body chunk is sent; background work
        # that runs afterwards must not count towards the request's duration.
        finished: float | None = None

        async def send_with_id(message: Message) -> None:
            nonlocal status, response_started, finished
            if message["type"] == "http.response.start":
                response_started = True
                status = int(message["status"])
                MutableHeaders(scope=message).append(REQUEST_ID_HEADER, request_id)
            elif message["type"] == "http.response.body" and not message.get("more_body"):
                finished = time.perf_counter()
            await send(message)

        try:
            await self.app(scope, receive, send_with_id)
        except Exception:
            logger.exception("unhandled error")
            if response_started:
                raise  # too late for an error response; the server drops the connection
            status = 500
            await _send_internal_error(send, request_id)
            finished = time.perf_counter()
        finally:
            access_logger.info(
                "request",
                extra={
                    "method": scope.get("method"),
                    "path": scope.get("path"),
                    "status": status,
                    "duration_ms": round(((finished or time.perf_counter()) - started) * 1000, 1),
                },
            )
            request_id_var.reset(token)


async def _send_internal_error(send: Send, request_id: str) -> None:
    body = json.dumps({"detail": "Internal server error", "request_id": request_id}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 500,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
                (REQUEST_ID_HEADER.lower().encode(), request_id.encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})
