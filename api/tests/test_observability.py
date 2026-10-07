from __future__ import annotations

import json
import logging
from contextlib import ExitStack

from fastapi import APIRouter

from pgs_api.app import create_app
from pgs_api.core.observability import (
    JsonFormatter,
    RequestIdFilter,
    configure_logging,
    request_id_var,
)

from .conftest import Harness, make_settings, open_client


def test_request_id_is_echoed(harness: Harness) -> None:
    response = harness.client.get("/health/live", headers={"X-Request-ID": "abc-123"})

    assert response.headers["x-request-id"] == "abc-123"


def test_request_id_is_generated_when_missing_or_unsafe(harness: Harness) -> None:
    missing = harness.client.get("/health/live")
    unsafe = harness.client.get("/health/live", headers={"X-Request-ID": "x" * 200})

    assert len(missing.headers["x-request-id"]) == 32
    assert len(unsafe.headers["x-request-id"]) == 32


def test_unhandled_error_is_a_json_500_with_the_request_id(harness: Harness) -> None:
    app = create_app(make_settings(), services_factory=lambda _settings: harness.services)
    router = APIRouter()

    @router.get("/boom")
    def boom() -> None:  # pyright: ignore[reportUnusedFunction]
        raise RuntimeError("secret internal detail")

    app.include_router(router)
    with ExitStack() as stack:
        client = open_client(stack, app, raise_server_exceptions=False)
        response = client.get("/boom", headers={"X-Request-ID": "req-1"})

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error", "request_id": "req-1"}
    assert response.headers["x-request-id"] == "req-1"
    assert "secret internal detail" not in response.text


def test_json_log_lines_carry_request_id_and_extras() -> None:
    record = logging.LogRecord("api.test", logging.INFO, __file__, 1, "hello %s", ("world",), None)
    record.status = 200
    token = request_id_var.set("req-9")
    try:
        RequestIdFilter().filter(record)
    finally:
        request_id_var.reset(token)

    line = json.loads(JsonFormatter().format(record))

    assert line["msg"] == "hello world"
    assert line["level"] == "INFO"
    assert line["request_id"] == "req-9"
    assert line["status"] == 200


def test_configure_logging_keeps_uvicorns_access_log_off() -> None:
    root = logging.getLogger()
    saved = (root.handlers[:], root.level)
    try:
        configure_logging("INFO")
        assert not logging.getLogger("uvicorn.access").hasHandlers()
        assert logging.getLogger("uvicorn.error").hasHandlers()  # via the root handler
    finally:
        root.handlers[:], level = saved
        root.setLevel(level)
