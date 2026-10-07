"""Error responses.

Request validation failures are 400 (not FastAPI's default 422) with
`{"detail": [{"loc": [...], "msg": "...", "type": "..."}]}`. The offending input is
never echoed back: it may be a password.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


def _public_errors(errors: Sequence[Any]) -> list[dict[str, Any]]:
    public: list[dict[str, Any]] = []
    for error in errors:
        if not isinstance(error, dict):
            continue
        entry = cast("dict[str, Any]", error)
        public.append(
            {
                "loc": list(entry.get("loc", ())),
                "msg": str(entry.get("msg", "")),
                "type": str(entry.get("type", "")),
            }
        )
    return public


async def validation_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    errors: Sequence[Any] = exc.errors() if isinstance(exc, RequestValidationError) else ()
    return JSONResponse(status_code=400, content={"detail": _public_errors(errors)})


def invalid_parameter(location: str, name: str, message: str) -> RequestValidationError:
    """A 400 for a parameter that passed type validation but is wrong in context."""
    return RequestValidationError(
        [{"loc": (location, name), "msg": message, "type": "value_error"}]
    )
