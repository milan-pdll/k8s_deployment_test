"""Application factory: routes, middleware, error handlers and the startup/shutdown of services."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware

from .core.errors import validation_error_handler
from .core.observability import REQUEST_ID_HEADER, RequestContextMiddleware
from .core.services import Services, build_services
from .core.settings import Settings
from .routers import admin, auth, geo, health, search

logger = logging.getLogger(__name__)

API_VERSION = "1.0.0"


def create_app(
    settings: Settings,
    *,
    services_factory: Callable[[Settings], Services] = build_services,
) -> FastAPI:
    """The FastAPI app. Services (DB engine, gRPC channel, ...) live from startup to shutdown."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        services = services_factory(settings)
        app.state.services = services
        logger.info(
            "api started",
            extra={
                "search_target": settings.search_target,
                "search_timeout_seconds": settings.search_timeout_seconds,
                "auth_enabled": settings.auth_enabled,
                "cors_origins": list(settings.cors_origins),
            },
        )
        try:
            yield
        finally:
            services.close()
            logger.info("api stopped")

    app = FastAPI(
        title="PGS Search Engine API",
        version=API_VERSION,
        summary="Search, gazetteer and admin API of the PGS Search Engine",
        lifespan=lifespan,
    )
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(settings.cors_origins),
            allow_methods=["GET", "POST"],
            allow_headers=["Authorization", "Content-Type", REQUEST_ID_HEADER],
            expose_headers=[REQUEST_ID_HEADER],
            max_age=600,
        )
    # Added last, so outermost: every response (CORS preflights and errors too) gets an id.
    app.add_middleware(RequestContextMiddleware)
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    for module in (health, search, geo, auth, admin):
        app.include_router(module.router)
    return app
