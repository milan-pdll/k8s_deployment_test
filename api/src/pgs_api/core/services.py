"""The per-process services the routes depend on, created at startup and closed at shutdown."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Request

from ..auth.tokens import TokenCodec
from ..db.database import Database, PgDatabase
from ..db.search_log import SearchLogSink, SearchLogWriter
from ..grpc.client import GrpcSearchClient, SearchBackend
from ..health.readiness import ReadinessProbe
from .settings import Settings

logger = logging.getLogger(__name__)


@dataclass
class Services:
    settings: Settings
    db: Database
    search: SearchBackend
    # None when API_AUTH_SECRET is unset: the auth and admin endpoints then answer 503.
    tokens: TokenCodec | None
    search_log: SearchLogSink
    readiness: ReadinessProbe

    def close(self) -> None:
        self.search_log.close()
        self.search.close()
        self.db.close()


def build_services(settings: Settings) -> Services:
    db = PgDatabase(settings.database_url.get_secret_value())
    search = GrpcSearchClient(
        settings.search_target, timeout_seconds=settings.search_timeout_seconds
    )
    tokens = (
        TokenCodec(settings.auth_secret.get_secret_value(), settings.token_ttl_seconds)
        if settings.auth_secret is not None
        else None
    )
    if tokens is None:
        logger.warning("API_AUTH_SECRET is not set: login and admin endpoints answer 503")
    return Services(
        settings=settings,
        db=db,
        search=search,
        tokens=tokens,
        search_log=SearchLogWriter(db),
        readiness=ReadinessProbe(db, search),
    )


def get_services(request: Request) -> Services:
    services = getattr(request.app.state, "services", None)
    if not isinstance(services, Services):
        raise RuntimeError("API services are not initialised (lifespan did not run)")
    return services


ServicesDep = Annotated[Services, Depends(get_services)]
