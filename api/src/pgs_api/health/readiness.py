"""The readiness report behind `GET /health/ready`.

- `database`: `pgs_db.health.check` (schema revision, extensions, reference data, Gold
  freshness). "failing" -- or an unreachable database -- makes the API unready (503).
- `search`: the engine's grpc.health.v1 status. Never fatal: without the engine the API
  still serves geo, auth and admin, so a down engine only makes the API "degraded".

Results are cached for a few seconds and computed by one caller at a time, so frequent
or concurrent probes cannot pile queries onto the database.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy.exc import SQLAlchemyError

from ..db.database import Database
from ..grpc.client import SearchBackend, SearchHealth

logger = logging.getLogger(__name__)

CACHE_SECONDS = 5.0

Status = Literal["ok", "degraded", "failing"]


@dataclass(frozen=True)
class Readiness:
    status: Status
    database: dict[str, Any]
    search: SearchHealth


class ReadinessProbe:
    def __init__(
        self,
        db: Database,
        search: SearchBackend,
        *,
        cache_seconds: float = CACHE_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._db = db
        self._search = search
        self._cache_seconds = cache_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._cached: tuple[float, Readiness] | None = None

    def check(self) -> Readiness:
        with self._lock:
            now = self._clock()
            if self._cached is not None and now - self._cached[0] < self._cache_seconds:
                return self._cached[1]
            result = self._run()
            self._cached = (self._clock(), result)
            return result

    def _run(self) -> Readiness:
        try:
            database = self._db.health()
        except SQLAlchemyError:
            logger.warning("readiness: database check failed", exc_info=True)
            database = {"status": "failing", "problems": ["database unreachable or check failed"]}
        search = self._search.health()
        db_status = database.get("status")
        status: Status
        if db_status == "failing":
            status = "failing"
        elif db_status != "ok" or search.status != "ok":
            status = "degraded"
        else:
            status = "ok"
        return Readiness(status=status, database=database, search=search)
