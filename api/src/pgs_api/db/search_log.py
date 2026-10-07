"""Best-effort search logging (`SearchLogRepository.log_query`) off the request path.

A search answers as soon as the engine does; its `search_queries` row is written later
by a small thread pool. When the database is slow the backlog is capped and new entries
are dropped (with a warning) instead of piling up; a failed write is logged and
forgotten. Neither ever reaches the user.
"""

from __future__ import annotations

import contextvars
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Protocol

from .database import Database, SearchLogEntry

logger = logging.getLogger(__name__)


class SearchLogSink(Protocol):
    def submit(self, entry: SearchLogEntry) -> bool: ...

    def close(self) -> None: ...


class SearchLogWriter:
    """Writes entries on `workers` background threads, at most `max_pending` queued."""

    def __init__(self, db: Database, *, workers: int = 2, max_pending: int = 100) -> None:
        self._db = db
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="search-log")
        self._slots = threading.BoundedSemaphore(max_pending)
        self._drain_until: float | None = None

    def submit(self, entry: SearchLogEntry) -> bool:
        """Queue an entry. False when it was dropped (backlog full or shutting down)."""
        if not self._slots.acquire(blocking=False):
            logger.warning("search log backlog full; entry dropped")
            return False
        # Copy the context so the write's log lines carry the request id.
        context = contextvars.copy_context()
        try:
            self._executor.submit(context.run, self._write, entry)
        except RuntimeError:  # the executor is shut down
            self._slots.release()
            return False
        return True

    def _write(self, entry: SearchLogEntry) -> None:
        try:
            if self._drain_until is not None and time.monotonic() > self._drain_until:
                return  # shutting down and out of time: drop what is left
            self._db.log_search(entry)
        except Exception:
            # Best effort by design: the search was already answered.
            logger.warning("search log write failed", exc_info=True)
        finally:
            self._slots.release()

    def close(self, timeout: float = 5.0) -> None:
        """Write what is queued for up to `timeout` seconds, then drop the rest."""
        self._drain_until = time.monotonic() + timeout
        self._executor.shutdown(wait=True)
