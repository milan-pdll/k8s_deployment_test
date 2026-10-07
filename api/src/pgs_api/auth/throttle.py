"""Per-account brake on failed logins (in memory, per API process).

After `max_failures` wrong passwords for one login within `window_seconds`, that login is
refused until the oldest failure ages out, whatever password comes next. The nginx limit
caps one client's rate; this caps what a spread-out attacker can try against one account.
It is per process, so with N API workers an attacker gets up to N times the allowance.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable

# Keys kept at most; the oldest are dropped first so a flood of random logins cannot grow
# the table without bound.
MAX_TRACKED_LOGINS = 10_000


class LoginThrottle:
    def __init__(
        self,
        max_failures: int = 5,
        window_seconds: float = 900.0,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._max = max_failures
        self._window = window_seconds
        self._clock = clock
        self._failures: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    def _recent(self, key: str, now: float) -> deque[float]:
        failures = self._failures.get(key)
        if failures is None:
            return deque()
        while failures and now - failures[0] >= self._window:
            failures.popleft()
        if not failures:
            del self._failures[key]
        return failures

    def retry_after(self, login: str) -> int:
        """Seconds until `login` may try again; 0 when it may try now."""
        key = login.casefold()
        with self._lock:
            now = self._clock()
            failures = self._recent(key, now)
            if len(failures) < self._max:
                return 0
            return max(1, int(self._window - (now - failures[0])) + 1)

    def record_failure(self, login: str) -> None:
        key = login.casefold()
        with self._lock:
            now = self._clock()
            failures = self._recent(key, now)
            failures.append(now)
            self._failures[key] = failures
            while len(self._failures) > MAX_TRACKED_LOGINS:
                del self._failures[next(iter(self._failures))]

    def record_success(self, login: str) -> None:
        with self._lock:
            self._failures.pop(login.casefold(), None)
