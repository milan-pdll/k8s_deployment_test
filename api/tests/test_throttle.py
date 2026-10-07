from pgs_api.auth.throttle import LoginThrottle


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_locks_after_max_failures_and_expires() -> None:
    clock = _Clock()
    throttle = LoginThrottle(max_failures=3, window_seconds=60, clock=clock)
    for _ in range(3):
        assert throttle.retry_after("Admin") == 0
        throttle.record_failure("Admin")
    assert throttle.retry_after("admin") > 0  # case-insensitive key
    assert throttle.retry_after("other") == 0
    clock.now = 61
    assert throttle.retry_after("admin") == 0


def test_success_clears_failures() -> None:
    throttle = LoginThrottle(max_failures=2, window_seconds=60, clock=_Clock())
    throttle.record_failure("admin")
    throttle.record_success("admin")
    throttle.record_failure("admin")
    assert throttle.retry_after("admin") == 0
