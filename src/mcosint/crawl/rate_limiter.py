from __future__ import annotations

import time


class WorkerRateLimiter:
    """Per-worker rate limiter.

    Triggering this limiter only pauses the owning worker — all other workers continue
    unaffected. Use alongside GlobalCooldown (a shared circuit breaker) when running
    without per-worker proxy isolation.

    Not thread-safe by design: each instance is owned by exactly one worker thread.
    """

    def __init__(self) -> None:
        self._until = 0.0  # monotonic seconds

    def wait_if_needed(self) -> None:
        while True:
            now = time.monotonic()
            if now >= self._until:
                return
            time.sleep(min(self._until - now, 1.0))

    def trigger(self, seconds: float) -> None:
        if seconds <= 0:
            return
        self._until = max(self._until, time.monotonic() + seconds)

    @property
    def active(self) -> bool:
        return time.monotonic() < self._until
