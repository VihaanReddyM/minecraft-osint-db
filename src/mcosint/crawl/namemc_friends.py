"""Crawl configuration + shared concurrency primitives used by `CrawlEngine`.

Historical note: this module used to contain a full `crawl_namemc_friends_to_db()`
loop that has since been replaced by `mcosint.crawl.engine.CrawlEngine`. Only the
shared building blocks remain.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class CrawlConfig:
    thread_count: int = 5
    request_delay_seconds: float = 1.0
    max_depth: int = 2
    max_total_requests: int | None = None  # None = unlimited
    max_friends_per_user: int = 50
    rate_limit_backoff_seconds: float = 10.0
    max_retries_per_uuid: int = 3
    force_recrawl: bool = False
    user_agent: str = "mcosint/0.1.0"
    flaresolverr_url: str | None = None
    flaresolverr_max_timeout_ms: int = 60_000
    timeout_seconds: float = 30.0


class GlobalCooldown:
    """Shared cross-thread cooldown used as a circuit breaker when no proxies are configured.

    When any worker hits 429 and all workers share the same outbound IP, this pauses all
    workers until the cooldown expires. With per-worker proxy isolation, use WorkerRateLimiter
    instead and leave GlobalCooldown dormant.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._until = 0.0  # monotonic seconds

    def wait_if_needed(self) -> None:
        while True:
            with self._lock:
                until = self._until
            now = time.monotonic()
            if now >= until:
                return
            time.sleep(min(until - now, 1.0))

    def trigger(self, seconds: float) -> None:
        if seconds <= 0:
            return
        with self._lock:
            self._until = max(self._until, time.monotonic() + seconds)


class RequestBudget:
    """Thread-safe request budget to cap total HTTP requests."""

    def __init__(self, max_total_requests: int | None) -> None:
        self._max = max_total_requests
        self._lock = threading.Lock()
        self._used = 0

    def consume(self) -> bool:
        if self._max is None:
            with self._lock:
                self._used += 1
            return True
        with self._lock:
            if self._used >= self._max:
                return False
            self._used += 1
            return True

    @property
    def used(self) -> int:
        with self._lock:
            return self._used
