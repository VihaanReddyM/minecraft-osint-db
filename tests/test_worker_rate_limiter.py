from __future__ import annotations

import time

from mcosint.crawl.rate_limiter import WorkerRateLimiter


class TestWorkerRateLimiter:
    def test_fresh_limiter_does_not_block(self) -> None:
        limiter = WorkerRateLimiter()
        t0 = time.monotonic()
        limiter.wait_if_needed()
        elapsed = time.monotonic() - t0
        assert elapsed < 0.05, f"Expected no wait, got {elapsed:.3f}s"

    def test_trigger_zero_does_not_block(self) -> None:
        limiter = WorkerRateLimiter()
        limiter.trigger(0)
        t0 = time.monotonic()
        limiter.wait_if_needed()
        assert time.monotonic() - t0 < 0.05

    def test_trigger_negative_does_not_block(self) -> None:
        limiter = WorkerRateLimiter()
        limiter.trigger(-5)
        t0 = time.monotonic()
        limiter.wait_if_needed()
        assert time.monotonic() - t0 < 0.05

    def test_trigger_causes_wait(self) -> None:
        limiter = WorkerRateLimiter()
        limiter.trigger(0.15)
        t0 = time.monotonic()
        limiter.wait_if_needed()
        elapsed = time.monotonic() - t0
        assert elapsed >= 0.10, f"Expected ~0.15s wait, got {elapsed:.3f}s"
        assert elapsed < 0.5, f"Wait too long: {elapsed:.3f}s"

    def test_active_property(self) -> None:
        limiter = WorkerRateLimiter()
        assert not limiter.active
        limiter.trigger(5.0)
        assert limiter.active

    def test_active_false_after_expiry(self) -> None:
        limiter = WorkerRateLimiter()
        limiter.trigger(0.05)
        time.sleep(0.1)
        assert not limiter.active

    def test_trigger_extends_deadline(self) -> None:
        limiter = WorkerRateLimiter()
        limiter.trigger(0.1)
        limiter.trigger(0.5)  # longer — should extend
        t0 = time.monotonic()
        limiter.wait_if_needed()
        elapsed = time.monotonic() - t0
        assert elapsed >= 0.4, f"Expected ~0.5s wait from extended trigger, got {elapsed:.3f}s"

    def test_trigger_does_not_shorten_deadline(self) -> None:
        limiter = WorkerRateLimiter()
        limiter.trigger(0.4)
        limiter.trigger(0.1)  # shorter — should NOT override the longer deadline
        t0 = time.monotonic()
        limiter.wait_if_needed()
        elapsed = time.monotonic() - t0
        assert elapsed >= 0.3, f"Short trigger should not shorten the deadline, got {elapsed:.3f}s"

    def test_wait_is_reentrant_after_expiry(self) -> None:
        limiter = WorkerRateLimiter()
        limiter.trigger(0.05)
        limiter.wait_if_needed()  # waits ~50ms

        # After expiry, waiting again should be instant.
        t0 = time.monotonic()
        limiter.wait_if_needed()
        assert time.monotonic() - t0 < 0.05
