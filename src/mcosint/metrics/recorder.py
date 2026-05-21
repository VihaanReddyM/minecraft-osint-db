from __future__ import annotations

import threading
from collections import defaultdict
from typing import Any, Protocol


class MetricsRecorder(Protocol):
    """Interface for recording crawl-time metrics.

    Pass a NoopMetricsRecorder when metrics aren't needed;
    pass InProcessMetricsRecorder to collect per-worker stats.
    """

    def inc_requests(self, worker_id: int, proxy: str | None, success: bool) -> None: ...
    def inc_rate_limits(self, worker_id: int, proxy: str | None) -> None: ...
    def record_request_duration_ms(self, worker_id: int, ms: float) -> None: ...
    def inc_db_writes(self, worker_id: int, count: int) -> None: ...
    def snapshot(self) -> dict[str, Any]: ...


class NoopMetricsRecorder:
    """Drop-in MetricsRecorder that does nothing — zero overhead."""

    def inc_requests(self, worker_id: int, proxy: str | None, success: bool) -> None:
        pass

    def inc_rate_limits(self, worker_id: int, proxy: str | None) -> None:
        pass

    def record_request_duration_ms(self, worker_id: int, ms: float) -> None:
        pass

    def inc_db_writes(self, worker_id: int, count: int) -> None:
        pass

    def snapshot(self) -> dict[str, Any]:
        return {}


class InProcessMetricsRecorder:
    """Thread-safe in-process metrics collector.

    `snapshot()` returns a dict suitable for logging or Discord embeds at crawl end.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests_ok: dict[int, int] = defaultdict(int)
        self._requests_err: dict[int, int] = defaultdict(int)
        self._rate_limits: dict[int, int] = defaultdict(int)
        self._db_writes: dict[int, int] = defaultdict(int)
        self._duration_ms: list[float] = []

    def inc_requests(self, worker_id: int, proxy: str | None, success: bool) -> None:
        with self._lock:
            if success:
                self._requests_ok[worker_id] += 1
            else:
                self._requests_err[worker_id] += 1

    def inc_rate_limits(self, worker_id: int, proxy: str | None) -> None:
        with self._lock:
            self._rate_limits[worker_id] += 1

    def record_request_duration_ms(self, worker_id: int, ms: float) -> None:
        with self._lock:
            self._duration_ms.append(ms)

    def inc_db_writes(self, worker_id: int, count: int) -> None:
        with self._lock:
            self._db_writes[worker_id] += count

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            total_ok = sum(self._requests_ok.values())
            total_err = sum(self._requests_err.values())
            total_rl = sum(self._rate_limits.values())
            total_db = sum(self._db_writes.values())
            durations = list(self._duration_ms)
            workers = sorted(set(list(self._requests_ok) + list(self._requests_err)))
            per_worker = {
                w: {
                    "ok": self._requests_ok[w],
                    "err": self._requests_err[w],
                    "rl": self._rate_limits[w],
                }
                for w in workers
            }
        avg_ms = round(sum(durations) / len(durations), 1) if durations else 0.0
        return {
            "requests_ok": total_ok,
            "requests_err": total_err,
            "rate_limits": total_rl,
            "db_writes": total_db,
            "avg_request_ms": avg_ms,
            "per_worker": per_worker,
        }
