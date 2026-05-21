from __future__ import annotations

import threading
from collections import defaultdict
from typing import Any, Protocol


class MetricsRecorder(Protocol):
    """Interface for recording crawl-time metrics.

    Pass a NoopMetricsRecorder when metrics aren't needed; pass
    InProcessMetricsRecorder to collect per-worker + per-proxy stats; pass
    PrometheusMetricsRecorder (when `prometheus_client` is installed) to expose
    a /metrics endpoint.
    """

    def inc_requests(self, worker_id: int, proxy: str | None, success: bool) -> None: ...
    def inc_rate_limits(self, worker_id: int, proxy: str | None) -> None: ...
    def record_request_duration_ms(
        self, worker_id: int, ms: float, proxy: str | None = None
    ) -> None: ...
    def inc_db_writes(self, worker_id: int, count: int) -> None: ...
    def snapshot(self) -> dict[str, Any]: ...


class NoopMetricsRecorder:
    """Drop-in MetricsRecorder that does nothing — zero overhead."""

    def inc_requests(self, worker_id: int, proxy: str | None, success: bool) -> None:
        pass

    def inc_rate_limits(self, worker_id: int, proxy: str | None) -> None:
        pass

    def record_request_duration_ms(
        self, worker_id: int, ms: float, proxy: str | None = None
    ) -> None:
        pass

    def inc_db_writes(self, worker_id: int, count: int) -> None:
        pass

    def snapshot(self) -> dict[str, Any]:
        return {}


# Sentinel used as the proxy key when the request went out the host IP
# (no proxy assigned to that worker). Keeps the per_proxy dict from
# collapsing all unproxied workers into a `None` key that's awkward to
# serialise.
_NO_PROXY_KEY = "direct"


class InProcessMetricsRecorder:
    """Thread-safe in-process metrics collector.

    Tracks counters per-worker AND per-proxy so an operator can tell which
    outbound IP is taking the rate-limit hits. `snapshot()` returns a dict
    suitable for logging or Discord embeds at crawl end.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()

        # Per-worker counters
        self._requests_ok: dict[int, int] = defaultdict(int)
        self._requests_err: dict[int, int] = defaultdict(int)
        self._rate_limits: dict[int, int] = defaultdict(int)
        self._db_writes: dict[int, int] = defaultdict(int)

        # Per-proxy counters (key = proxy URL or _NO_PROXY_KEY)
        self._proxy_requests_ok: dict[str, int] = defaultdict(int)
        self._proxy_requests_err: dict[str, int] = defaultdict(int)
        self._proxy_rate_limits: dict[str, int] = defaultdict(int)
        self._proxy_durations: dict[str, list[float]] = defaultdict(list)

        # Global duration list (kept for overall avg; per-proxy goes above)
        self._duration_ms: list[float] = []

    @staticmethod
    def _proxy_key(proxy: str | None) -> str:
        return proxy if proxy else _NO_PROXY_KEY

    def inc_requests(self, worker_id: int, proxy: str | None, success: bool) -> None:
        key = self._proxy_key(proxy)
        with self._lock:
            if success:
                self._requests_ok[worker_id] += 1
                self._proxy_requests_ok[key] += 1
            else:
                self._requests_err[worker_id] += 1
                self._proxy_requests_err[key] += 1

    def inc_rate_limits(self, worker_id: int, proxy: str | None) -> None:
        key = self._proxy_key(proxy)
        with self._lock:
            self._rate_limits[worker_id] += 1
            self._proxy_rate_limits[key] += 1

    def record_request_duration_ms(
        self, worker_id: int, ms: float, proxy: str | None = None
    ) -> None:
        key = self._proxy_key(proxy)
        with self._lock:
            self._duration_ms.append(ms)
            self._proxy_durations[key].append(ms)

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

            proxies = sorted(
                set(
                    list(self._proxy_requests_ok)
                    + list(self._proxy_requests_err)
                    + list(self._proxy_rate_limits)
                    + list(self._proxy_durations)
                )
            )
            per_proxy: dict[str, dict[str, Any]] = {}
            for p in proxies:
                durs = self._proxy_durations.get(p, [])
                per_proxy[p] = {
                    "ok": self._proxy_requests_ok[p],
                    "err": self._proxy_requests_err[p],
                    "rl": self._proxy_rate_limits[p],
                    "avg_ms": round(sum(durs) / len(durs), 1) if durs else 0.0,
                    "n": len(durs),
                }

        avg_ms = round(sum(durations) / len(durations), 1) if durations else 0.0
        return {
            "requests_ok": total_ok,
            "requests_err": total_err,
            "rate_limits": total_rl,
            "db_writes": total_db,
            "avg_request_ms": avg_ms,
            "per_worker": per_worker,
            "per_proxy": per_proxy,
        }
