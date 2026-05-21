"""Prometheus-backed MetricsRecorder.

Only imported when the `prometheus_client` package is installed (it's an
optional dependency: `pip install "mcosint[metrics]"`). The CLI's
`--metrics-port` flag wires this up; without that flag, the engine continues
to use the in-process recorder and no /metrics endpoint is exposed.

Metrics shape (matches docs/ARCHITECTURE.md §15):

  mcosint_requests_total{worker, proxy, outcome}     Counter
  mcosint_request_duration_seconds{worker, proxy}    Histogram
  mcosint_db_writes_total{worker}                    Counter
  mcosint_rate_limits_total{worker, proxy}           Counter
  mcosint_queue_depth{status}                        Gauge (set externally)
  mcosint_active_workers                             Gauge (set externally)
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

_NO_PROXY_LABEL = "direct"


class PrometheusUnavailableError(RuntimeError):
    """Raised when caller asks for Prometheus support but the dep isn't installed."""


def _import_prometheus():
    """Lazy import so missing prometheus_client doesn't break anything else."""
    try:
        import prometheus_client  # noqa: F401
        from prometheus_client import (
            CollectorRegistry,
            Counter,
            Gauge,
            Histogram,
            start_http_server,
        )

        return CollectorRegistry, Counter, Gauge, Histogram, start_http_server
    except ImportError as e:
        raise PrometheusUnavailableError(
            "prometheus_client is not installed. "
            "Install it via: pip install 'mcosint[metrics]'"
        ) from e


class PrometheusMetricsRecorder:
    """MetricsRecorder that exports counters/histograms via prometheus_client.

    Uses a private `CollectorRegistry` so multiple instances (e.g. in tests
    or in subsequent CLI invocations within the same process) don't collide
    on metric registration. The `/metrics` HTTP server, when started via
    `start_http_server()`, scrapes the *default* global registry — so call
    `start_metrics_http_server(port, recorder)` instead of starting the
    server directly. That helper exposes this recorder's registry.
    """

    def __init__(self) -> None:
        (
            self._CollectorRegistry,
            self._Counter,
            self._Gauge,
            self._Histogram,
            self._start_http_server,
        ) = _import_prometheus()

        self.registry = self._CollectorRegistry()

        self._requests = self._Counter(
            "mcosint_requests_total",
            "NameMC API requests dispatched, broken out by worker, proxy, and outcome.",
            labelnames=("worker", "proxy", "outcome"),
            registry=self.registry,
        )
        self._rate_limits = self._Counter(
            "mcosint_rate_limits_total",
            "Count of 429 / RateLimitedError events per worker + proxy.",
            labelnames=("worker", "proxy"),
            registry=self.registry,
        )
        self._db_writes = self._Counter(
            "mcosint_db_writes_total",
            "Successful persist_namemc_friend_response transactions per worker.",
            labelnames=("worker",),
            registry=self.registry,
        )
        self._duration = self._Histogram(
            "mcosint_request_duration_seconds",
            "Wall-clock duration of NameMC requests per worker + proxy.",
            labelnames=("worker", "proxy"),
            buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
            registry=self.registry,
        )

        # External gauges — set by the worker harness, not by the engine.
        self.queue_depth = self._Gauge(
            "mcosint_queue_depth",
            "crawl_queue rows grouped by status.",
            labelnames=("status",),
            registry=self.registry,
        )
        self.active_workers = self._Gauge(
            "mcosint_active_workers",
            "Live worker nodes per node_id (1 = active, 0 = inactive).",
            labelnames=("node_id",),
            registry=self.registry,
        )

    # ── MetricsRecorder protocol ──────────────────────────────────────────

    def inc_requests(self, worker_id: int, proxy: str | None, success: bool) -> None:
        self._requests.labels(
            worker=str(worker_id),
            proxy=proxy or _NO_PROXY_LABEL,
            outcome="ok" if success else "err",
        ).inc()

    def inc_rate_limits(self, worker_id: int, proxy: str | None) -> None:
        self._rate_limits.labels(
            worker=str(worker_id),
            proxy=proxy or _NO_PROXY_LABEL,
        ).inc()

    def record_request_duration_ms(
        self, worker_id: int, ms: float, proxy: str | None = None
    ) -> None:
        self._duration.labels(
            worker=str(worker_id),
            proxy=proxy or _NO_PROXY_LABEL,
        ).observe(ms / 1000.0)

    def inc_db_writes(self, worker_id: int, count: int) -> None:
        self._db_writes.labels(worker=str(worker_id)).inc(count)

    def snapshot(self) -> dict[str, Any]:
        # Prometheus' model is "scrape, don't snapshot" — values live in the
        # registry. We return an empty dict so the engine's `if snap:` check
        # in run() short-circuits and doesn't log a confusing one-line dump.
        return {}


def start_metrics_http_server(
    port: int,
    recorder: PrometheusMetricsRecorder,
    *,
    addr: str = "",
) -> None:
    """Start a daemon HTTP server exposing the recorder's metrics on /metrics.

    Uses prometheus_client.start_http_server(), which spawns a daemon thread
    that survives the duration of the process and dies cleanly on exit.
    Idempotency: prometheus_client raises if the port is already bound — we
    let that propagate so the operator notices.
    """
    log.info("Starting Prometheus metrics endpoint on :%d/metrics", port)
    recorder._start_http_server(port, addr=addr, registry=recorder.registry)


def try_make_prometheus_recorder() -> PrometheusMetricsRecorder | None:
    """Build a PrometheusMetricsRecorder, or return None if the dep is missing.

    Use this from CLI code that wants to fall back to InProcessMetricsRecorder
    when prometheus_client isn't installed, rather than crashing.
    """
    try:
        return PrometheusMetricsRecorder()
    except PrometheusUnavailableError as e:
        log.warning("Prometheus metrics disabled: %s", e)
        return None
