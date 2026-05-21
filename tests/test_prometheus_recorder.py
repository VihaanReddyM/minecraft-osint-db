"""Phase 5b tests for PrometheusMetricsRecorder.

Skipped when the optional `prometheus_client` dep is not installed (i.e. when
the package was installed without the `[metrics]` extra).
"""

from __future__ import annotations

import pytest

prometheus_client = pytest.importorskip("prometheus_client")

from mcosint.metrics.prometheus import (  # noqa: E402
    PrometheusMetricsRecorder,
)


def _value(metric, **labels) -> float:
    """Sample a single labelled child of a Counter/Gauge."""
    return metric.labels(**labels)._value.get()


def _hist_count(metric, **labels) -> float:
    """Number of observations on a histogram child."""
    return metric.labels(**labels)._sum.get()


class TestPrometheusMetricsRecorder:
    def test_protocol_methods_callable(self) -> None:
        r = PrometheusMetricsRecorder()
        r.inc_requests(0, "socks5://a:1", True)
        r.inc_requests(1, None, False)
        r.inc_rate_limits(0, "socks5://a:1")
        r.record_request_duration_ms(0, 123.0, "socks5://a:1")
        r.inc_db_writes(0, 3)
        assert r.snapshot() == {}

    def test_requests_counter_labels(self) -> None:
        r = PrometheusMetricsRecorder()
        r.inc_requests(0, "socks5://a:1", True)
        r.inc_requests(0, "socks5://a:1", True)
        r.inc_requests(0, "socks5://a:1", False)
        assert _value(r._requests, worker="0", proxy="socks5://a:1", outcome="ok") == 2.0
        assert _value(r._requests, worker="0", proxy="socks5://a:1", outcome="err") == 1.0

    def test_no_proxy_uses_direct_label(self) -> None:
        r = PrometheusMetricsRecorder()
        r.inc_requests(0, None, True)
        assert _value(r._requests, worker="0", proxy="direct", outcome="ok") == 1.0

    def test_rate_limits_counter(self) -> None:
        r = PrometheusMetricsRecorder()
        r.inc_rate_limits(0, "socks5://a:1")
        r.inc_rate_limits(0, "socks5://a:1")
        assert _value(r._rate_limits, worker="0", proxy="socks5://a:1") == 2.0

    def test_db_writes_counter(self) -> None:
        r = PrometheusMetricsRecorder()
        r.inc_db_writes(0, 5)
        assert _value(r._db_writes, worker="0") == 5.0

    def test_request_duration_histogram_observes(self) -> None:
        r = PrometheusMetricsRecorder()
        # ms → seconds: 100ms becomes 0.1s in the histogram.
        r.record_request_duration_ms(0, 100.0, "socks5://a:1")
        r.record_request_duration_ms(0, 200.0, "socks5://a:1")
        # Sum should be 0.3 seconds total.
        total = _hist_count(r._duration, worker="0", proxy="socks5://a:1")
        assert abs(total - 0.3) < 1e-9

    def test_external_gauges_set(self) -> None:
        r = PrometheusMetricsRecorder()
        r.queue_depth.labels(status="pending").set(42)
        r.queue_depth.labels(status="in_progress").set(3)
        assert r.queue_depth.labels(status="pending")._value.get() == 42.0
        assert r.queue_depth.labels(status="in_progress")._value.get() == 3.0

    def test_registry_is_isolated_per_instance(self) -> None:
        # Two recorders should not collide on metric registration.
        a = PrometheusMetricsRecorder()
        b = PrometheusMetricsRecorder()
        assert a.registry is not b.registry
        a.inc_db_writes(0, 1)
        # b's counter is untouched.
        assert _value(b._db_writes, worker="0") == 0.0


class TestTryMakeRecorder:
    def test_factory_returns_instance(self) -> None:
        from mcosint.metrics.prometheus import try_make_prometheus_recorder

        rec = try_make_prometheus_recorder()
        assert rec is not None
        assert isinstance(rec, PrometheusMetricsRecorder)
