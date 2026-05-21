"""Phase 5b per-proxy aggregation tests for InProcessMetricsRecorder."""

from __future__ import annotations

from mcosint.metrics.recorder import InProcessMetricsRecorder


class TestPerProxyBreakdown:
    def test_no_proxy_uses_direct_key(self) -> None:
        m = InProcessMetricsRecorder()
        m.inc_requests(0, None, True)
        per_proxy = m.snapshot()["per_proxy"]
        assert "direct" in per_proxy
        assert per_proxy["direct"]["ok"] == 1

    def test_per_proxy_request_counts(self) -> None:
        m = InProcessMetricsRecorder()
        m.inc_requests(0, "socks5://a:1", True)
        m.inc_requests(1, "socks5://a:1", True)
        m.inc_requests(2, "socks5://b:1", False)
        per_proxy = m.snapshot()["per_proxy"]
        assert per_proxy["socks5://a:1"]["ok"] == 2
        assert per_proxy["socks5://a:1"]["err"] == 0
        assert per_proxy["socks5://b:1"]["ok"] == 0
        assert per_proxy["socks5://b:1"]["err"] == 1

    def test_per_proxy_rate_limits(self) -> None:
        m = InProcessMetricsRecorder()
        m.inc_rate_limits(0, "socks5://a:1")
        m.inc_rate_limits(1, "socks5://a:1")
        m.inc_rate_limits(2, "socks5://b:1")
        per_proxy = m.snapshot()["per_proxy"]
        assert per_proxy["socks5://a:1"]["rl"] == 2
        assert per_proxy["socks5://b:1"]["rl"] == 1

    def test_per_proxy_durations(self) -> None:
        m = InProcessMetricsRecorder()
        m.record_request_duration_ms(0, 100.0, "socks5://a:1")
        m.record_request_duration_ms(1, 200.0, "socks5://a:1")
        m.record_request_duration_ms(2, 50.0, "socks5://b:1")
        per_proxy = m.snapshot()["per_proxy"]
        assert per_proxy["socks5://a:1"]["avg_ms"] == 150.0
        assert per_proxy["socks5://a:1"]["n"] == 2
        assert per_proxy["socks5://b:1"]["avg_ms"] == 50.0

    def test_mixed_direct_and_proxy(self) -> None:
        m = InProcessMetricsRecorder()
        m.inc_requests(0, None, True)
        m.inc_requests(1, "socks5://a:1", True)
        per_proxy = m.snapshot()["per_proxy"]
        assert per_proxy["direct"]["ok"] == 1
        assert per_proxy["socks5://a:1"]["ok"] == 1

    def test_per_worker_still_works(self) -> None:
        m = InProcessMetricsRecorder()
        m.inc_requests(0, "socks5://a:1", True)
        m.inc_requests(1, "socks5://b:1", False)
        per_worker = m.snapshot()["per_worker"]
        assert per_worker[0]["ok"] == 1
        assert per_worker[1]["err"] == 1
