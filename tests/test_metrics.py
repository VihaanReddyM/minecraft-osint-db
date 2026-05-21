from __future__ import annotations

from mcosint.metrics.recorder import InProcessMetricsRecorder, NoopMetricsRecorder


class TestNoopMetricsRecorder:
    def test_all_methods_callable(self) -> None:
        m = NoopMetricsRecorder()
        m.inc_requests(0, None, True)
        m.inc_requests(1, "socks5://127.0.0.1:1080", False)
        m.inc_rate_limits(0, None)
        m.record_request_duration_ms(0, 123.4)
        m.inc_db_writes(0, 5)
        snap = m.snapshot()
        assert snap == {}


class TestInProcessMetricsRecorder:
    def test_snapshot_empty(self) -> None:
        m = InProcessMetricsRecorder()
        s = m.snapshot()
        assert s["requests_ok"] == 0
        assert s["requests_err"] == 0
        assert s["rate_limits"] == 0
        assert s["db_writes"] == 0
        assert s["avg_request_ms"] == 0.0

    def test_inc_requests_ok(self) -> None:
        m = InProcessMetricsRecorder()
        m.inc_requests(0, None, True)
        m.inc_requests(0, None, True)
        m.inc_requests(1, None, True)
        assert m.snapshot()["requests_ok"] == 3

    def test_inc_requests_err(self) -> None:
        m = InProcessMetricsRecorder()
        m.inc_requests(0, None, False)
        assert m.snapshot()["requests_err"] == 1
        assert m.snapshot()["requests_ok"] == 0

    def test_inc_rate_limits(self) -> None:
        m = InProcessMetricsRecorder()
        m.inc_rate_limits(0, "proxy-a")
        m.inc_rate_limits(1, "proxy-b")
        assert m.snapshot()["rate_limits"] == 2

    def test_avg_request_ms(self) -> None:
        m = InProcessMetricsRecorder()
        m.record_request_duration_ms(0, 100.0)
        m.record_request_duration_ms(0, 200.0)
        assert m.snapshot()["avg_request_ms"] == 150.0

    def test_db_writes(self) -> None:
        m = InProcessMetricsRecorder()
        m.inc_db_writes(0, 3)
        m.inc_db_writes(1, 2)
        assert m.snapshot()["db_writes"] == 5

    def test_per_worker(self) -> None:
        m = InProcessMetricsRecorder()
        m.inc_requests(0, None, True)
        m.inc_requests(1, None, False)
        m.inc_requests(1, None, True)
        snap = m.snapshot()
        assert snap["per_worker"][0]["ok"] == 1
        assert snap["per_worker"][0]["err"] == 0
        assert snap["per_worker"][1]["ok"] == 1
        assert snap["per_worker"][1]["err"] == 1

    def test_thread_safety(self) -> None:
        import threading
        m = InProcessMetricsRecorder()

        def _work():
            for _ in range(100):
                m.inc_requests(0, None, True)

        threads = [threading.Thread(target=_work) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert m.snapshot()["requests_ok"] == 500
