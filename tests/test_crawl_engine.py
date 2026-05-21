from __future__ import annotations

import threading
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from mcosint.crawl.engine import CrawlEngine, CrawlMetrics, build_mesh_from_db
from mcosint.crawl.namemc_friends import CrawlConfig
from mcosint.crawl.task_queue import InProcessTaskQueue


def _mock_pool(friends_crawled: bool = False, friends: list[str] | None = None):
    """Build a minimal mock ConnectionPool."""
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.transaction = MagicMock()
    conn.transaction().__enter__ = MagicMock(return_value=conn)
    conn.transaction().__exit__ = MagicMock(return_value=False)
    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool, conn


def _minimal_cfg(**overrides) -> CrawlConfig:
    defaults = dict(
        thread_count=1,
        request_delay_seconds=0.0,
        max_depth=0,
        max_total_requests=None,
        max_friends_per_user=10,
        rate_limit_backoff_seconds=1.0,
        max_retries_per_uuid=0,
        force_recrawl=False,
        flaresolverr_url=None,
        flaresolverr_max_timeout_ms=60_000,
        timeout_seconds=5.0,
        user_agent="test/0.1",
    )
    defaults.update(overrides)
    return CrawlConfig(**defaults)


class TestCrawlMetrics:
    def test_to_dict(self) -> None:
        m = CrawlMetrics(requests_used=5, db_writes=3, duration_seconds=1.234)
        d = m.to_dict()
        assert d["requests_used"] == 5
        assert d["db_writes"] == 3
        assert d["duration_seconds"] == 1.23

    def test_defaults_zero(self) -> None:
        m = CrawlMetrics()
        assert m.requests_used == 0
        assert m.errors == 0


class TestBuildMeshFromDb:
    def test_single_node_no_friends(self) -> None:
        conn = MagicMock()
        with patch("mcosint.crawl.engine.get_friend_uuids_from_db", return_value=[]):
            mesh = build_mesh_from_db(conn, "00000000-0000-0000-0000-000000000001",
                                      max_depth=1)
        assert len(mesh) == 1

    def test_respects_max_depth(self) -> None:
        uuid_a = "00000000-0000-0000-0000-000000000001"
        uuid_b = "00000000-0000-0000-0000-000000000002"
        uuid_c = "00000000-0000-0000-0000-000000000003"

        def _friends(conn, *, player_uuid, limit=None):
            if player_uuid == uuid_a:
                return [uuid_b]
            if player_uuid == uuid_b:
                return [uuid_c]
            return []

        conn = MagicMock()
        with patch("mcosint.crawl.engine.get_friend_uuids_from_db", side_effect=_friends):
            mesh = build_mesh_from_db(conn, uuid_a, max_depth=1)

        assert uuid_a in mesh
        assert uuid_b in mesh
        assert uuid_c not in mesh  # beyond max_depth=1


class TestCrawlEngineInit:
    def test_defaults_to_in_process_queue(self) -> None:
        pool, _ = _mock_pool()
        cfg = _minimal_cfg()
        engine = CrawlEngine(db_pool=pool, config=cfg)
        assert isinstance(engine._task_queue, InProcessTaskQueue)

    def test_accepts_custom_task_queue(self) -> None:
        pool, _ = _mock_pool()
        cfg = _minimal_cfg()
        custom_q = InProcessTaskQueue()
        engine = CrawlEngine(db_pool=pool, config=cfg, task_queue=custom_q)
        assert engine._task_queue is custom_q

    def test_accepts_metrics(self) -> None:
        from mcosint.metrics.recorder import InProcessMetricsRecorder
        pool, _ = _mock_pool()
        cfg = _minimal_cfg()
        m = InProcessMetricsRecorder()
        engine = CrawlEngine(db_pool=pool, config=cfg, metrics=m)
        assert engine._metrics is m


class TestCrawlEngineRun:
    def test_empty_seeds_raises(self) -> None:
        pool, _ = _mock_pool()
        cfg = _minimal_cfg()
        engine = CrawlEngine(db_pool=pool, config=cfg)
        with pytest.raises(ValueError, match="No valid seed"):
            engine.run([])

    def test_whitespace_only_seeds_raises(self) -> None:
        pool, _ = _mock_pool()
        cfg = _minimal_cfg()
        engine = CrawlEngine(db_pool=pool, config=cfg)
        with pytest.raises(ValueError):
            engine.run(["   ", "\t"])

    def test_run_returns_crawl_metrics(self) -> None:
        pool, conn = _mock_pool()

        with (
            patch("mcosint.crawl.engine.upsert_discovered_players"),
            patch("mcosint.crawl.engine.is_player_friends_crawled", return_value=True),
            patch("mcosint.crawl.engine.get_friend_uuids_from_db", return_value=[]),
        ):
            cfg = _minimal_cfg(max_depth=0, force_recrawl=False)
            engine = CrawlEngine(db_pool=pool, config=cfg)
            result = engine.run(["00000000-0000-0000-0000-000000000001"])

        assert isinstance(result, CrawlMetrics)
        assert result.duration_seconds >= 0
