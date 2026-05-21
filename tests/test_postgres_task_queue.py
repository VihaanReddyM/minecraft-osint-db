"""Mock-based tests for PostgresTaskQueue.

These do not exercise a real Postgres — they verify the SQL emitted by each
method against a mocked connection. Real integration tests against a live
Postgres are on the Phase 5b roadmap (see docs/ARCHITECTURE.md §16).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from mcosint.crawl.task_queue import PostgresTaskQueue


def _mock_pool() -> tuple[MagicMock, MagicMock]:
    """Build a minimal mock ConnectionPool that returns a mock connection."""
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    pool = MagicMock()
    pool.connection.return_value = conn
    return pool, conn


class TestEnqueueLeastDepth:
    """Phase 5a bug #5: enqueue mirrors InProcessTaskQueue's shallower-depth-wins."""

    def test_enqueue_uses_least_depth_and_resets_done(self) -> None:
        pool, conn = _mock_pool()
        q = PostgresTaskQueue(pool)
        q.enqueue("uuid-a", 2)

        # Verify SQL was issued with the expected structure
        assert conn.execute.called
        sql, params = conn.execute.call_args.args
        assert "ON CONFLICT" in sql
        assert "LEAST(crawl_queue.depth, EXCLUDED.depth)" in sql
        assert "'done'" in sql  # The status-reset case
        assert "pending" in sql
        assert params == ("uuid-a", 2)


class TestCompleteWithWorkerId:
    """Phase 5a bug #4: complete() filters by worker_id and status='in_progress'."""

    def test_complete_includes_worker_id_filter(self) -> None:
        pool, conn = _mock_pool()
        q = PostgresTaskQueue(pool)
        q.complete("uuid-a")

        assert conn.execute.called
        sql, params = conn.execute.call_args.args
        assert "worker_id = %s" in sql
        assert "status = 'in_progress'" in sql
        # The second param should be the node_id (e.g. "host-12345")
        assert params[0] == "uuid-a"
        assert isinstance(params[1], str) and params[1]  # non-empty node_id

    def test_node_id_format(self) -> None:
        pool, _ = _mock_pool()
        q = PostgresTaskQueue(pool)
        # The node_id is "{hostname}-{pid}"; just sanity-check it contains "-"
        assert "-" in q._node_id


class TestTryClaim:
    def test_try_claim_uses_skip_locked(self) -> None:
        pool, conn = _mock_pool()
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        conn.execute.return_value = cursor

        q = PostgresTaskQueue(pool)
        result = q._try_claim()

        sql, params = conn.execute.call_args.args
        assert "FOR UPDATE SKIP LOCKED" in sql
        assert "in_progress" in sql
        assert result is None
