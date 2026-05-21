from __future__ import annotations

import logging
import os
import queue
import threading
import time
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from psycopg_pool import ConnectionPool

log = logging.getLogger(__name__)


class TaskQueue(Protocol):
    """Abstract task queue for the BFS crawl frontier.

    InProcessTaskQueue wraps queue.Queue (single-process, in-memory).
    PostgresTaskQueue uses SELECT FOR UPDATE SKIP LOCKED (crash-safe, multi-process).
    """

    def enqueue(self, uuid: str, depth: int) -> None: ...
    def dequeue(self, timeout: float) -> tuple[str, int] | None: ...
    def complete(self, uuid: str) -> None: ...
    def qsize(self) -> int: ...
    def join(self) -> None: ...


class InProcessTaskQueue:
    """Thread-safe in-process BFS queue with built-in deduplication.

    Deduplication: a UUID is only enqueued if it hasn't been seen before
    (or if the new depth is shallower than the previously recorded depth).
    Uses queue.Queue.join() semantics — join() blocks until all complete() calls
    have matched the number of dequeued items.
    """

    def __init__(self) -> None:
        self._q: queue.Queue[tuple[str, int]] = queue.Queue()
        self._seen: dict[str, int] = {}
        self._lock = threading.Lock()

    def enqueue(self, uuid: str, depth: int) -> None:
        with self._lock:
            prev = self._seen.get(uuid)
            if prev is not None and depth >= prev:
                return
            self._seen[uuid] = depth
        self._q.put((uuid, depth))

    def dequeue(self, timeout: float = 0.5) -> tuple[str, int] | None:
        try:
            return self._q.get(timeout=timeout)
        except queue.Empty:
            return None

    def complete(self, uuid: str) -> None:
        self._q.task_done()

    def qsize(self) -> int:
        return self._q.qsize()

    def join(self) -> None:
        self._q.join()

    @property
    def seen_count(self) -> int:
        with self._lock:
            return len(self._seen)


class PostgresTaskQueue:
    """Crash-safe BFS queue backed by a PostgreSQL table.

    Uses SELECT FOR UPDATE SKIP LOCKED for multi-worker / multi-process safety.
    Tasks claimed but not completed within claim_timeout are automatically
    re-queued by gc_stale_tasks().

    Requires the crawl_queue table to exist — run `mcosint db init` first.
    """

    def __init__(
        self,
        pool: ConnectionPool,
        claim_timeout: float = 300.0,
    ) -> None:
        self._pool = pool
        self._claim_timeout = claim_timeout
        self._node_id = f"{os.uname().nodename}-{os.getpid()}"
        self._stop_gc = threading.Event()
        self._gc_thread: threading.Thread | None = None

    # ── Core queue operations ─────────────────────────────────────────────────

    def enqueue(self, uuid: str, depth: int) -> None:
        with self._pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO crawl_queue (uuid, depth)
                VALUES (%s, %s)
                ON CONFLICT (uuid) DO NOTHING
                """,
                (uuid, depth),
            )

    def dequeue(self, timeout: float = 0.5) -> tuple[str, int] | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            row = self._try_claim()
            if row:
                return row
            time.sleep(0.05)
        return None

    def complete(self, uuid: str) -> None:
        with self._pool.connection() as conn:
            conn.execute(
                "UPDATE crawl_queue SET status = 'done' WHERE uuid = %s",
                (uuid,),
            )

    def qsize(self) -> int:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM crawl_queue WHERE status IN ('pending', 'in_progress')"
            ).fetchone()
        return int(row[0]) if row else 0

    def join(self) -> None:
        """Block until no pending or in_progress tasks remain."""
        while True:
            with self._pool.connection() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM crawl_queue WHERE status IN ('pending', 'in_progress')"
                ).fetchone()
            if not row or int(row[0]) == 0:
                return
            time.sleep(0.5)

    # ── Node registration / heartbeat ────────────────────────────────────────

    def register_node(
        self,
        node_id: str,
        host: str,
        worker_count: int,
        proxy_count: int,
    ) -> None:
        """Upsert this worker node's presence into worker_nodes."""
        with self._pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO worker_nodes (node_id, host, last_heartbeat, worker_count, proxy_count, status)
                VALUES (%s, %s, NOW(), %s, %s, 'active')
                ON CONFLICT (node_id) DO UPDATE SET
                    host = EXCLUDED.host,
                    last_heartbeat = NOW(),
                    worker_count = EXCLUDED.worker_count,
                    proxy_count = EXCLUDED.proxy_count,
                    status = 'active'
                """,
                (node_id, host, worker_count, proxy_count),
            )

    def deregister_node(self, node_id: str) -> None:
        """Mark this node as inactive."""
        with self._pool.connection() as conn:
            conn.execute(
                "UPDATE worker_nodes SET status = 'inactive', last_heartbeat = NOW() WHERE node_id = %s",
                (node_id,),
            )

    def start_heartbeat(
        self,
        node_id: str,
        host: str,
        worker_count: int,
        proxy_count: int,
        interval_seconds: float = 30.0,
    ) -> None:
        """Start a background daemon thread that calls register_node on a loop."""
        self._stop_heartbeat = threading.Event()

        def _loop() -> None:
            while not self._stop_heartbeat.wait(timeout=interval_seconds):
                try:
                    self.register_node(node_id, host, worker_count, proxy_count)
                    log.debug("Heartbeat sent for node %s", node_id)
                except Exception:
                    log.debug("Heartbeat error for node %s", node_id, exc_info=True)

        self._heartbeat_thread = threading.Thread(
            target=_loop,
            daemon=True,
            name=f"heartbeat-{node_id}",
        )
        self._heartbeat_thread.start()

    def stop_heartbeat(self) -> None:
        """Stop the background heartbeat thread."""
        if hasattr(self, "_stop_heartbeat"):
            self._stop_heartbeat.set()

    # ── GC for stale in-progress tasks ───────────────────────────────────────

    def gc_stale_tasks(self) -> int:
        """Re-queue in_progress tasks that have been claimed for longer than claim_timeout."""
        with self._pool.connection() as conn:
            result = conn.execute(
                """
                UPDATE crawl_queue
                SET status = 'pending', claimed_at = NULL, worker_id = NULL
                WHERE status = 'in_progress'
                  AND claimed_at < NOW() - MAKE_INTERVAL(secs => %s)
                """,
                (self._claim_timeout,),
            )
            return result.rowcount

    def start_gc(self, interval: float = 60.0) -> None:
        """Start a background GC thread that re-queues stale tasks every `interval` seconds."""
        self._stop_gc.clear()

        def _loop() -> None:
            while not self._stop_gc.wait(timeout=interval):
                try:
                    n = self.gc_stale_tasks()
                    if n:
                        log.info("PostgresTaskQueue GC: re-queued %d stale tasks", n)
                except Exception:
                    log.debug("PostgresTaskQueue GC error", exc_info=True)

        self._gc_thread = threading.Thread(target=_loop, daemon=True, name="crawl-queue-gc")
        self._gc_thread.start()

    def stop_gc(self) -> None:
        self._stop_gc.set()

    # ── Internal ──────────────────────────────────────────────────────────────

    def _try_claim(self) -> tuple[str, int] | None:
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                UPDATE crawl_queue
                SET status = 'in_progress',
                    claimed_at = NOW(),
                    worker_id = %s
                WHERE uuid = (
                    SELECT uuid FROM crawl_queue
                    WHERE status = 'pending'
                    ORDER BY enqueued_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING uuid::text, depth
                """,
                (self._node_id,),
            ).fetchone()
        if row:
            return (str(row[0]), int(row[1]))
        return None
