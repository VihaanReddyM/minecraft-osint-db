from __future__ import annotations

import threading
import time

import pytest

from mcosint.crawl.task_queue import InProcessTaskQueue


class TestInProcessTaskQueue:
    def test_enqueue_dequeue(self) -> None:
        q = InProcessTaskQueue()
        q.enqueue("uuid-a", 0)
        item = q.dequeue(timeout=0.1)
        assert item == ("uuid-a", 0)

    def test_dequeue_timeout_returns_none(self) -> None:
        q = InProcessTaskQueue()
        item = q.dequeue(timeout=0.05)
        assert item is None

    def test_deduplication_same_depth(self) -> None:
        q = InProcessTaskQueue()
        q.enqueue("uuid-a", 1)
        q.enqueue("uuid-a", 1)  # duplicate — should be ignored
        assert q.qsize() == 1

    def test_deduplication_deeper_ignored(self) -> None:
        q = InProcessTaskQueue()
        q.enqueue("uuid-a", 1)
        q.enqueue("uuid-a", 2)  # deeper — ignored
        assert q.qsize() == 1

    def test_shallower_replaces_deeper(self) -> None:
        q = InProcessTaskQueue()
        q.enqueue("uuid-a", 3)
        q.enqueue("uuid-a", 1)  # shallower — should be enqueued again
        # Both are enqueued (the original stays + new shallower one)
        assert q.qsize() == 2

    def test_complete_unblocks_join(self) -> None:
        q = InProcessTaskQueue()
        q.enqueue("uuid-a", 0)
        item = q.dequeue(timeout=0.1)
        assert item is not None
        q.complete(item[0])
        # join() should return without blocking since we called complete
        q.join()

    def test_join_blocks_until_complete(self) -> None:
        q = InProcessTaskQueue()
        q.enqueue("uuid-a", 0)
        item = q.dequeue(timeout=0.1)
        assert item is not None

        results = []

        def _complete_later():
            time.sleep(0.05)
            q.complete(item[0])

        t = threading.Thread(target=_complete_later)
        t.start()
        t0 = time.monotonic()
        q.join()
        elapsed = time.monotonic() - t0
        assert elapsed >= 0.04

    def test_seen_count(self) -> None:
        q = InProcessTaskQueue()
        q.enqueue("uuid-a", 0)
        q.enqueue("uuid-b", 1)
        q.enqueue("uuid-a", 2)  # duplicate
        assert q.seen_count == 2

    def test_qsize(self) -> None:
        q = InProcessTaskQueue()
        assert q.qsize() == 0
        q.enqueue("uuid-a", 0)
        q.enqueue("uuid-b", 0)
        assert q.qsize() == 2
