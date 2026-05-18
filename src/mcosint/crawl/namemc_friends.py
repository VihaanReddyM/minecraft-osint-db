from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from queue import Empty, Queue

import httpx

from mcosint.db.operations import persist_namemc_friend_response

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CrawlConfig:
    thread_count: int = 5
    request_delay_seconds: float = 1.0
    max_depth: int = 2
    max_total_requests: int = 10
    max_friends_per_user: int = 3
    rate_limit_backoff_seconds: float = 10.0
    max_retries_per_uuid: int = 3


def load_crawl_config_from_env() -> CrawlConfig:
    return CrawlConfig(
        thread_count=int(os.getenv("THREAD_COUNT", "5")),
        request_delay_seconds=float(os.getenv("REQUEST_DELAY", "1.0")),
        max_depth=int(os.getenv("MAX_DEPTH", "2")),
        max_total_requests=int(os.getenv("MAX_TOTAL_REQUESTS", "100")),
        max_friends_per_user=int(os.getenv("MAX_FRIENDS_PER_USER", "50")),
        rate_limit_backoff_seconds=float(os.getenv("RATE_LIMIT_BACKOFF_SECONDS", "10")),
        max_retries_per_uuid=int(os.getenv("MAX_RETRIES_PER_UUID", "3")),
    )


class GlobalCooldown:
    """A simple cross-thread cooldown.

    When any thread hits 429, it can extend a global 'do not request before' timestamp.
    Other threads will wait before making requests.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._until = 0.0  # monotonic seconds

    def wait_if_needed(self) -> None:
        while True:
            with self._lock:
                until = self._until
            now = time.monotonic()
            if now >= until:
                return
            time.sleep(min(until - now, 1.0))

    def trigger(self, seconds: float) -> None:
        if seconds <= 0:
            return
        with self._lock:
            self._until = max(self._until, time.monotonic() + seconds)


class RequestBudget:
    """Thread-safe request budget to cap total HTTP requests."""

    def __init__(self, max_total_requests: int) -> None:
        self._max = max_total_requests
        self._lock = threading.Lock()
        self._used = 0

    def consume(self) -> bool:
        with self._lock:
            if self._used >= self._max:
                return False
            self._used += 1
            return True

    @property
    def used(self) -> int:
        with self._lock:
            return self._used





def _parse_retry_after_seconds(headers: httpx.Headers) -> float | None:
    ra = headers.get("Retry-After")
    if not ra:
        return None
    try:
        return float(ra)
    except ValueError:
        return None


def crawl_namemc_friends_to_db(
    *,
    pool,
    start_uuid: str,
    cfg: CrawlConfig,
    user_agent: str = "mcosint/0.1.0",
) -> dict[str, int]:
    """Crawl NameMC friends using a BFS frontier and persist results to PostgreSQL.

    Concurrency model:
      - A single shared `Queue[(uuid, depth)]`
      - A thread-safe `seen` set to avoid duplicate API calls across threads
      - A global request budget (caps total HTTP requests)
      - A global cooldown to coordinate 429 backoff

    Persistence:
      - Each successful response is written immediately to the DB in the worker thread.

    Returns basic run metrics.
    """

    from concurrent.futures import ThreadPoolExecutor

    q: Queue[tuple[str, int] | None] = Queue()

    seen_lock = threading.Lock()
    seen: set[str] = set()

    cooldown = GlobalCooldown()
    budget = RequestBudget(cfg.max_total_requests)

    stop_event = threading.Event()

    def enqueue(uuid: str, depth: int) -> None:
        if depth > cfg.max_depth:
            return
        with seen_lock:
            if uuid in seen:
                return
            seen.add(uuid)
        q.put((uuid, depth))

    enqueue(start_uuid, 0)

    def worker(worker_id: int) -> None:
        client = httpx.Client(
            headers={"User-Agent": user_agent},
            timeout=httpx.Timeout(30.0),
        )
        try:
            while not stop_event.is_set():
                try:
                    item = q.get(timeout=0.5)
                except Empty:
                    continue

                if item is None:
                    q.task_done()
                    return

                uuid, depth = item

                if depth > cfg.max_depth or stop_event.is_set():
                    q.task_done()
                    continue

                # Fetch with retries for this uuid.
                attempt = 0
                while attempt <= cfg.max_retries_per_uuid and not stop_event.is_set():
                    attempt += 1

                    cooldown.wait_if_needed()

                    if not budget.consume():
                        stop_event.set()
                        break

                    url = f"https://api.namemc.com/profile/{uuid}/friends"

                    try:
                        resp = client.get(url)
                    except httpx.TransportError as e:
                        log.warning(
                            "worker=%s transport error uuid=%s attempt=%s err=%s",
                            worker_id,
                            uuid,
                            attempt,
                            e,
                        )
                        time.sleep(min(2.0 * attempt, 10.0))
                        continue

                    if resp.status_code == 429:
                        backoff = _parse_retry_after_seconds(resp.headers) or cfg.rate_limit_backoff_seconds
                        log.warning(
                            "worker=%s rate limited (429) uuid=%s backoff=%ss",
                            worker_id,
                            uuid,
                            backoff,
                        )
                        cooldown.trigger(backoff)
                        # Sleep this worker, but other workers will also honor the global cooldown.
                        time.sleep(min(backoff, 30.0))
                        continue

                    if resp.status_code != 200:
                        log.info(
                            "worker=%s non-200 uuid=%s status=%s",
                            worker_id,
                            uuid,
                            resp.status_code,
                        )
                        break

                    try:
                        data = resp.json()
                    except ValueError:
                        log.info("worker=%s invalid json uuid=%s", worker_id, uuid)
                        break

                    if not isinstance(data, list):
                        log.info(
                            "worker=%s unexpected response uuid=%s type=%s",
                            worker_id,
                            uuid,
                            type(data),
                        )
                        break

                    # Limit branching.
                    data = data[: cfg.max_friends_per_user]

                    # Persist immediately.
                    try:
                        with pool.connection() as conn:
                            friends_norm = persist_namemc_friend_response(
                                conn,
                                player_uuid=uuid,
                                player_username=None,
                                friends=data,
                            )
                    except Exception as e:
                        # DB might be down or deadlocked; log and retry this uuid.
                        log.warning(
                            "worker=%s db error uuid=%s attempt=%s err=%s",
                            worker_id,
                            uuid,
                            attempt,
                            e,
                        )
                        time.sleep(min(2.0 * attempt, 10.0))
                        continue

                    # Enqueue discovered friends.
                    for f_uuid, _f_name in friends_norm:
                        enqueue(f_uuid, depth + 1)

                    # Respect per-request delay (doesn't block other threads).
                    if cfg.request_delay_seconds > 0:
                        time.sleep(cfg.request_delay_seconds)

                    break

                q.task_done()
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=cfg.thread_count) as ex:
        futures = [ex.submit(worker, i) for i in range(cfg.thread_count)]

        try:
            # Wait until the queue is fully drained.
            q.join()
        except KeyboardInterrupt:
            log.warning("KeyboardInterrupt received; stopping workers...")
        finally:
            # Stop workers.
            stop_event.set()
            for _ in range(cfg.thread_count):
                q.put(None)

            for f in futures:
                f.result()

    return {
        "requests_used": budget.used,
        "seen_uuids": len(seen),
    }
