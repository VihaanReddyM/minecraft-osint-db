from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from psycopg_pool import ConnectionPool

    from mcosint.crawl.task_queue import TaskQueue
    from mcosint.crawl.namemc_friends import CrawlConfig
    from mcosint.metrics.recorder import MetricsRecorder
    from mcosint.notify.base import Notifier
    from mcosint.proxy.pool import ProxyProvider

from mcosint.db.operations import (
    get_friend_uuids_from_db,
    is_player_friends_crawled,
    persist_namemc_friend_response,
    upsert_discovered_players,
)

log = logging.getLogger(__name__)


@dataclass
class CrawlMetrics:
    requests_used: int = 0
    db_writes: int = 0
    rate_limits: int = 0
    errors: int = 0
    seen_uuids: int = 0
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "requests_used": self.requests_used,
            "db_writes": self.db_writes,
            "rate_limits": self.rate_limits,
            "errors": self.errors,
            "seen_uuids": self.seen_uuids,
            "duration_seconds": round(self.duration_seconds, 2),
        }


def build_mesh_from_db(
    conn: Any,
    start_uuid: str,
    *,
    max_depth: int,
    max_friends_per_user: int | None = None,
) -> dict[str, list[str]]:
    """Reconstruct the friend mesh from the DB after a crawl completes.

    Returns {uuid: [friend_uuid, ...]} for all nodes reachable within max_depth.
    """
    from mcosint.util.uuid_tools import normalize_uuid_str

    start_uuid = normalize_uuid_str(start_uuid)
    mesh: dict[str, list[str]] = {}
    frontier: deque[tuple[str, int]] = deque([(start_uuid, 0)])
    visited: set[str] = set()

    while frontier:
        uuid, depth = frontier.popleft()
        if depth > max_depth or uuid in visited:
            continue
        visited.add(uuid)
        friends = get_friend_uuids_from_db(conn, player_uuid=uuid, limit=max_friends_per_user)
        mesh[uuid] = friends
        for f in friends:
            if f not in visited:
                frontier.append((f, depth + 1))

    return mesh


class CrawlEngine:
    """Unified BFS crawl engine with pluggable task queue, metrics, proxy pool, and notifier.

    Both `crawl-friends-db` and `friends-mesh` CLI commands instantiate this engine.
    For `friends-mesh`, call `build_mesh_from_db()` after `run()` to reconstruct the mesh.

    Default dependencies:
      - task_queue: InProcessTaskQueue (in-memory, deduplication built in)
      - metrics: NoopMetricsRecorder (zero overhead)
      - proxy_pool: None (direct connections)
      - notifier: None (no notifications)
    """

    def __init__(
        self,
        *,
        db_pool: ConnectionPool,
        config: CrawlConfig,
        task_queue: TaskQueue | None = None,
        proxy_pool: ProxyProvider | None = None,
        metrics: MetricsRecorder | None = None,
        notifier: Notifier | None = None,
    ) -> None:
        from mcosint.crawl.task_queue import InProcessTaskQueue
        from mcosint.metrics.recorder import NoopMetricsRecorder

        self._pool = db_pool
        self._cfg = config
        self._task_queue = task_queue if task_queue is not None else InProcessTaskQueue()
        self._proxy_pool = proxy_pool
        self._metrics = metrics if metrics is not None else NoopMetricsRecorder()
        self._notifier = notifier

    def run(self, seeds: list[str]) -> CrawlMetrics:
        """Execute BFS from seeds and return metrics. Blocks until complete."""
        from concurrent.futures import ThreadPoolExecutor

        from mcosint.crawl.namemc_friends import GlobalCooldown, RequestBudget

        t_start = time.monotonic()
        result = CrawlMetrics()

        seeds = list(dict.fromkeys(s.strip() for s in seeds if s.strip()))
        if not seeds:
            raise ValueError("No valid seed UUIDs provided")

        try:
            with self._pool.connection() as conn:
                upsert_discovered_players(conn, ((u, None, 0) for u in seeds))
        except Exception:
            log.exception("Failed to upsert seed players — continuing anyway")

        for seed in seeds:
            self._task_queue.enqueue(seed, 0)

        has_proxies = self._proxy_pool is not None and bool(self._proxy_pool.all_proxies())
        budget = RequestBudget(self._cfg.max_total_requests)
        shared_cooldown = GlobalCooldown()
        stop_event = threading.Event()

        result_lock = threading.Lock()

        def worker(worker_id: int) -> None:
            from mcosint.crawl.rate_limiter import WorkerRateLimiter
            from mcosint.services.namemc_threaded import (
                RateLimitedError,
                ThreadedNameMCClient,
                ThreadedNameMCConfig,
            )

            proxy = self._proxy_pool.get_proxy(worker_id) if self._proxy_pool else None
            worker_fs_url = self._cfg.flaresolverr_url if not proxy else None
            use_flaresolverr = bool(worker_fs_url)

            namemc_cfg = ThreadedNameMCConfig(
                user_agent=self._cfg.user_agent,
                timeout_seconds=self._cfg.timeout_seconds,
                flaresolverr_url=worker_fs_url,
                flaresolverr_max_timeout_ms=self._cfg.flaresolverr_max_timeout_ms,
                proxy_url=proxy.httpx_url() if proxy else None,
            )
            namemc_client = ThreadedNameMCClient(namemc_cfg)
            worker_limiter = WorkerRateLimiter()

            vpn_tunnel = None
            if self._proxy_pool is not None and hasattr(self._proxy_pool, "get_tunnel"):
                vpn_tunnel = self._proxy_pool.get_tunnel(worker_id)

            proxy_str = proxy.httpx_url() if proxy else None

            try:
                while not stop_event.is_set():
                    item = self._task_queue.dequeue(timeout=0.5)
                    if item is None:
                        continue

                    uuid, depth = item
                    try:
                        if stop_event.is_set() or depth > self._cfg.max_depth:
                            continue

                        from mcosint.util.uuid_tools import maybe_normalize_uuid_str
                        uuid_norm = maybe_normalize_uuid_str(uuid)
                        if uuid_norm is None:
                            log.warning("worker=%s skipping invalid uuid %r", worker_id, uuid)
                            continue
                        uuid = uuid_norm

                        # DB cache path: skip API if already crawled.
                        try:
                            with self._pool.connection() as conn:
                                if (not self._cfg.force_recrawl) and is_player_friends_crawled(conn, uuid):
                                    friend_uuids = get_friend_uuids_from_db(
                                        conn, player_uuid=uuid, limit=self._cfg.max_friends_per_user
                                    )
                                    for f_uuid in friend_uuids:
                                        self._task_queue.enqueue(f_uuid, depth + 1)
                                    continue
                        except Exception as e:
                            log.warning("worker=%s db read error uuid=%s err=%s", worker_id, uuid, e)

                        # HTTP fetch with retry loop.
                        attempt = 0
                        while attempt <= self._cfg.max_retries_per_uuid and not stop_event.is_set():
                            attempt += 1

                            worker_limiter.wait_if_needed()
                            if not has_proxies:
                                shared_cooldown.wait_if_needed()

                            if not budget.consume():
                                stop_event.set()
                                break

                            t_req = time.monotonic()
                            try:
                                data = namemc_client.get_friends(uuid, use_flaresolverr=use_flaresolverr)
                            except RateLimitedError as e:
                                backoff = e.retry_after_seconds or self._cfg.rate_limit_backoff_seconds
                                log.warning(
                                    "worker=%s rate-limited uuid=%s backoff=%.1fs",
                                    worker_id, uuid, backoff,
                                )
                                self._metrics.inc_rate_limits(worker_id, proxy_str)
                                worker_limiter.trigger(backoff)
                                if not has_proxies:
                                    shared_cooldown.trigger(backoff)
                                with result_lock:
                                    result.rate_limits += 1
                                if self._notifier is not None:
                                    try:
                                        self._notifier.send(embed={
                                            "title": "Rate Limited",
                                            "description": f"worker={worker_id} uuid=`{uuid}` wait={backoff}s",
                                            "color": 15158332,
                                        })
                                    except Exception:
                                        pass
                                continue
                            except Exception as e:
                                elapsed_ms = (time.monotonic() - t_req) * 1000
                                self._metrics.inc_requests(worker_id, proxy_str, success=False)
                                # VPN tunnel fault detection
                                if vpn_tunnel is not None and not vpn_tunnel.health_check():
                                    log.warning(
                                        "worker=%s tunnel %s is down, restarting...",
                                        worker_id, vpn_tunnel.ns_name,
                                    )
                                    try:
                                        ok = self._proxy_pool.manager.restart_tunnel(vpn_tunnel)
                                        if ok:
                                            log.info("worker=%s tunnel restarted", worker_id)
                                            namemc_client.close()
                                            namemc_client = ThreadedNameMCClient(namemc_cfg)
                                            continue
                                    except Exception as re:
                                        log.error("worker=%s tunnel restart failed: %s", worker_id, re)
                                log.warning(
                                    "worker=%s transport error uuid=%s attempt=%s err=%s",
                                    worker_id, uuid, attempt, e,
                                )
                                with result_lock:
                                    result.errors += 1
                                time.sleep(min(2.0 * attempt, 10.0))
                                continue

                            elapsed_ms = (time.monotonic() - t_req) * 1000
                            self._metrics.inc_requests(worker_id, proxy_str, success=True)
                            self._metrics.record_request_duration_ms(worker_id, elapsed_ms)

                            data = data[: self._cfg.max_friends_per_user]

                            try:
                                with self._pool.connection() as conn:
                                    friends_norm = persist_namemc_friend_response(
                                        conn,
                                        player_uuid=uuid,
                                        player_username=None,
                                        player_depth=depth,
                                        friends=data,
                                    )
                                self._metrics.inc_db_writes(worker_id, 1)
                                with result_lock:
                                    result.db_writes += 1
                            except Exception as e:
                                log.warning(
                                    "worker=%s db write error uuid=%s attempt=%s err=%s",
                                    worker_id, uuid, attempt, e,
                                )
                                time.sleep(min(2.0 * attempt, 10.0))
                                continue

                            for f_uuid, _ in friends_norm:
                                self._task_queue.enqueue(f_uuid, depth + 1)

                            with result_lock:
                                result.requests_used += 1

                            if self._cfg.request_delay_seconds > 0:
                                time.sleep(self._cfg.request_delay_seconds)
                            break
                    finally:
                        self._task_queue.complete(uuid)
            finally:
                namemc_client.close()

        log.info(
            "CrawlEngine starting: seeds=%d depth=%d threads=%d",
            len(seeds), self._cfg.max_depth, self._cfg.thread_count,
        )

        with ThreadPoolExecutor(max_workers=self._cfg.thread_count) as ex:
            futures = [ex.submit(worker, i) for i in range(self._cfg.thread_count)]
            try:
                self._task_queue.join()
            except KeyboardInterrupt:
                log.warning("KeyboardInterrupt — stopping workers")
            finally:
                stop_event.set()
                for f in futures:
                    f.result()

        result.duration_seconds = time.monotonic() - t_start
        result.seen_uuids = (
            self._task_queue.seen_count
            if hasattr(self._task_queue, "seen_count")
            else result.requests_used
        )

        snap = self._metrics.snapshot()
        if snap:
            log.info("CrawlEngine metrics: %s", snap)

        return result
