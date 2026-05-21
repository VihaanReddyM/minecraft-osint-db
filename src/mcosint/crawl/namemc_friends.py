from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from queue import Empty, Queue
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mcosint.proxy.pool import ProxyProvider

from mcosint.crawl.rate_limiter import WorkerRateLimiter
from mcosint.db.operations import (
    get_friend_uuids_from_db,
    is_player_friends_crawled,
    persist_namemc_friend_response,
    upsert_discovered_players,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CrawlConfig:
    thread_count: int = 5
    request_delay_seconds: float = 1.0
    max_depth: int = 2
    max_total_requests: int | None = None  # None = unlimited
    max_friends_per_user: int = 50
    rate_limit_backoff_seconds: float = 10.0
    max_retries_per_uuid: int = 3
    force_recrawl: bool = False
    user_agent: str = "mcosint/0.1.0"
    # Proxy / HTTP settings (Phase 1)
    proxy_list: list[str] = field(default_factory=list)
    flaresolverr_url: str | None = None
    flaresolverr_max_timeout_ms: int = 60_000
    timeout_seconds: float = 30.0


def load_crawl_config_from_env() -> CrawlConfig:
    return CrawlConfig(
        thread_count=int(os.getenv("THREAD_COUNT", "5")),
        request_delay_seconds=float(os.getenv("REQUEST_DELAY", "1.0")),
        max_depth=int(os.getenv("MAX_DEPTH", "2")),
        max_total_requests=int(os.getenv("MAX_TOTAL_REQUESTS", "0")) or None,
        max_friends_per_user=int(os.getenv("MAX_FRIENDS_PER_USER", "50")),
        rate_limit_backoff_seconds=float(os.getenv("RATE_LIMIT_BACKOFF_SECONDS", "10")),
        max_retries_per_uuid=int(os.getenv("MAX_RETRIES_PER_UUID", "3")),
        force_recrawl=os.getenv("FORCE_RECRAWL", "false").strip().lower() in {"1", "true", "yes", "on"},
    )


class GlobalCooldown:
    """Shared cross-thread cooldown used as a circuit breaker when no proxies are configured.

    When any worker hits 429 and all workers share the same outbound IP, this pauses all
    workers until the cooldown expires. With per-worker proxy isolation, use WorkerRateLimiter
    instead and leave GlobalCooldown dormant.
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

    def __init__(self, max_total_requests: int | None) -> None:
        self._max = max_total_requests
        self._lock = threading.Lock()
        self._used = 0

    def consume(self) -> bool:
        if self._max is None:
            with self._lock:
                self._used += 1
            return True
        with self._lock:
            if self._used >= self._max:
                return False
            self._used += 1
            return True

    @property
    def used(self) -> int:
        with self._lock:
            return self._used


def _parse_retry_after_seconds(headers) -> float | None:
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
    start_uuids: list[str],
    cfg: CrawlConfig,
    user_agent: str = "mcosint/0.1.0",
    proxy_pool: ProxyProvider | None = None,
) -> dict[str, int]:
    """Crawl NameMC friends using BFS and persist results to PostgreSQL.

    Concurrency model:
      - ThreadPoolExecutor with cfg.thread_count workers
      - Shared Queue[(uuid, depth)] as the BFS frontier
      - Thread-safe seen set to avoid duplicate API calls
      - GlobalCooldown as shared circuit breaker (used only when no proxies)
      - WorkerRateLimiter per worker (used always; isolates 429 backoff per proxy)
      - RequestBudget caps total HTTP requests

    Proxy isolation (when proxy_pool is provided):
      - Each worker is assigned a proxy via proxy_pool.get_proxy(worker_id)
      - Workers with a proxy use their own outbound IP; 429 only pauses that worker
      - Workers without proxies fall back to host IP and share GlobalCooldown

    FlareSolverr (when cfg.flaresolverr_url is set):
      - Workers without a proxy route requests through FlareSolverr
      - Workers with a proxy use direct HTTP through their proxy (no FlareSolverr)
    """

    from concurrent.futures import ThreadPoolExecutor

    # Build proxy pool from cfg.proxy_list if proxy_pool not supplied explicitly.
    effective_proxy_pool = proxy_pool
    if effective_proxy_pool is None and cfg.proxy_list:
        from mcosint.proxy.pool import StaticProxyPool
        effective_proxy_pool = StaticProxyPool.from_urls(cfg.proxy_list)

    has_proxies = effective_proxy_pool is not None and bool(
        effective_proxy_pool.all_proxies()
    )

    q: Queue[tuple[str, int] | None] = Queue()

    seen_lock = threading.Lock()
    seen_depth: dict[str, int] = {}

    global_cooldown = GlobalCooldown()
    budget = RequestBudget(cfg.max_total_requests)

    stop_event = threading.Event()

    def enqueue(uuid: str, depth: int) -> None:
        if depth > cfg.max_depth:
            return
        with seen_lock:
            prev = seen_depth.get(uuid)
            if prev is not None and depth >= prev:
                return
            seen_depth[uuid] = depth
        q.put((uuid, depth))

    start_uuids = [u.strip() for u in start_uuids if u.strip()]
    start_uuids = list(dict.fromkeys(start_uuids))

    try:
        with pool.connection() as conn:
            upsert_discovered_players(conn, ((u, None, 0) for u in start_uuids))
    except Exception:
        log.exception("Failed to upsert seed players; continuing anyway")

    for u in start_uuids:
        enqueue(u, 0)

    def worker(worker_id: int) -> None:
        from mcosint.services.namemc_threaded import (
            RateLimitedError,
            ThreadedNameMCClient,
            ThreadedNameMCConfig,
        )

        proxy = effective_proxy_pool.get_proxy(worker_id) if effective_proxy_pool else None

        # VPN tunnel reference — present only when using VPNProxyProvider
        vpn_tunnel = None
        if effective_proxy_pool is not None and hasattr(effective_proxy_pool, "get_tunnel"):
            vpn_tunnel = effective_proxy_pool.get_tunnel(worker_id)

        # Workers with a proxy route directly through it (no FlareSolverr).
        # Workers without a proxy use FlareSolverr if configured.
        worker_flaresolverr_url = cfg.flaresolverr_url if not proxy else None
        use_flaresolverr = bool(worker_flaresolverr_url)

        namemc_cfg = ThreadedNameMCConfig(
            user_agent=user_agent,
            timeout_seconds=cfg.timeout_seconds,
            flaresolverr_url=worker_flaresolverr_url,
            flaresolverr_max_timeout_ms=cfg.flaresolverr_max_timeout_ms,
            proxy_url=proxy.httpx_url() if proxy else None,
        )
        namemc_client = ThreadedNameMCClient(namemc_cfg)
        worker_limiter = WorkerRateLimiter()

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

                if stop_event.is_set() or depth > cfg.max_depth:
                    q.task_done()
                    continue

                with seen_lock:
                    current_best = seen_depth.get(uuid)
                if current_best is not None and depth != current_best:
                    q.task_done()
                    continue

                # Resume-friendly: expand from DB if already crawled.
                try:
                    with pool.connection() as conn:
                        if (not cfg.force_recrawl) and is_player_friends_crawled(conn, uuid):
                            friend_uuids = get_friend_uuids_from_db(
                                conn,
                                player_uuid=uuid,
                                limit=cfg.max_friends_per_user,
                            )
                            for f_uuid in friend_uuids:
                                enqueue(f_uuid, depth + 1)

                            q.task_done()
                            continue
                except Exception as e:
                    log.warning(
                        "worker=%s db read error uuid=%s err=%s",
                        worker_id,
                        uuid,
                        e,
                    )

                # Fetch with retries.
                attempt = 0
                while attempt <= cfg.max_retries_per_uuid and not stop_event.is_set():
                    attempt += 1

                    # Per-worker backoff first (isolates this proxy's rate limit).
                    worker_limiter.wait_if_needed()
                    # Shared circuit breaker (only meaningful when sharing an IP).
                    if not has_proxies:
                        global_cooldown.wait_if_needed()

                    if not budget.consume():
                        stop_event.set()
                        break

                    try:
                        data = namemc_client.get_friends(uuid, use_flaresolverr=use_flaresolverr)
                    except RateLimitedError as e:
                        backoff = e.retry_after_seconds or cfg.rate_limit_backoff_seconds
                        log.warning(
                            "worker=%s rate limited (429) uuid=%s backoff=%.1fs proxy=%s",
                            worker_id,
                            uuid,
                            backoff,
                            proxy.httpx_url() if proxy else "none",
                        )
                        worker_limiter.trigger(backoff)
                        if not has_proxies:
                            global_cooldown.trigger(backoff)
                        continue
                    except Exception as e:
                        if vpn_tunnel is not None and not vpn_tunnel.health_check():
                            log.warning(
                                "worker=%s tunnel %s is down, restarting...",
                                worker_id,
                                vpn_tunnel.ns_name,
                            )
                            try:
                                ok = effective_proxy_pool.manager.restart_tunnel(vpn_tunnel)
                                if ok:
                                    log.info(
                                        "worker=%s tunnel %s restarted successfully",
                                        worker_id,
                                        vpn_tunnel.ns_name,
                                    )
                                    namemc_client.close()
                                    namemc_client = ThreadedNameMCClient(namemc_cfg)
                                    continue
                            except Exception as restart_err:
                                log.error(
                                    "worker=%s tunnel restart failed: %s", worker_id, restart_err
                                )
                        log.warning(
                            "worker=%s transport error uuid=%s attempt=%s err=%s",
                            worker_id,
                            uuid,
                            attempt,
                            e,
                        )
                        time.sleep(min(2.0 * attempt, 10.0))
                        continue

                    data = data[: cfg.max_friends_per_user]

                    try:
                        with pool.connection() as conn:
                            friends_norm = persist_namemc_friend_response(
                                conn,
                                player_uuid=uuid,
                                player_username=None,
                                player_depth=depth,
                                friends=data,
                            )
                    except Exception as e:
                        log.warning(
                            "worker=%s db error uuid=%s attempt=%s err=%s",
                            worker_id,
                            uuid,
                            attempt,
                            e,
                        )
                        time.sleep(min(2.0 * attempt, 10.0))
                        continue

                    for f_uuid, _f_name in friends_norm:
                        enqueue(f_uuid, depth + 1)

                    if cfg.request_delay_seconds > 0:
                        time.sleep(cfg.request_delay_seconds)

                    break

                q.task_done()
        finally:
            namemc_client.close()

    with ThreadPoolExecutor(max_workers=cfg.thread_count) as ex:
        futures = [ex.submit(worker, i) for i in range(cfg.thread_count)]

        try:
            q.join()
        except KeyboardInterrupt:
            log.warning("KeyboardInterrupt received; stopping workers...")
        finally:
            stop_event.set()
            for _ in range(cfg.thread_count):
                q.put(None)

            for f in futures:
                f.result()

    return {
        "requests_used": budget.used,
        "seen_uuids": len(seen_depth),
    }
