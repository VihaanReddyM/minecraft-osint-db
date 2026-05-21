from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import Any, Protocol

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from psycopg_pool import ConnectionPool

    from mcosint.proxy.pool import ProxyProvider
    from mcosint.services.namemc_threaded import ThreadedNameMCConfig
else:
    try:
        from psycopg_pool import ConnectionPool
    except ModuleNotFoundError:  # pragma: no cover
        ConnectionPool = Any  # type: ignore[misc,assignment]

    ProxyProvider = Any  # type: ignore[misc,assignment]
    ThreadedNameMCConfig = Any  # type: ignore[misc,assignment]

from mcosint.crawl.rate_limiter import WorkerRateLimiter
from mcosint.util.uuid_tools import maybe_normalize_uuid_str


class FriendUUIDProvider(Protocol):
    async def get_friend_uuids(self, uuid: str, *, use_flaresolverr: bool = False) -> list[str]: ...


class FriendProvider(Protocol):
    async def get_friends(self, uuid: str, *, use_flaresolverr: bool = False) -> list[dict[str, Any]]: ...


class BlockingFriendProvider(Protocol):
    def get_friends(self, uuid: str, *, use_flaresolverr: bool) -> list[dict[str, Any]]: ...

log = logging.getLogger(__name__)


async def build_friend_mesh(
    *,
    namemc: FriendProvider,
    start_uuid: str,
    max_depth: int = 2,
    delay_seconds: float = 1.0,
    max_total_calls: int | None = None,
    max_friends_per_user: int | None = None,
    use_flaresolverr: bool = False,
    pool: ConnectionPool | None = None,
    force_recrawl: bool = False,
) -> dict[str, list[str]]:
    """Build a sample friend mesh (BFS) using the NameMC API.

    This mirrors your original `main.py` behavior, but is async and injectable.
    """

    visited: set[str] = set()
    mesh: dict[str, list[str]] = {}
    queue: deque[tuple[str, int]] = deque([(start_uuid, 0)])

    calls_made = 0

    if max_total_calls is None:
        log.info(
            "Starting mesh for %s (max_depth=%s, max_friends_per_user=%s)",
            start_uuid,
            max_depth,
            max_friends_per_user,
        )
    else:
        log.info(
            "Starting mesh for %s (max_depth=%s, max_total_calls=%s, max_friends_per_user=%s)",
            start_uuid,
            max_depth,
            max_total_calls,
            max_friends_per_user,
        )

    # If DB is enabled, ensure the seed exists so friendship inserts don't fail on FK.
    if pool is not None:
        from mcosint.db.operations import upsert_discovered_players

        try:
            with pool.connection() as conn:
                upsert_discovered_players(conn, [(start_uuid, None, 0)])
        except Exception:
            log.exception("Failed to upsert seed player %s", start_uuid)
            raise

    while queue:
        if max_total_calls is not None and calls_made >= max_total_calls:
            log.info("Reached max_total_calls=%s; stopping.", max_total_calls)
            break

        current_uuid, depth = queue.popleft()

        # If we have a UUID-ish value, canonicalize it early so visited checks work.
        uuid_norm = maybe_normalize_uuid_str(current_uuid)
        if uuid_norm is not None:
            current_uuid = uuid_norm
        elif pool is not None:
            # In DB-backed mode we expect real UUIDs. Skip anything malformed to avoid
            # bad HTTP URLs like /profile/b'...'/friends.
            log.warning("Skipping invalid uuid value: %r", current_uuid)
            visited.add(current_uuid)
            continue

        if depth > max_depth:
            continue
        if current_uuid in visited:
            continue

        # Resume/caching path: if already crawled, expand from DB without API.
        if pool is not None:
            from mcosint.db.operations import get_friend_uuids_from_db, is_player_friends_crawled

            try:
                with pool.connection() as conn:
                    if (not force_recrawl) and is_player_friends_crawled(conn, current_uuid):
                        friends = get_friend_uuids_from_db(
                            conn,
                            player_uuid=current_uuid,
                            limit=max_friends_per_user,
                        )
                        mesh[current_uuid] = friends
                        visited.add(current_uuid)

                        for f_uuid in friends:
                            if f_uuid not in visited:
                                queue.append((f_uuid, depth + 1))

                        if delay_seconds > 0:
                            await asyncio.sleep(delay_seconds)
                        continue
            except Exception:
                # If DB read fails, fall back to HTTP.
                log.exception("DB read failed for %s", current_uuid)
                raise

        calls_made += 1
        if max_total_calls is None:
            log.info("[%s] Fetching %s (depth=%s)", calls_made, current_uuid, depth)
        else:
            log.info("[%s/%s] Fetching %s (depth=%s)", calls_made, max_total_calls, current_uuid, depth)

        try:
            friends_payload = await namemc.get_friends(current_uuid, use_flaresolverr=use_flaresolverr)
        except Exception:
            log.exception("Failed to fetch friends for %s", current_uuid)
            visited.add(current_uuid)
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
            continue

        friends_norm: list[tuple[str, str | None]]

        if pool is not None:
            from mcosint.db.operations import persist_namemc_friend_response

            # Persist the full response (no branching limit) so later runs can use DB.
            try:
                with pool.connection() as conn:
                    friends_norm = persist_namemc_friend_response(
                        conn,
                        player_uuid=current_uuid,
                        player_username=None,
                        player_depth=depth,
                        friends=friends_payload,
                    )
            except Exception:
                log.exception("Failed to persist friends response for %s", current_uuid)
                raise
        else:
            # No DB: best-effort normalize from the API response.
            friends_norm = []
            for item in friends_payload:
                f_uuid = item.get("uuid")
                if f_uuid:
                    friends_norm.append((str(f_uuid), None))

        friends = [f_uuid for (f_uuid, _name) in friends_norm]
        if max_friends_per_user is not None:
            friends = friends[:max_friends_per_user]
        mesh[current_uuid] = friends
        visited.add(current_uuid)

        for f_uuid in friends:
            if f_uuid not in visited:
                queue.append((f_uuid, depth + 1))

        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)

    return mesh


def build_friend_mesh_threaded(
    *,
    namemc: BlockingFriendProvider,
    start_uuid: str,
    max_depth: int = 2,
    delay_seconds: float = 1.0,
    max_total_calls: int | None = None,
    max_friends_per_user: int | None = None,
    use_flaresolverr: bool = False,
    pool: ConnectionPool,
    threads: int = 5,
    rate_limit_backoff_seconds: float = 10.0,
    max_retries_per_uuid: int = 3,
    notifier=None,
    proxy_pool: ProxyProvider | None = None,
    namemc_cfg: ThreadedNameMCConfig | None = None,
) -> dict[str, list[str]]:
    """Threaded BFS that persists to DB and avoids repeat API requests.

    Returns the mesh mapping (uuid → [friend_uuid, ...]) for all traversed nodes.

    Proxy isolation (Phase 1):
      When `proxy_pool` and `namemc_cfg` are both provided, each worker creates its own
      ThreadedNameMCClient bound to its assigned proxy. Workers with a proxy route directly
      through it (no FlareSolverr); workers without fall back to the shared `namemc` client.
    """

    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor
    from queue import Empty, Queue

    if threads < 1:
        threads = 1

    start_uuid = str(start_uuid).strip()
    if not start_uuid:
        raise ValueError("start_uuid is required")

    start_uuid_norm = maybe_normalize_uuid_str(start_uuid)
    if start_uuid_norm is None:
        raise ValueError(f"start_uuid is not a valid uuid: {start_uuid!r}")
    start_uuid = start_uuid_norm

    has_proxy_pool = proxy_pool is not None and namemc_cfg is not None

    with pool.connection() as conn:
        from mcosint.db.operations import upsert_discovered_players

        upsert_discovered_players(conn, [(start_uuid, None, 0)])

    q: Queue[tuple[str, int] | None] = Queue()
    q.put((start_uuid, 0))

    visited_lock = threading.Lock()
    visited: set[str] = set()

    mesh_lock = threading.Lock()
    mesh: dict[str, list[str]] = {}

    calls_lock = threading.Lock()
    calls_made = 0

    stop_event = threading.Event()

    def can_take_call() -> bool:
        nonlocal calls_made
        if max_total_calls is None:
            with calls_lock:
                calls_made += 1
                return True
        with calls_lock:
            if calls_made >= max_total_calls:
                return False
            calls_made += 1
            return True

    def worker(worker_id: int) -> None:
        import dataclasses

        from mcosint.services.namemc_threaded import RateLimitedError, ThreadedNameMCClient

        # Per-worker client setup.
        worker_namemc_client: ThreadedNameMCClient | None = None
        if has_proxy_pool and proxy_pool is not None and namemc_cfg is not None:
            proxy = proxy_pool.get_proxy(worker_id)
            # Workers with a proxy use direct HTTP through it; skip FlareSolverr for them.
            worker_cfg = dataclasses.replace(
                namemc_cfg,
                proxy_url=proxy.httpx_url() if proxy else None,
                flaresolverr_url=namemc_cfg.flaresolverr_url if not proxy else None,
            )
            worker_namemc_client = ThreadedNameMCClient(worker_cfg)
            active_namemc: BlockingFriendProvider = worker_namemc_client
            worker_use_fs = bool(namemc_cfg.flaresolverr_url and not proxy)
        else:
            active_namemc = namemc
            worker_use_fs = use_flaresolverr

        vpn_tunnel = None
        if proxy_pool is not None and hasattr(proxy_pool, "get_tunnel"):
            vpn_tunnel = proxy_pool.get_tunnel(worker_id)

        worker_limiter = WorkerRateLimiter()

        def notify_rate_limited(uuid: str, wait: float) -> None:
            if notifier is None:
                return
            try:
                notifier.send(
                    embed={
                        "title": "Rate Limited",
                        "description": f"worker={worker_id} uuid=`{uuid}` wait={wait}s",
                        "color": 15158332,
                    }
                )
            except Exception:
                log.debug("notifier failed", exc_info=True)

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
                try:
                    if depth > max_depth:
                        continue

                    uuid_norm = maybe_normalize_uuid_str(uuid)
                    if uuid_norm is None:
                        log.warning("Skipping invalid uuid value: %r", uuid)
                        continue
                    uuid = uuid_norm

                    with visited_lock:
                        if uuid in visited:
                            continue
                        visited.add(uuid)

                    # DB cache path
                    with pool.connection() as conn:
                        from mcosint.db.operations import (
                            get_friend_uuids_from_db,
                            is_player_friends_crawled,
                        )

                        if is_player_friends_crawled(conn, uuid):
                            friends = get_friend_uuids_from_db(
                                conn,
                                player_uuid=uuid,
                                limit=max_friends_per_user,
                            )
                            with mesh_lock:
                                mesh[uuid] = friends
                            for f_uuid in friends:
                                q.put((f_uuid, depth + 1))
                            continue

                    # HTTP path
                    if not can_take_call():
                        stop_event.set()
                        return

                    log.info(
                        "[%s%s] Fetching %s (depth=%s)",
                        calls_made,
                        f"/{max_total_calls}" if max_total_calls is not None else "",
                        uuid,
                        depth,
                    )

                    worker_limiter.wait_if_needed()

                    try:
                        friends_payload = active_namemc.get_friends(
                            uuid, use_flaresolverr=worker_use_fs
                        )
                    except RateLimitedError as e:
                        wait = e.retry_after_seconds or rate_limit_backoff_seconds
                        log.warning(
                            "worker=%s rate limited uuid=%s; backing off %.1fs",
                            worker_id,
                            uuid,
                            wait,
                        )
                        notify_rate_limited(uuid, float(wait))
                        worker_limiter.trigger(wait)
                        with visited_lock:
                            visited.discard(uuid)
                        q.put((uuid, depth))
                        continue

                    except Exception as e:
                        if vpn_tunnel is not None and not vpn_tunnel.health_check():
                            log.warning(
                                "worker=%s tunnel %s is down, restarting...",
                                worker_id,
                                vpn_tunnel.ns_name,
                            )
                            try:
                                ok = proxy_pool.manager.restart_tunnel(vpn_tunnel)
                                if ok:
                                    log.info(
                                        "worker=%s tunnel %s restarted", worker_id, vpn_tunnel.ns_name
                                    )
                                    if worker_namemc_client is not None:
                                        worker_namemc_client.close()
                                    proxy = proxy_pool.get_proxy(worker_id)
                                    worker_cfg = dataclasses.replace(
                                        namemc_cfg,
                                        proxy_url=proxy.httpx_url() if proxy else None,
                                        flaresolverr_url=namemc_cfg.flaresolverr_url if not proxy else None,
                                    )
                                    worker_namemc_client = ThreadedNameMCClient(worker_cfg)
                                    active_namemc = worker_namemc_client
                            except Exception as restart_err:
                                log.error(
                                    "worker=%s tunnel restart failed: %s", worker_id, restart_err
                                )
                        attempt = 0
                        while attempt < max(1, max_retries_per_uuid):
                            attempt += 1
                            log.warning(
                                "worker=%s error uuid=%s attempt=%s err=%s",
                                worker_id,
                                uuid,
                                attempt,
                                e,
                            )
                            time.sleep(min(2.0 * attempt, 10.0))
                            try:
                                friends_payload = active_namemc.get_friends(
                                    uuid, use_flaresolverr=worker_use_fs
                                )
                                break
                            except RateLimitedError as e2:
                                wait = e2.retry_after_seconds or rate_limit_backoff_seconds
                                notify_rate_limited(uuid, float(wait))
                                worker_limiter.trigger(wait)
                                continue
                            except Exception as e2:
                                e = e2
                                continue
                        else:
                            log.exception(
                                "worker=%s exhausted retries uuid=%s", worker_id, uuid
                            )
                            continue

                    with pool.connection() as conn:
                        from mcosint.db.operations import persist_namemc_friend_response

                        friends_norm = persist_namemc_friend_response(
                            conn,
                            player_uuid=uuid,
                            player_username=None,
                            player_depth=depth,
                            friends=friends_payload,
                        )

                    friends = [f_uuid for (f_uuid, _name) in friends_norm]
                    if max_friends_per_user is not None:
                        friends = friends[:max_friends_per_user]
                    with mesh_lock:
                        mesh[uuid] = friends

                    for f_uuid in friends:
                        q.put((f_uuid, depth + 1))

                    if delay_seconds > 0:
                        time.sleep(delay_seconds)
                finally:
                    q.task_done()
        finally:
            if worker_namemc_client is not None:
                worker_namemc_client.close()

    log.info(
        "Starting mesh for %s (max_depth=%s, max_friends_per_user=%s, threads=%s)",
        start_uuid,
        max_depth,
        max_friends_per_user,
        threads,
    )

    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = [ex.submit(worker, i) for i in range(threads)]

        try:
            q.join()
        finally:
            stop_event.set()
            for _ in range(threads):
                q.put(None)
            for f in futures:
                f.result()

    return mesh
