from __future__ import annotations

import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from mcosint.config import load_config
from mcosint.db.connection import DbPoolConfig, create_pool
from mcosint.db.schema import create_schema
from mcosint.graph.friend_mesh import build_friend_mesh_threaded
from mcosint.notify.discord import DiscordWebhook
from mcosint.services.namemc_threaded import ThreadedNameMCClient, ThreadedNameMCConfig
from mcosint.storage.json_store import write_json
from mcosint.util.uuid_tools import maybe_normalize_uuid_str

log = logging.getLogger(__name__)


def _bool_from_env(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    v = value.strip().lower()
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off"}:
        return False
    return default


def _int_or_none_from_env(value: str | None) -> int | None:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    i = int(s)
    # Common convention: <=0 means unlimited.
    if i <= 0:
        return None
    return i


@dataclass(frozen=True)
class BurstConfig:
    min_api_wait_time: float
    max_api_wait_time: float
    min_burst_calls: int
    max_burst_calls: int


class BurstRateLimiter:
    """Global burst-and-sleep limiter shared by worker threads.

    Threads call `before_call()` right before issuing an HTTP request.
    The limiter enforces:
      - A randomized burst size in [min_burst_calls, max_burst_calls]
      - After `burst_size` calls, a randomized sleep in [min_wait, max_wait]
    """

    def __init__(
        self,
        cfg: BurstConfig,
        *,
        per_call_min_seconds: float = 0.3,
        per_call_max_seconds: float = 1.0,
    ) -> None:
        if cfg.min_burst_calls < 1 or cfg.max_burst_calls < 1:
            raise ValueError("Burst calls must be >= 1")
        if cfg.min_burst_calls > cfg.max_burst_calls:
            raise ValueError("MIN_BURST_CALLS must be <= MAX_BURST_CALLS")
        if cfg.min_api_wait_time < 0 or cfg.max_api_wait_time < 0:
            raise ValueError("Wait times must be >= 0")
        if cfg.min_api_wait_time > cfg.max_api_wait_time:
            raise ValueError("MIN_API_WAIT_TIME must be <= MAX_API_WAIT_TIME")

        if per_call_min_seconds < 0 or per_call_max_seconds < 0:
            raise ValueError("per-call wait must be >= 0")
        if per_call_min_seconds > per_call_max_seconds:
            raise ValueError("PER_CALL_MIN_WAIT_SECONDS must be <= PER_CALL_MAX_WAIT_SECONDS")

        self._cfg = cfg
        self._per_call_min = per_call_min_seconds
        self._per_call_max = per_call_max_seconds
        self._lock = threading.Lock()
        self._cooldown_until = 0.0  # monotonic seconds

        self._burst_target = random.randint(cfg.min_burst_calls, cfg.max_burst_calls)
        self._burst_used = 0

    def _sleep_if_needed_locked(self) -> None:
        now = time.monotonic()
        if now >= self._cooldown_until:
            return
        remaining = self._cooldown_until - now
        # Sleep while holding the lock: this intentionally blocks other threads so
        # the cooldown is global and consistent.
        time.sleep(remaining)

    def before_call(self) -> None:
        with self._lock:
            self._sleep_if_needed_locked()

            # Always jitter between calls.
            time.sleep(random.uniform(self._per_call_min, self._per_call_max))

            self._burst_used += 1
            if self._burst_used < self._burst_target:
                return

            # Burst reached: set a global cooldown for the *next* call.
            wait = random.uniform(self._cfg.min_api_wait_time, self._cfg.max_api_wait_time)
            self._cooldown_until = time.monotonic() + wait
            log.info("Burst complete (%s calls). Sleeping for %.2fs...", self._burst_used, wait)

            # Reset burst window for the next cycle.
            self._burst_used = 0
            self._burst_target = random.randint(self._cfg.min_burst_calls, self._cfg.max_burst_calls)


@dataclass(frozen=True)
class WorkerConfig:
    uuid_list_file_path: Path

    max_depth: int
    max_total_calls: int | None
    max_friends_per_user: int | None
    output_path: Path

    use_flaresolverr: bool
    flaresolverr_url: str | None

    init_db: bool
    threads: int
    rate_limit_backoff_seconds: float
    max_retries_per_uuid: int

    discord_webhook_url: str | None

    burst: BurstConfig

    # Per-worker IP isolation (optional). If both VPN and proxy are set,
    # VPN wins. If neither is set, all workers share the host's outbound IP
    # via the single fallback ThreadedNameMCClient (legacy behavior).
    proxy_file_path: Path | None = None
    vpn_dir: Path | None = None
    vpn_count: int | None = None


def load_worker_config_from_env() -> WorkerConfig:
    # Load .env (and allow overriding by already-set env vars)
    # Use an explicit path to avoid python-dotenv's stack inspection edge cases.
    load_dotenv(dotenv_path=Path.cwd() / ".env", override=True)

    uuid_list_file = os.getenv("UUID_LIST_FILE_PATH")
    if not uuid_list_file:
        raise RuntimeError("UUID_LIST_FILE_PATH is required")

    max_depth = int(os.getenv("MAX_DEPTH", "2"))
    max_total_calls = _int_or_none_from_env(os.getenv("MAX_TOTAL_CALLS"))
    max_friends_per_user = _int_or_none_from_env(os.getenv("MAX_FRIENDS_PER_USER"))

    output_path = Path(os.getenv("OUTPUT_PATH", "output/friend_mesh.json"))

    use_fs = _bool_from_env(
        os.getenv("USE_FLARESOLVERR")
        if os.getenv("USE_FLARESOLVERR") is not None
        else os.getenv("MCOSINT_USE_FLARESOLVERR"),
        default=True,
    )
    fs_url = os.getenv("FLARESOLVERR_URL")
    init_db = _bool_from_env(os.getenv("INIT_DB"), default=False)

    threads = int(os.getenv("THREADS", os.getenv("THREAD_COUNT", "5")))
    rate_limit_backoff_seconds = float(os.getenv("RATE_LIMIT_BACKOFF_SECONDS", "10"))
    max_retries_per_uuid = int(os.getenv("MAX_RETRIES_PER_UUID", "3"))

    webhook = os.getenv("DISCORD_WEBHOOK_URL") or os.getenv("MCOSINT_DISCORD_WEBHOOK_URL")

    # Burst settings. Defaults match .env.example so running without an .env file
    # still gives the documented operational shape (3-8 calls per burst, 15-45s cool-down)
    # rather than the historical aggressive defaults (1 call per burst, 0.3-1.0s).
    burst = BurstConfig(
        min_api_wait_time=float(os.getenv("MIN_API_WAIT_TIME", "15.0")),
        max_api_wait_time=float(os.getenv("MAX_API_WAIT_TIME", "45.0")),
        min_burst_calls=int(os.getenv("MIN_BURST_CALLS", "3")),
        max_burst_calls=int(os.getenv("MAX_BURST_CALLS", "8")),
    )

    proxy_file_env = os.getenv("PROXY_FILE_PATH")
    proxy_file_path = Path(proxy_file_env) if proxy_file_env else None
    vpn_dir_env = os.getenv("VPN_DIR")
    vpn_dir = Path(vpn_dir_env) if vpn_dir_env else None
    vpn_count = _int_or_none_from_env(os.getenv("VPN_COUNT"))

    return WorkerConfig(
        uuid_list_file_path=Path(uuid_list_file),
        max_depth=max_depth,
        max_total_calls=max_total_calls,
        max_friends_per_user=max_friends_per_user,
        output_path=output_path,
        use_flaresolverr=use_fs,
        flaresolverr_url=fs_url,
        init_db=init_db,
        threads=threads,
        rate_limit_backoff_seconds=rate_limit_backoff_seconds,
        max_retries_per_uuid=max_retries_per_uuid,
        discord_webhook_url=webhook,
        burst=burst,
        proxy_file_path=proxy_file_path,
        vpn_dir=vpn_dir,
        vpn_count=vpn_count,
    )


def _build_proxy_pool(cfg: WorkerConfig):
    """Build a ProxyProvider + (optional) VPNTunnelManager from worker config.

    Returns `(proxy_pool, vpn_manager)`. Either may be None.
    VPN_DIR takes precedence over PROXY_FILE_PATH if both are set.
    """
    if cfg.vpn_dir is not None:
        from mcosint.proxy.vpn import VPNProxyProvider, VPNTunnelManager

        vpn_manager = VPNTunnelManager(cfg.vpn_dir)
        n_tunnels = cfg.vpn_count if cfg.vpn_count is not None else cfg.threads
        tunnels = vpn_manager.start_all(n_tunnels)
        running = sum(1 for t in tunnels if t.state == "running")
        log.info("Started %d/%d VPN tunnel(s).", running, n_tunnels)
        return VPNProxyProvider(vpn_manager), vpn_manager

    if cfg.proxy_file_path is not None:
        from mcosint.proxy.pool import StaticProxyPool

        pool = StaticProxyPool.from_file(cfg.proxy_file_path)
        if pool.all_proxies():
            log.info(
                "Loaded %d proxy/proxies from %s; round-robin assigned to %d workers.",
                len(pool.all_proxies()),
                cfg.proxy_file_path,
                cfg.threads,
            )
            return pool, None

    return None, None


def load_seed_uuids(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(str(path))

    seeds: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        u = maybe_normalize_uuid_str(s)
        if u is None:
            log.warning("Skipping invalid UUID in seed file: %r", s)
            continue
        seeds.append(u)

    # De-dupe while preserving order
    seeds = list(dict.fromkeys(seeds))
    return seeds


def run() -> None:
    cfg = load_worker_config_from_env()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is required")

    seeds = load_seed_uuids(cfg.uuid_list_file_path)
    if not seeds:
        raise RuntimeError("No valid UUIDs found in UUID_LIST_FILE_PATH")

    pool = create_pool(
        DbPoolConfig(
            database_url=db_url,
            min_size=int(os.getenv("DB_POOL_MIN_SIZE", "1")),
            max_size=int(os.getenv("DB_POOL_MAX_SIZE", str(max(cfg.threads + 2, 10)))),
        )
    )

    # Fail fast unless the schema exists. INIT_DB=true makes it auto-create.
    with pool.connection() as conn:
        if cfg.init_db:
            create_schema(conn)
        else:
            try:
                conn.execute("SELECT 1 FROM players LIMIT 1")
            except psycopg.errors.UndefinedTable as e:
                raise RuntimeError(
                    "Database schema is missing. Run: mcosint db init (or set INIT_DB=true)."
                ) from e

    app_cfg = load_config()

    # If USE_FLARESOLVERR is enabled but FLARESOLVERR_URL isn't provided, fall back to
    # existing config/env overrides via load_config().
    fs_url = cfg.flaresolverr_url or os.getenv("MCOSINT_FLARESOLVERR_URL") or app_cfg.flaresolverr.url

    notifier = None
    if cfg.discord_webhook_url:
        notifier = DiscordWebhook(cfg.discord_webhook_url)

    # Default webhook behavior requested: if url present in env, it's enabled by default.
    if notifier is not None:
        notifier.send(
            embed={
                "title": "Mesh Worker Started",
                "description": f"Seeds: {len(seeds)}\nmax_depth={cfg.max_depth} threads={cfg.threads}",
                "color": 3447003,
            }
        )

    burst_limiter = BurstRateLimiter(
        cfg.burst,
        per_call_min_seconds=float(os.getenv("PER_CALL_MIN_WAIT_SECONDS", "0.3")),
        per_call_max_seconds=float(os.getenv("PER_CALL_MAX_WAIT_SECONDS", "1.0")),
    )

    # Build the per-worker NameMC config template. When `proxy_pool` is set,
    # build_friend_mesh_threaded uses this to construct one isolated client per
    # worker (each gets its own outbound IP). When no proxy pool, the shared
    # `client` below is used by all workers (legacy single-IP behavior).
    namemc_cfg = ThreadedNameMCConfig(
        user_agent=app_cfg.http.user_agent,
        timeout_seconds=app_cfg.http.timeout_seconds,
        flaresolverr_url=fs_url,
        flaresolverr_max_timeout_ms=app_cfg.flaresolverr.max_timeout_ms,
    )

    proxy_pool, vpn_manager = _build_proxy_pool(cfg)

    client = ThreadedNameMCClient(namemc_cfg)

    try:
        mesh = build_friend_mesh_threaded(
            namemc=client,
            start_uuids=seeds,
            max_depth=cfg.max_depth,
            max_total_calls=cfg.max_total_calls,
            max_friends_per_user=cfg.max_friends_per_user,
            use_flaresolverr=cfg.use_flaresolverr,
            pool=pool,
            threads=cfg.threads,
            rate_limit_backoff_seconds=cfg.rate_limit_backoff_seconds,
            max_retries_per_uuid=cfg.max_retries_per_uuid,
            notifier=notifier,
            burst_limiter=burst_limiter,
            proxy_pool=proxy_pool,
            namemc_cfg=namemc_cfg if proxy_pool is not None else None,
        )
    finally:
        client.close()
        if vpn_manager is not None:
            vpn_manager.stop_all()

    cfg.output_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(cfg.output_path, mesh, indent=2)
    log.info("Saved: %s", cfg.output_path)

    if notifier is not None:
        notifier.send(
            embed={
                "title": "Mesh Worker Finished",
                "description": f"Nodes: {len(mesh)}\nSaved: `{cfg.output_path}`",
                "color": 10181046,
            }
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
