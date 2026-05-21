from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated

import psycopg
import typer

from mcosint.config import load_config
from mcosint.storage.json_store import write_json

namemc_app = typer.Typer(add_completion=False, help="NameMC-related commands")


def _load_proxy_pool(
    proxy: list[str],
    proxy_file: Path | None,
) -> object:
    """Build a StaticProxyPool from CLI --proxy flags and/or --proxy-file."""
    from mcosint.proxy.pool import StaticProxyPool

    urls: list[str] = list(proxy)
    if proxy_file:
        file_pool = StaticProxyPool.from_file(proxy_file)
        urls.extend(p.httpx_url() for p in file_pool.all_proxies())

    return StaticProxyPool.from_urls(urls) if urls else None


@namemc_app.command("crawl-friends-db")
def namemc_crawl_friends_db(
    start_uuid: str | None = typer.Argument(None, help="Starting UUID (optional if using --uuids-file)"),
    uuids_file: Path | None = typer.Option(
        None,
        "--uuids-file",
        help="Path to a .txt file with one UUID per line (comments with # supported)",
    ),
    threads: int = typer.Option(5, "--threads", help="ThreadPoolExecutor worker count"),
    request_delay: float = typer.Option(1.0, "--request-delay", help="Delay after each successful request"),
    max_depth: int = typer.Option(2, "--max-depth"),
    max_total_requests: int = typer.Option(100, "--max-total-requests"),
    max_friends_per_user: int = typer.Option(50, "--max-friends-per-user"),
    rate_limit_backoff_seconds: float = typer.Option(10.0, "--rate-limit-backoff-seconds"),
    max_retries_per_uuid: int = typer.Option(3, "--max-retries-per-uuid"),
    force_recrawl: bool = typer.Option(False, "--force-recrawl", help="Ignore resume and recrawl even if already crawled"),
    init_db: bool = typer.Option(False, "--init-db", help="Ensure schema before crawling"),
    proxy: Annotated[
        list[str],
        typer.Option(
            "--proxy",
            help="Proxy URL (e.g. socks5://127.0.0.1:1080). Repeatable. Workers round-robin assigned.",
        ),
    ] = [],
    proxy_file: Path | None = typer.Option(
        None,
        "--proxy-file",
        help="Text file with one proxy URL per line (# comments supported). Merged with --proxy.",
    ),
    vpn_dir: Path | None = typer.Option(None, "--vpn-dir", help="Directory of .ovpn files; starts one tunnel per worker"),
    vpn_count: int | None = typer.Option(None, "--vpn-count", help="Tunnel count (default: --threads)"),
    use_flaresolverr: bool = typer.Option(
        False,
        "--use-flaresolverr/--no-use-flaresolverr",
        help="Route requests through FlareSolverr (applied to workers without a --proxy assignment)",
    ),
    flaresolverr_url: str | None = typer.Option(
        None,
        "--flaresolverr-url",
        help="Override FlareSolverr base URL (default from config/env)",
    ),
) -> None:
    """Multi-threaded crawl that persists results to PostgreSQL in real time."""

    from mcosint.crawl.namemc_friends import CrawlConfig
    from mcosint.db.connection import DbPoolConfig, create_pool
    from mcosint.db.schema import create_schema

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise typer.BadParameter(
            "DATABASE_URL is not set. Set it in the environment (see .env.example)."
        )

    seeds: list[str] = []
    if start_uuid:
        seeds.append(start_uuid)

    if uuids_file:
        lines = uuids_file.read_text(encoding="utf-8").splitlines()
        for line in lines:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            seeds.append(s)

    seeds = list(dict.fromkeys(seeds))

    if not seeds:
        raise typer.BadParameter("Provide START_UUID or --uuids-file")

    vpn_manager = None
    if vpn_dir:
        from mcosint.proxy.vpn import VPNProxyProvider, VPNTunnelManager
        vpn_manager = VPNTunnelManager(vpn_dir)
        n_tunnels = vpn_count if vpn_count is not None else threads
        tunnels = vpn_manager.start_all(n_tunnels)
        running = sum(1 for t in tunnels if t.state == "running")
        typer.echo(f"Started {running}/{n_tunnels} VPN tunnel(s).")
        proxy_pool = VPNProxyProvider(vpn_manager)
    else:
        proxy_pool = _load_proxy_pool(proxy, proxy_file)
        if proxy_pool:
            n = len(proxy_pool.all_proxies())
            typer.echo(f"Loaded {n} proxy/proxies; assigning round-robin to {threads} workers.")

    cfg_obj = load_config()
    fs_url = None
    if use_flaresolverr:
        fs_url = flaresolverr_url or cfg_obj.flaresolverr.url

    cfg = CrawlConfig(
        thread_count=threads,
        request_delay_seconds=request_delay,
        max_depth=max_depth,
        max_total_requests=max_total_requests if max_total_requests else None,
        max_friends_per_user=max_friends_per_user,
        rate_limit_backoff_seconds=rate_limit_backoff_seconds,
        max_retries_per_uuid=max_retries_per_uuid,
        force_recrawl=force_recrawl,
        flaresolverr_url=fs_url,
        flaresolverr_max_timeout_ms=cfg_obj.flaresolverr.max_timeout_ms,
        timeout_seconds=cfg_obj.http.timeout_seconds,
        user_agent=cfg_obj.http.user_agent,
    )

    pool = create_pool(
        DbPoolConfig(
            database_url=db_url,
            min_size=1,
            max_size=max(threads + 2, int(os.getenv("DB_POOL_MAX_SIZE", str(threads + 2)))),
        )
    )

    if init_db:
        with pool.connection() as conn:
            create_schema(conn)

    from mcosint.crawl.engine import CrawlEngine
    from mcosint.metrics.recorder import InProcessMetricsRecorder

    engine_metrics = InProcessMetricsRecorder()
    engine = CrawlEngine(
        db_pool=pool,
        config=cfg,
        proxy_pool=proxy_pool,
        metrics=engine_metrics,
    )
    try:
        metrics = engine.run(seeds)
        typer.echo(f"Done. {metrics.to_dict()}")
        snap = engine_metrics.snapshot()
        if snap.get("requests_ok") or snap.get("requests_err"):
            typer.echo(f"metrics={snap}")
    finally:
        if vpn_manager is not None:
            vpn_manager.stop_all()


@namemc_app.command("friends-mesh")
def namemc_friends_mesh(
    start_uuid: str = typer.Argument(..., help="Starting UUID"),
    max_depth: int = typer.Option(2, "--max-depth"),
    delay_seconds: float = typer.Option(1.0, "--delay-seconds"),
    max_total_calls: int | None = typer.Option(
        None,
        "--max-total-calls",
        help="Optional safety cap for total API calls (default: unlimited)",
    ),
    max_friends_per_user: int | None = typer.Option(
        None,
        "--max-friends-per-user",
        help="Optional cap on how many friends to expand per user (default: unlimited)",
    ),
    out: Path = typer.Option(Path("output/friend_mesh.json"), "--out"),
    use_flaresolverr: bool = typer.Option(
        True,
        "--use-flaresolverr/--no-use-flaresolverr",
        help="Route requests through FlareSolverr when no --proxy is assigned (default: enabled)",
    ),
    flaresolverr_url: str | None = typer.Option(
        None,
        "--flaresolverr-url",
        help="Override FlareSolverr base URL (default from config/env)",
    ),
    init_db: bool = typer.Option(False, "--init-db", help="Ensure schema before crawling"),
    threads: int | None = typer.Option(
        None,
        "--threads",
        help="Worker thread count (default: THREAD_COUNT env var, or 5)",
    ),
    rate_limit_backoff_seconds: float | None = typer.Option(
        None,
        "--rate-limit-backoff-seconds",
        help="Backoff when rate limited (default: RATE_LIMIT_BACKOFF_SECONDS env var, or 10)",
    ),
    max_retries_per_uuid: int | None = typer.Option(
        None,
        "--max-retries-per-uuid",
        help="Max retries per UUID (default: MAX_RETRIES_PER_UUID env var, or 3)",
    ),
    discord_webhook_url: str | None = typer.Option(
        None,
        "--discord-webhook-url",
        help="Override Discord webhook URL (default from env MCOSINT_DISCORD_WEBHOOK_URL)",
    ),
    proxy: Annotated[
        list[str],
        typer.Option(
            "--proxy",
            help="Proxy URL (e.g. socks5://127.0.0.1:1080). Repeatable. Workers round-robin assigned.",
        ),
    ] = [],
    proxy_file: Path | None = typer.Option(
        None,
        "--proxy-file",
        help="Text file with one proxy URL per line (# comments supported). Merged with --proxy.",
    ),
    vpn_dir: Path | None = typer.Option(None, "--vpn-dir", help="Directory of .ovpn files; starts one tunnel per worker"),
    vpn_count: int | None = typer.Option(None, "--vpn-count", help="Tunnel count (default: --threads)"),
) -> None:
    # Resolve env-backed defaults here (after load_dotenv has run in the root callback).
    resolved_threads = threads if threads is not None else int(os.getenv("THREAD_COUNT", "5"))
    resolved_backoff = (
        rate_limit_backoff_seconds
        if rate_limit_backoff_seconds is not None
        else float(os.getenv("RATE_LIMIT_BACKOFF_SECONDS", "10"))
    )
    resolved_retries = (
        max_retries_per_uuid
        if max_retries_per_uuid is not None
        else int(os.getenv("MAX_RETRIES_PER_UUID", "3"))
    )

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise typer.BadParameter(
            "DATABASE_URL is not set. friends-mesh persists to DB; set DATABASE_URL and run: mcosint db init"
        )

    from mcosint.crawl.namemc_friends import CrawlConfig
    from mcosint.db.connection import DbPoolConfig, create_pool
    from mcosint.db.schema import create_schema
    from mcosint.notify.discord import DiscordWebhook

    vpn_manager = None
    if vpn_dir:
        from mcosint.proxy.vpn import VPNProxyProvider, VPNTunnelManager
        vpn_manager = VPNTunnelManager(vpn_dir)
        n_tunnels = vpn_count if vpn_count is not None else resolved_threads
        tunnels = vpn_manager.start_all(n_tunnels)
        running = sum(1 for t in tunnels if t.state == "running")
        typer.echo(f"Started {running}/{n_tunnels} VPN tunnel(s).")
        proxy_pool = VPNProxyProvider(vpn_manager)
    else:
        proxy_pool = _load_proxy_pool(proxy, proxy_file)
        if proxy_pool:
            n = len(proxy_pool.all_proxies())
            typer.echo(f"Loaded {n} proxy/proxies; assigning round-robin to {resolved_threads} workers.")

    pool = create_pool(DbPoolConfig(database_url=db_url, min_size=1, max_size=10))
    with pool.connection() as conn:
        if init_db:
            create_schema(conn)
        else:
            try:
                conn.execute("SELECT 1 FROM players LIMIT 1")
            except psycopg.errors.UndefinedTable as e:
                raise typer.BadParameter(
                    "Database schema is missing. Run: mcosint db init (or re-run with --init-db)."
                ) from e

    cfg = load_config()
    fs_url = flaresolverr_url or cfg.flaresolverr.url

    webhook = discord_webhook_url or os.getenv("MCOSINT_DISCORD_WEBHOOK_URL")
    notifier = DiscordWebhook(webhook or "")
    notifier.send(
        embed={
            "title": "Mesh Build Started",
            "description": (
                f"Target: `{start_uuid}`\nmax_depth={max_depth} threads={resolved_threads}"
                + (f"\nproxies={len(proxy_pool.all_proxies())}" if proxy_pool else "")
            ),
            "color": 3447003,
        }
    )

    crawl_cfg = CrawlConfig(
        thread_count=resolved_threads,
        request_delay_seconds=delay_seconds,
        max_depth=max_depth,
        max_total_requests=max_total_calls,
        max_friends_per_user=max_friends_per_user,
        rate_limit_backoff_seconds=resolved_backoff,
        max_retries_per_uuid=resolved_retries,
        flaresolverr_url=fs_url if use_flaresolverr else None,
        flaresolverr_max_timeout_ms=cfg.flaresolverr.max_timeout_ms,
        timeout_seconds=cfg.http.timeout_seconds,
        user_agent=cfg.http.user_agent,
    )

    from mcosint.crawl.engine import CrawlEngine, build_mesh_from_db
    from mcosint.metrics.recorder import InProcessMetricsRecorder

    engine_metrics = InProcessMetricsRecorder()
    engine = CrawlEngine(
        db_pool=pool,
        config=crawl_cfg,
        proxy_pool=proxy_pool,
        metrics=engine_metrics,
        notifier=notifier if (webhook or discord_webhook_url) else None,
    )

    try:
        crawl_result = engine.run([start_uuid])
        typer.echo(f"Crawl done. {crawl_result.to_dict()}")
    finally:
        if vpn_manager is not None:
            vpn_manager.stop_all()

    with pool.connection() as conn:
        mesh = build_mesh_from_db(
            conn,
            start_uuid,
            max_depth=max_depth,
            max_friends_per_user=max_friends_per_user,
        )

    write_json(out, mesh, indent=2)
    typer.echo(f"Saved: {out}")

    notifier.send(
        embed={
            "title": "Mesh Build Finished",
            "description": f"Nodes: {len(mesh)}\nSaved: `{out}`",
            "color": 10181046,
        }
    )
