from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated

import typer

from mcosint.config import load_config


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


def _build_metrics_recorder(metrics_port: int):
    """Build a MetricsRecorder. If metrics_port > 0, attempt Prometheus
    (with `/metrics` HTTP server); fall back to InProcessMetricsRecorder if
    `prometheus_client` isn't installed."""
    from mcosint.metrics.recorder import InProcessMetricsRecorder

    if metrics_port <= 0:
        return InProcessMetricsRecorder()

    from mcosint.metrics.prometheus import (
        start_metrics_http_server,
        try_make_prometheus_recorder,
    )

    rec = try_make_prometheus_recorder()
    if rec is None:
        typer.echo(
            "warning: --metrics-port requested but prometheus_client is not installed. "
            "Falling back to in-process metrics. "
            "Install via: pip install 'mcosint[metrics]'",
            err=True,
        )
        return InProcessMetricsRecorder()

    start_metrics_http_server(metrics_port, rec)
    typer.echo(f"Prometheus /metrics exposed on :{metrics_port}")
    return rec


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
    metrics_port: int = typer.Option(
        0,
        "--metrics-port",
        help=(
            "Expose Prometheus /metrics on this port (0 = disabled). "
            "Requires `pip install 'mcosint[metrics]'`."
        ),
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

    engine_metrics = _build_metrics_recorder(metrics_port)

    engine = CrawlEngine(
        db_pool=pool,
        config=cfg,
        proxy_pool=proxy_pool,
        metrics=engine_metrics,
    )
    try:
        metrics = engine.run(seeds)
        typer.echo(f"Done. {metrics.to_dict()}")
        if isinstance(engine_metrics, InProcessMetricsRecorder):
            snap = engine_metrics.snapshot()
            if snap.get("requests_ok") or snap.get("requests_err"):
                typer.echo(f"metrics={snap}")
    finally:
        if vpn_manager is not None:
            vpn_manager.stop_all()


@namemc_app.command("friends-mesh")
def namemc_friends_mesh() -> None:
    """Run the env-driven friends mesh worker.

    Configuration is loaded from `.env`/environment variables; see `.env.example`.
    """
    from mcosint.workers.namemc_friends_mesh_worker import run

    run()
