from __future__ import annotations

import asyncio
from pathlib import Path

import typer

from mcosint.config import load_config
from mcosint.graph.friend_mesh import build_friend_mesh
from mcosint.http.async_client import AsyncFetcher, FetcherConfig
from mcosint.services.namemc import NameMCClient
from mcosint.storage.json_store import write_json

namemc_app = typer.Typer(add_completion=False, help="NameMC-related commands")


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
) -> None:
    """Multi-threaded crawl that persists results to PostgreSQL in real time."""

    import os

    from mcosint.crawl.namemc_friends import CrawlConfig, crawl_namemc_friends_to_db
    from mcosint.db.connection import DbPoolConfig, get_pool
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

    # De-dupe while preserving order
    seeds = list(dict.fromkeys(seeds))

    if not seeds:
        raise typer.BadParameter("Provide START_UUID or --uuids-file")

    cfg = CrawlConfig(
        thread_count=threads,
        request_delay_seconds=request_delay,
        max_depth=max_depth,
        max_total_requests=max_total_requests,
        max_friends_per_user=max_friends_per_user,
        rate_limit_backoff_seconds=rate_limit_backoff_seconds,
        max_retries_per_uuid=max_retries_per_uuid,
        force_recrawl=force_recrawl,
    )

    # Pool size should at least cover worker threads.
    pool = get_pool(
        DbPoolConfig(
            database_url=db_url,
            min_size=1,
            max_size=max(threads + 2, int(os.getenv("DB_POOL_MAX_SIZE", str(threads + 2)))),
        )
    )

    if init_db:
        with pool.connection() as conn:
            create_schema(conn)

    metrics = crawl_namemc_friends_to_db(pool=pool, start_uuids=seeds, cfg=cfg)
    typer.echo(f"Done. metrics={metrics}")


@namemc_app.command("friends-mesh")
def namemc_friends_mesh(
    start_uuid: str = typer.Argument(..., help="Starting UUID"),
    max_depth: int = typer.Option(2, "--max-depth"),
    delay_seconds: float = typer.Option(1.0, "--delay-seconds"),
    max_total_calls: int = typer.Option(10, "--max-total-calls"),
    max_friends_per_user: int = typer.Option(3, "--max-friends-per-user"),
    out: Path = typer.Option(Path("output/friend_mesh.json"), "--out"),
    use_flaresolverr: bool = typer.Option(
        False,
        "--use-flaresolverr",
        help="Route requests through FlareSolverr (requires it running)",
    ),
    flaresolverr_url: str | None = typer.Option(
        None,
        "--flaresolverr-url",
        help="Override FlareSolverr base URL (default from config/env)",
    ),
) -> None:
    cfg = load_config()

    fs_url = flaresolverr_url or cfg.flaresolverr.url
    fetch_cfg = FetcherConfig(
        timeout_seconds=cfg.http.timeout_seconds,
        user_agent=cfg.http.user_agent,
        flaresolverr_url=fs_url,
        flaresolverr_max_timeout_ms=cfg.flaresolverr.max_timeout_ms,
    )

    async def _run() -> None:
        async with AsyncFetcher(fetch_cfg) as fetcher:
            namemc = NameMCClient(fetcher)
            mesh = await build_friend_mesh(
                namemc=namemc,
                start_uuid=start_uuid,
                max_depth=max_depth,
                delay_seconds=delay_seconds,
                max_total_calls=max_total_calls,
                max_friends_per_user=max_friends_per_user,
                use_flaresolverr=use_flaresolverr,
            )
            write_json(out, mesh, indent=2)

    asyncio.run(_run())
    typer.echo(f"Saved: {out}")
