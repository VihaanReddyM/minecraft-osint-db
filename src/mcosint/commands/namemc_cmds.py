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
