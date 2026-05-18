from __future__ import annotations

import asyncio

import typer

from mcosint.config import load_config
from mcosint.http.flaresolverr import FlareSolverrClient

flaresolverr_app = typer.Typer(add_completion=False, help="FlareSolverr helpers")


@flaresolverr_app.command("health")
def flaresolverr_health(
    flaresolverr_url: str | None = typer.Option(
        None,
        "--flaresolverr-url",
        help="Override FlareSolverr base URL (default from config/env)",
    ),
) -> None:
    cfg = load_config()
    url = flaresolverr_url or cfg.flaresolverr.url

    async def _run() -> bool:
        async with FlareSolverrClient(base_url=url, max_timeout_ms=cfg.flaresolverr.max_timeout_ms) as fs:
            return await fs.health()

    ok = asyncio.run(_run())
    typer.echo("OK" if ok else "UNHEALTHY")
    raise typer.Exit(code=0 if ok else 1)
