from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Annotated

import typer

from mcosint.config import load_config
from mcosint.http.async_client import AsyncFetcher, FetcherConfig
from mcosint.storage.json_store import write_json

http_app = typer.Typer(add_completion=False, help="Generic HTTP helpers")


@http_app.command("get")
def http_get(  # noqa: B008
    url: Annotated[str, typer.Argument(help="URL to fetch")],
    out: Annotated[Path | None, typer.Option("--out", help="Write response body to a file")] = None,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Parse response as JSON and pretty-print"),
    ] = False,
    use_flaresolverr: Annotated[
        bool,
        typer.Option(
            "--use-flaresolverr",
            help="Route request through FlareSolverr (requires it running)",
        ),
    ] = False,
    flaresolverr_url: Annotated[
        str | None,
        typer.Option(
            "--flaresolverr-url",
            help="Override FlareSolverr base URL (default from config/env)",
        ),
    ] = None,
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
            if as_json:
                data = await fetcher.get_json(url, use_flaresolverr=use_flaresolverr)
                if out:
                    write_json(out, data, indent=2)
                else:
                    typer.echo(typer.style("(no --out provided; printing JSON)", dim=True))
                    typer.echo(json.dumps(data, indent=2))
            else:
                text = await fetcher.get_text(url, use_flaresolverr=use_flaresolverr)
                if out:
                    out.parent.mkdir(parents=True, exist_ok=True)
                    out.write_text(text, encoding="utf-8")
                else:
                    typer.echo(text)

    asyncio.run(_run())

    if out:
        typer.echo(f"Saved: {out}")


@http_app.command("batch-get")
def http_batch_get(
    urls_file: Annotated[Path, typer.Argument(help="Text file containing one URL per line")],
    out_dir: Annotated[Path, typer.Option("--out-dir", help="Directory to write responses into")] = Path(
        "output/http_batch"
    ),
    concurrency: Annotated[int, typer.Option("--concurrency", help="Max in-flight requests")] = 20,
    use_flaresolverr: Annotated[
        bool,
        typer.Option(
            "--use-flaresolverr",
            help="Route requests through FlareSolverr (requires it running)",
        ),
    ] = False,
) -> None:
    """Fetch many URLs concurrently.

    This is a starter workflow for doing lots of API calls. It writes each body to a file and
    produces an `index.json` with success/error metadata.
    """

    cfg = load_config()

    urls = [
        line.strip()
        for line in urls_file.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]

    fetch_cfg = FetcherConfig(
        timeout_seconds=cfg.http.timeout_seconds,
        user_agent=cfg.http.user_agent,
        flaresolverr_url=cfg.flaresolverr.url,
        flaresolverr_max_timeout_ms=cfg.flaresolverr.max_timeout_ms,
    )

    async def _run() -> dict[str, dict[str, str]]:
        import hashlib

        out_dir.mkdir(parents=True, exist_ok=True)
        sem = asyncio.Semaphore(max(1, concurrency))

        results: dict[str, dict[str, str]] = {}

        async with AsyncFetcher(fetch_cfg) as fetcher:

            async def one(url: str) -> None:
                async with sem:
                    try:
                        body = await fetcher.get_text(url, use_flaresolverr=use_flaresolverr)
                        name = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16] + ".txt"
                        path = out_dir / name
                        path.write_text(body, encoding="utf-8")
                        results[url] = {"status": "ok", "file": str(path)}
                    except Exception as e:
                        results[url] = {"status": "error", "error": str(e)}

            await asyncio.gather(*(one(u) for u in urls))

        return results

    index = asyncio.run(_run())
    index_path = out_dir / "index.json"
    write_json(index_path, index, indent=2)
    typer.echo(f"Wrote index: {index_path}")
