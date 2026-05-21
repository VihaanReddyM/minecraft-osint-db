from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Annotated

import typer

from mcosint.db.connection import DbPoolConfig, create_pool

worker_app = typer.Typer(add_completion=False, help="Standalone worker node commands")


# ── helpers ───────────────────────────────────────────────────────────────────


def _load_proxy_urls(proxy: list[str], proxy_file: Path | None) -> list[str]:
    """Merge --proxy flags and --proxy-file into a flat list of URL strings."""
    from mcosint.proxy.pool import StaticProxyPool

    urls: list[str] = list(proxy)
    if proxy_file:
        pool = StaticProxyPool.from_file(proxy_file)
        urls.extend(p.httpx_url() for p in pool.all_proxies())
    return urls


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


# ── worker start ──────────────────────────────────────────────────────────────


@worker_app.command("start")
def worker_start(
    concurrency: int = typer.Option(5, "--concurrency", help="Number of worker threads"),
    max_depth: int = typer.Option(3, "--max-depth", help="BFS depth limit"),
    db_url: str | None = typer.Option(
        None,
        "--db-url",
        help="PostgreSQL connection URL (default: DATABASE_URL env var)",
    ),
    proxy: Annotated[
        list[str],
        typer.Option(
            "--proxy",
            help="socks5://... proxy URL. Repeatable. Workers round-robin assigned.",
        ),
    ] = [],
    proxy_file: Path | None = typer.Option(
        None,
        "--proxy-file",
        help="Text file with one proxy URL per line (# comments supported).",
    ),
    node_id: str | None = typer.Option(
        None,
        "--node-id",
        help="Unique name for this node (default: hostname)",
    ),
    heartbeat_interval: float = typer.Option(
        30.0,
        "--heartbeat-interval",
        help="Seconds between heartbeats",
    ),
    request_delay: float = typer.Option(
        1.0,
        "--request-delay",
        help="Seconds between requests per worker",
    ),
    max_total_requests: int = typer.Option(
        0,
        "--max-total-requests",
        help="Total request cap (0 = unlimited)",
    ),
    init_db: bool = typer.Option(False, "--init-db", help="Run schema init before starting"),
    metrics_port: int = typer.Option(
        0,
        "--metrics-port",
        help=(
            "Expose Prometheus /metrics on this port (0 = disabled). "
            "Requires `pip install 'mcosint[metrics]'`."
        ),
    ),
) -> None:
    """Drain the crawl_queue as a standalone worker node.

    Seeds must be pre-loaded via `mcosint worker seeds`. This command just
    consumes whatever is already in crawl_queue.
    """
    from mcosint.crawl.engine import CrawlEngine
    from mcosint.crawl.namemc_friends import CrawlConfig
    from mcosint.crawl.task_queue import PostgresTaskQueue
    from mcosint.db.schema import create_schema
    from mcosint.proxy.pool import StaticProxyPool

    resolved_db_url = db_url or os.getenv("DATABASE_URL")
    if not resolved_db_url:
        raise typer.BadParameter(
            "DATABASE_URL is not set. Provide --db-url or set DATABASE_URL in the environment."
        )

    resolved_node_id = node_id or socket.gethostname()
    resolved_host = socket.gethostname()

    proxy_urls = _load_proxy_urls(proxy, proxy_file)
    proxy_pool = StaticProxyPool.from_urls(proxy_urls) if proxy_urls else None
    proxy_count = len(proxy_urls)

    pool = create_pool(
        DbPoolConfig(
            database_url=resolved_db_url,
            min_size=1,
            max_size=max(concurrency + 2, 5),
        )
    )

    if init_db:
        with pool.connection() as conn:
            create_schema(conn)

    task_queue = PostgresTaskQueue(pool)

    # Register node and start heartbeat.
    task_queue.register_node(resolved_node_id, resolved_host, concurrency, proxy_count)
    task_queue.start_heartbeat(
        resolved_node_id,
        resolved_host,
        concurrency,
        proxy_count,
        interval_seconds=heartbeat_interval,
    )
    task_queue.start_gc()

    typer.echo(
        f"Worker node '{resolved_node_id}' started: concurrency={concurrency} "
        f"max_depth={max_depth} proxies={proxy_count}"
    )

    cfg = CrawlConfig(
        thread_count=concurrency,
        request_delay_seconds=request_delay,
        max_depth=max_depth,
        max_total_requests=max_total_requests if max_total_requests else None,
    )

    # Workers drain the queue. Pass a single placeholder seed so CrawlEngine
    # doesn't raise on an empty seeds list — the engine will immediately
    # find the queue already populated from `worker seeds`.
    # We patch run() by passing seeds from the queue's current state instead.
    engine_metrics = _build_metrics_recorder(metrics_port)
    engine = CrawlEngine(
        db_pool=pool,
        config=cfg,
        task_queue=task_queue,
        proxy_pool=proxy_pool,
        metrics=engine_metrics,
    )

    try:
        # Collect pending UUIDs from the queue to pass as seeds.
        # These are already in crawl_queue; we just need at least one seed
        # for CrawlEngine.run() to start workers. We read one pending row
        # without claiming it so the queue's own dequeue logic handles it.
        with pool.connection() as conn:
            row = conn.execute(
                "SELECT uuid::text FROM crawl_queue WHERE status = 'pending' LIMIT 1"
            ).fetchone()

        if row is None:
            typer.echo("No pending seeds in crawl_queue. Load seeds via: mcosint worker seeds")
            return

        # run() expects at least one seed to bootstrap worker threads,
        # but all actual work comes from the PostgresTaskQueue.
        # We pass the single UUID we found; run() will upsert it and
        # enqueue it, but ON CONFLICT DO NOTHING means no duplicates.
        metrics = engine.run([row[0]])
        typer.echo(f"Done. {metrics.to_dict()}")
    finally:
        task_queue.stop_heartbeat()
        task_queue.stop_gc()
        try:
            task_queue.deregister_node(resolved_node_id)
        except Exception:
            pass


# ── worker seeds ──────────────────────────────────────────────────────────────


@worker_app.command("seeds")
def worker_seeds(
    uuids: Annotated[
        list[str],
        typer.Argument(help="UUIDs to enqueue as seeds"),
    ] = [],
    uuids_file: Path | None = typer.Option(
        None,
        "--uuids-file",
        help="Text file with one UUID per line (# comments supported)",
    ),
    depth: int = typer.Option(0, "--depth", help="Starting depth for all seeds"),
    db_url: str | None = typer.Option(
        None,
        "--db-url",
        help="PostgreSQL connection URL (default: DATABASE_URL env var)",
    ),
) -> None:
    """Insert UUIDs into crawl_queue so workers can pick them up."""
    resolved_db_url = db_url or os.getenv("DATABASE_URL")
    if not resolved_db_url:
        raise typer.BadParameter(
            "DATABASE_URL is not set. Provide --db-url or set DATABASE_URL in the environment."
        )

    all_uuids: list[str] = list(uuids)

    if uuids_file:
        lines = uuids_file.read_text(encoding="utf-8").splitlines()
        for line in lines:
            s = line.strip()
            if s and not s.startswith("#"):
                all_uuids.append(s)

    # Deduplicate while preserving order.
    all_uuids = list(dict.fromkeys(all_uuids))

    if not all_uuids:
        typer.echo("No UUIDs provided. Nothing to enqueue.")
        return

    pool = create_pool(DbPoolConfig(database_url=resolved_db_url, min_size=1, max_size=3))

    inserted = 0
    skipped = 0

    with pool.connection() as conn:
        for uuid in all_uuids:
            result = conn.execute(
                "INSERT INTO crawl_queue (uuid, depth, status) VALUES (%s, %s, 'pending')"
                " ON CONFLICT (uuid) DO NOTHING",
                (uuid, depth),
            )
            if result.rowcount > 0:
                inserted += 1
            else:
                skipped += 1

    typer.echo(f"Enqueued {inserted} seed(s), skipped {skipped} duplicate(s).")


# ── worker status ─────────────────────────────────────────────────────────────


@worker_app.command("status")
def worker_status(
    db_url: str | None = typer.Option(
        None,
        "--db-url",
        help="PostgreSQL connection URL (default: DATABASE_URL env var)",
    ),
) -> None:
    """Show active worker nodes and crawl_queue depth by status."""
    resolved_db_url = db_url or os.getenv("DATABASE_URL")
    if not resolved_db_url:
        raise typer.BadParameter(
            "DATABASE_URL is not set. Provide --db-url or set DATABASE_URL in the environment."
        )

    pool = create_pool(DbPoolConfig(database_url=resolved_db_url, min_size=1, max_size=3))

    with pool.connection() as conn:
        nodes = conn.execute(
            """
            SELECT node_id, host, last_heartbeat, worker_count, proxy_count, status
            FROM worker_nodes
            ORDER BY last_heartbeat DESC
            """
        ).fetchall()

        queue_counts = conn.execute(
            "SELECT status, COUNT(*) FROM crawl_queue GROUP BY status"
        ).fetchall()

    typer.echo("\n=== Worker Nodes ===")
    if nodes:
        typer.echo(
            f"{'NODE ID':<30} {'HOST':<20} {'LAST HEARTBEAT':<28} {'WORKERS':>7} {'PROXIES':>7} {'STATUS':<10}"
        )
        typer.echo("-" * 110)
        for row in nodes:
            node_id, host, last_hb, wcount, pcount, status = row
            typer.echo(
                f"{str(node_id):<30} {str(host):<20} {str(last_hb):<28} "
                f"{wcount:>7} {pcount:>7} {str(status):<10}"
            )
    else:
        typer.echo("(no nodes registered)")

    typer.echo("\n=== Queue Depth ===")
    if queue_counts:
        for status, count in queue_counts:
            typer.echo(f"  {status}: {count}")
    else:
        typer.echo("  (empty)")
