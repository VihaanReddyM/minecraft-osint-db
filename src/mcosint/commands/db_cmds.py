from __future__ import annotations

import os

import typer

from mcosint.db.connection import DbPoolConfig, create_pool
from mcosint.db.schema import create_schema

db_app = typer.Typer(add_completion=False, help="Database commands")


@db_app.command("init")
def db_init(
    database_url: str | None = typer.Option(
        None,
        "--database-url",
        help="Override DATABASE_URL (defaults to env var DATABASE_URL)",
    ),
    pool_max_size: int = typer.Option(
        10,
        "--pool-max-size",
        help="DB connection pool max size (for init this can be small)",
    ),
) -> None:
    url = database_url or os.getenv("DATABASE_URL")
    if not url:
        raise typer.BadParameter(
            "DATABASE_URL is not set. Provide --database-url or set DATABASE_URL in the environment."
        )

    pool = create_pool(DbPoolConfig(database_url=url, min_size=1, max_size=pool_max_size))
    with pool.connection() as conn:
        create_schema(conn)

    typer.echo("DB schema ensured.")
