from __future__ import annotations

import os
from dataclasses import dataclass

from psycopg_pool import ConnectionPool


@dataclass(frozen=True)
class DbPoolConfig:
    database_url: str
    min_size: int = 1
    max_size: int = 10


_pool: ConnectionPool | None = None


def load_db_pool_config_from_env() -> DbPoolConfig:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Provide it via environment variables (see .env.example)."
        )

    min_size = int(os.getenv("DB_POOL_MIN_SIZE", "1"))
    max_size = int(os.getenv("DB_POOL_MAX_SIZE", "10"))

    return DbPoolConfig(database_url=url, min_size=min_size, max_size=max_size)


def get_pool(cfg: DbPoolConfig | None = None) -> ConnectionPool:
    """Get (and lazily initialize) the global thread-safe connection pool."""

    global _pool
    if _pool is not None:
        return _pool

    cfg = cfg or load_db_pool_config_from_env()

    # psycopg_pool.ConnectionPool is thread-safe and intended for multi-threaded workloads.
    _pool = ConnectionPool(
        conninfo=cfg.database_url,
        min_size=cfg.min_size,
        max_size=cfg.max_size,
        open=True,
        timeout=30,
    )
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
