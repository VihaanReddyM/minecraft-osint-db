from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

import psycopg
from psycopg import Connection
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

log = logging.getLogger(__name__)


class DbTransientError(RuntimeError):
    """Raised for DB errors worth retrying at the application layer."""


def _is_retryable_psycopg_error(exc: BaseException) -> bool:
    # psycopg3 exceptions are structured but we keep this conservative.
    return isinstance(
        exc,
        (
            psycopg.OperationalError,
            psycopg.InterfaceError,
            psycopg.errors.DeadlockDetected,
            psycopg.errors.SerializationFailure,
        ),
    )


def upsert_discovered_players(
    conn: Connection,
    players: Iterable[tuple[str, str | None, int | None]],
) -> None:
    """Upsert many discovered players (not marking them crawled).

    - Updates `username` if provided.
    - Maintains `min_depth` as the minimum known depth.
    - Updates `updated_at`.
    """

    sql = """
    INSERT INTO players (uuid, username, min_depth, updated_at)
    VALUES (%s, %s, %s, NOW())
    ON CONFLICT (uuid)
    DO UPDATE SET
      username = COALESCE(EXCLUDED.username, players.username),
      min_depth = LEAST(COALESCE(players.min_depth, EXCLUDED.min_depth), EXCLUDED.min_depth),
      updated_at = NOW();
    """.strip()

    rows = list(players)
    if not rows:
        return

    try:
        conn.executemany(sql, rows)
    except Exception as e:
        if _is_retryable_psycopg_error(e):
            raise DbTransientError(str(e)) from e
        raise


def upsert_crawled_player(
    conn: Connection,
    *,
    uuid: str,
    username: str | None,
    depth: int | None,
) -> None:
    """Upsert a player and mark it as successfully crawled now."""

    sql = """
    INSERT INTO players (uuid, username, min_depth, updated_at, friends_crawled_at)
    VALUES (%s, %s, %s, NOW(), NOW())
    ON CONFLICT (uuid)
    DO UPDATE SET
      username = COALESCE(EXCLUDED.username, players.username),
      min_depth = LEAST(COALESCE(players.min_depth, EXCLUDED.min_depth), EXCLUDED.min_depth),
      updated_at = NOW(),
      friends_crawled_at = NOW();
    """.strip()

    try:
        conn.execute(sql, (uuid, username, depth))
    except Exception as e:
        if _is_retryable_psycopg_error(e):
            raise DbTransientError(str(e)) from e
        raise


def insert_friendships(conn: Connection, edges: Iterable[tuple[str, str]]) -> None:
    """Insert many friendships, de-duplicated by PK."""

    sql = """
    INSERT INTO friendships (player_uuid, friend_uuid)
    VALUES (%s, %s)
    ON CONFLICT DO NOTHING;
    """.strip()

    rows = list(edges)
    if not rows:
        return

    try:
        conn.executemany(sql, rows)
    except Exception as e:
        if _is_retryable_psycopg_error(e):
            raise DbTransientError(str(e)) from e
        raise


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=0.5, max=10.0),
    retry=retry_if_exception_type((DbTransientError,)),
)
def is_player_friends_crawled(conn: Connection, uuid: str) -> bool:
    """Return True if we have already crawled this player's friends."""

    try:
        row = conn.execute(
            "SELECT friends_crawled_at IS NOT NULL FROM players WHERE uuid = %s",
            (uuid,),
        ).fetchone()
    except Exception as e:
        if _is_retryable_psycopg_error(e):
            raise DbTransientError(str(e)) from e
        raise

    if row is None:
        return False
    return bool(row[0])


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=0.5, max=10.0),
    retry=retry_if_exception_type((DbTransientError,)),
)
def get_friend_uuids_from_db(
    conn: Connection,
    *,
    player_uuid: str,
    limit: int | None = None,
) -> list[str]:
    """Get friend UUIDs for a player from the DB."""

    sql = "SELECT friend_uuid::text FROM friendships WHERE player_uuid = %s"
    params: tuple[Any, ...] = (player_uuid,)
    if limit is not None:
        sql += " LIMIT %s"
        params = (player_uuid, limit)

    try:
        rows = conn.execute(sql, params).fetchall()
    except Exception as e:
        if _is_retryable_psycopg_error(e):
            raise DbTransientError(str(e)) from e
        raise

    return [str(r[0]) for r in rows]


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=0.5, max=10.0),
    retry=retry_if_exception_type((DbTransientError,)),
)
def persist_namemc_friend_response(
    conn: Connection,
    *,
    player_uuid: str,
    player_username: str | None,
    player_depth: int | None,
    friends: list[dict[str, Any]],
) -> list[tuple[str, str | None]]:
    """Persist one NameMC friends response.

    Returns the normalized friend list: [(friend_uuid, friend_username), ...]

    Expected input item keys (best-effort):
      - uuid
      - name / username
    """

    normalized_friends: list[tuple[str, str | None]] = []
    for f in friends:
        if not isinstance(f, dict):
            continue
        f_uuid = f.get("uuid")
        if not f_uuid:
            continue
        f_name = f.get("name") or f.get("username")
        normalized_friends.append((str(f_uuid), str(f_name) if f_name else None))

    # Sorting helps reduce deadlock probability in high concurrency.
    normalized_friends.sort(key=lambda x: x[0])

    try:
        # Transaction groups player upsert + friend upserts + edge inserts.
        with conn.transaction():
            upsert_crawled_player(
                conn,
                uuid=player_uuid,
                username=player_username,
                depth=player_depth,
            )

            discovered = [
                (f_uuid, f_name, (player_depth + 1) if player_depth is not None else None)
                for (f_uuid, f_name) in normalized_friends
            ]
            upsert_discovered_players(conn, discovered)

            insert_friendships(
                conn,
                ((player_uuid, f_uuid) for (f_uuid, _) in normalized_friends),
            )
    except Exception as e:
        if _is_retryable_psycopg_error(e):
            raise DbTransientError(str(e)) from e
        raise

    return normalized_friends
