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


def upsert_players(conn: Connection, players: Iterable[tuple[str, str | None]]) -> None:
    """Upsert many players.

    Uses `ON CONFLICT DO UPDATE` to refresh username and updated_at.
    """

    sql = """
    INSERT INTO players (uuid, username, updated_at)
    VALUES (%s, %s, NOW())
    ON CONFLICT (uuid)
    DO UPDATE SET
      username = COALESCE(EXCLUDED.username, players.username),
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
def persist_namemc_friend_response(
    conn: Connection,
    *,
    player_uuid: str,
    player_username: str | None,
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

    try:
        # Transaction groups player upsert + friend upserts + edge inserts.
        with conn.transaction():
            upsert_players(conn, [(player_uuid, player_username), *normalized_friends])
            insert_friendships(
                conn,
                ((player_uuid, f_uuid) for (f_uuid, _) in normalized_friends),
            )
    except Exception as e:
        if _is_retryable_psycopg_error(e):
            raise DbTransientError(str(e)) from e
        raise

    return normalized_friends
