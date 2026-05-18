from __future__ import annotations

from psycopg import Connection


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS players (
  uuid UUID PRIMARY KEY,
  username TEXT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  friends_crawled_at TIMESTAMPTZ NULL,
  min_depth INTEGER NULL
);

-- Migration safety (if upgrading from older schema)
ALTER TABLE players ADD COLUMN IF NOT EXISTS friends_crawled_at TIMESTAMPTZ NULL;
ALTER TABLE players ADD COLUMN IF NOT EXISTS min_depth INTEGER NULL;

CREATE TABLE IF NOT EXISTS friendships (
  player_uuid UUID NOT NULL REFERENCES players(uuid) ON DELETE CASCADE,
  friend_uuid UUID NOT NULL REFERENCES players(uuid) ON DELETE CASCADE,
  PRIMARY KEY (player_uuid, friend_uuid)
);
""".strip()


INDEXES_SQL = """
-- Fast lookups from a player -> their friends
CREATE INDEX IF NOT EXISTS idx_friendships_player_uuid ON friendships(player_uuid);

-- Fast reverse lookups (who is friends with X)
CREATE INDEX IF NOT EXISTS idx_friendships_friend_uuid ON friendships(friend_uuid);

-- Helpful for undirected traversals / joins
CREATE INDEX IF NOT EXISTS idx_friendships_friend_player ON friendships(friend_uuid, player_uuid);

-- Helps filtering/sorting by recency for refresh jobs
CREATE INDEX IF NOT EXISTS idx_players_updated_at ON players(updated_at);

-- Fast resume queries (uncrawled frontier)
CREATE INDEX IF NOT EXISTS idx_players_friends_crawled_at ON players(friends_crawled_at);
CREATE INDEX IF NOT EXISTS idx_players_min_depth ON players(min_depth);
CREATE INDEX IF NOT EXISTS idx_players_min_depth_crawled ON players(min_depth, friends_crawled_at);
""".strip()


def _execute_statements(conn: Connection, sql: str) -> None:
    # psycopg does not guarantee multi-statement execution in a single `execute()` call.
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for stmt in statements:
        conn.execute(stmt)


def create_schema(conn: Connection) -> None:
    """Create tables and indexes.

    This function is safe to run repeatedly.
    """

    _execute_statements(conn, SCHEMA_SQL)
    _execute_statements(conn, INDEXES_SQL)
