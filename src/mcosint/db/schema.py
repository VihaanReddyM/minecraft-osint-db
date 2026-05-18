from __future__ import annotations

from psycopg import Connection


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS players (
  uuid UUID PRIMARY KEY,
  username TEXT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
""".strip()


def create_schema(conn: Connection) -> None:
    """Create tables and indexes.

    This function is safe to run repeatedly.
    """

    conn.execute(SCHEMA_SQL)
    conn.execute(INDEXES_SQL)
