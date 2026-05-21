from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from mcosint.cli import app


def _make_mock_pool(rowcount: int = 1) -> tuple[MagicMock, MagicMock]:
    """Build a minimal mock ConnectionPool for worker_cmds tests."""
    cursor = MagicMock()
    cursor.rowcount = rowcount

    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value = cursor

    pool = MagicMock()
    pool.connection.return_value = conn
    return pool, conn


class TestWorkerSeedsCommand:
    def _runner(self) -> CliRunner:
        return CliRunner()

    def test_requires_db_url(self) -> None:
        runner = self._runner()
        # Remove DATABASE_URL from env so the command cannot find it.
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        result = runner.invoke(
            app,
            ["worker", "seeds", "00000000-0000-0000-0000-000000000001"],
            env=env,
        )
        assert result.exit_code != 0

    def test_no_uuids_exits_cleanly(self) -> None:
        runner = self._runner()
        pool, _ = _make_mock_pool()
        with (
            patch("mcosint.commands.worker_cmds.create_pool", return_value=pool),
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://x:x@localhost/x"}),
        ):
            result = runner.invoke(app, ["worker", "seeds"])
        assert result.exit_code == 0
        assert "Nothing to enqueue" in result.output

    def test_single_uuid_inserted(self) -> None:
        runner = self._runner()
        pool, conn = _make_mock_pool(rowcount=1)
        with (
            patch("mcosint.commands.worker_cmds.create_pool", return_value=pool),
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://x:x@localhost/x"}),
        ):
            result = runner.invoke(
                app,
                ["worker", "seeds", "00000000-0000-0000-0000-000000000001"],
            )
        assert result.exit_code == 0
        assert "Enqueued 1" in result.output
        assert "skipped 0" in result.output

    def test_duplicate_uuid_skipped(self) -> None:
        runner = self._runner()
        pool, conn = _make_mock_pool(rowcount=0)
        with (
            patch("mcosint.commands.worker_cmds.create_pool", return_value=pool),
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://x:x@localhost/x"}),
        ):
            result = runner.invoke(
                app,
                ["worker", "seeds", "00000000-0000-0000-0000-000000000001"],
            )
        assert result.exit_code == 0
        assert "skipped 1" in result.output

    def test_multiple_uuids(self) -> None:
        runner = self._runner()
        # First call inserts, subsequent calls are skipped (rowcount=0 for dupes).
        call_count = [0]
        original_execute = None

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            m = MagicMock()
            m.rowcount = 1 if call_count[0] <= 2 else 0
            return m

        pool, conn = _make_mock_pool()
        conn.execute.side_effect = side_effect

        with (
            patch("mcosint.commands.worker_cmds.create_pool", return_value=pool),
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://x:x@localhost/x"}),
        ):
            result = runner.invoke(
                app,
                [
                    "worker",
                    "seeds",
                    "00000000-0000-0000-0000-000000000001",
                    "00000000-0000-0000-0000-000000000002",
                ],
            )
        assert result.exit_code == 0
        assert "Enqueued 2" in result.output

    def test_uuids_file_loaded(self, tmp_path: Path) -> None:
        seeds_file = tmp_path / "seeds.txt"
        seeds_file.write_text(
            "# comment\n"
            "00000000-0000-0000-0000-000000000001\n"
            "00000000-0000-0000-0000-000000000002\n"
        )
        runner = self._runner()
        pool, conn = _make_mock_pool(rowcount=1)

        with (
            patch("mcosint.commands.worker_cmds.create_pool", return_value=pool),
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://x:x@localhost/x"}),
        ):
            result = runner.invoke(
                app,
                ["worker", "seeds", "--uuids-file", str(seeds_file)],
            )
        assert result.exit_code == 0
        assert "Enqueued 2" in result.output

    def test_custom_depth_passed(self) -> None:
        runner = self._runner()
        pool, conn = _make_mock_pool(rowcount=1)

        with (
            patch("mcosint.commands.worker_cmds.create_pool", return_value=pool),
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://x:x@localhost/x"}),
        ):
            result = runner.invoke(
                app,
                [
                    "worker",
                    "seeds",
                    "--depth",
                    "2",
                    "00000000-0000-0000-0000-000000000001",
                ],
            )
        assert result.exit_code == 0
        # Verify the depth argument was passed to execute()
        call_args = conn.execute.call_args
        assert call_args[0][1][1] == 2  # depth parameter

    def test_db_url_flag_overrides_env(self) -> None:
        runner = self._runner()
        pool, conn = _make_mock_pool(rowcount=0)
        captured_urls: list[str] = []

        def capture_create_pool(cfg):
            captured_urls.append(cfg.database_url)
            return pool

        with (
            patch("mcosint.commands.worker_cmds.create_pool", side_effect=capture_create_pool),
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://env:env@localhost/env"}),
        ):
            result = runner.invoke(
                app,
                [
                    "worker",
                    "seeds",
                    "--db-url",
                    "postgresql://flag:flag@localhost/flag",
                    "00000000-0000-0000-0000-000000000001",
                ],
            )
        # The pool should be created with the --db-url flag value, not the env var.
        assert result.exit_code == 0
        assert len(captured_urls) == 1
        assert captured_urls[0] == "postgresql://flag:flag@localhost/flag"


class TestWorkerStatusCommand:
    def _runner(self) -> CliRunner:
        return CliRunner()

    def test_requires_db_url(self) -> None:
        runner = self._runner()
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        result = runner.invoke(app, ["worker", "status"], env=env)
        assert result.exit_code != 0

    def test_status_no_nodes(self) -> None:
        runner = self._runner()

        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        # fetchall() returns empty for both queries
        conn.execute.return_value.fetchall.return_value = []

        pool = MagicMock()
        pool.connection.return_value = conn

        with (
            patch("mcosint.commands.worker_cmds.create_pool", return_value=pool),
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://x:x@localhost/x"}),
        ):
            result = runner.invoke(app, ["worker", "status"])

        assert result.exit_code == 0
        assert "no nodes registered" in result.output
        assert "empty" in result.output
