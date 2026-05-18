# pyright: reportMissingImports=false

"""Repo entrypoint.

This file remains for convenience so you can still run:

  python main.py ...

But the real app is now a proper CLI package under `src/mcosint`.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_src_on_path() -> None:
    repo_root = Path(__file__).resolve().parent
    src = repo_root / "src"
    if src.exists():
        sys.path.insert(0, str(src))


_ensure_src_on_path()


def main() -> None:
    from mcosint.cli import main as cli_main  # type: ignore

    cli_main()


if __name__ == "__main__":
    main()
