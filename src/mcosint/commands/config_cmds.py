from __future__ import annotations

from pathlib import Path

import typer

from mcosint.config import default_config_path, save_default_config

config_app = typer.Typer(add_completion=False, help="Config commands")


@config_app.command("init")
def config_init(
    path: Path | None = typer.Option(
        None,
        "--path",
        help="Where to write the config file (defaults to the user config dir)",
    ),
    overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite existing config"),
) -> None:
    cfg_path = save_default_config(path, overwrite=overwrite)
    typer.echo(f"Wrote config: {cfg_path}")


@config_app.command("path")
def config_path() -> None:
    typer.echo(str(default_config_path()))
