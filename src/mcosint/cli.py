from __future__ import annotations

import typer

from mcosint.commands import config_app, flaresolverr_app, http_app, namemc_app
from mcosint.logging import configure_logging

app = typer.Typer(add_completion=False, help="Minecraft OSINT CLI toolkit")

app.add_typer(config_app, name="config")
app.add_typer(flaresolverr_app, name="flaresolverr")
app.add_typer(http_app, name="http")
app.add_typer(namemc_app, name="namemc")


@app.callback()
def _root(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
) -> None:
    configure_logging(verbose=verbose)


def main() -> None:
    app()
