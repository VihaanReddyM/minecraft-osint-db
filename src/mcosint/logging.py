import logging

from rich.logging import RichHandler


def configure_logging(*, verbose: bool = False) -> None:
    """Configure app-wide logging.

    Call once at process start (e.g., from the CLI callback).
    """

    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
    )
