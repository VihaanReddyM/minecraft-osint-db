from __future__ import annotations

from typing import Any, Protocol


class Notifier(Protocol):
    """Minimal notification interface.

    DiscordWebhook already satisfies this protocol.
    """

    def send(self, *, content: str | None = None, embed: dict[str, Any] | None = None) -> None: ...
