from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DiscordWebhook:
    url: str

    def send(self, *, content: str | None = None, embed: dict[str, Any] | None = None) -> None:
        if not self.url:
            return

        payload: dict[str, Any] = {}
        if content:
            payload["content"] = content
        if embed:
            payload["embeds"] = [embed]

        if not payload:
            return

        try:
            httpx.post(self.url, json=payload, timeout=5.0)
        except Exception as e:
            log.warning("Discord webhook send failed: %s", e)
