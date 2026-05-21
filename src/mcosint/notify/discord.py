"""Discord webhook notifier.

Sends are dispatched on a background thread so callers in the crawl hot path
never block on network I/O. Failures are logged and swallowed — a broken
webhook should never crash a crawl.
"""

from __future__ import annotations

import atexit
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

import httpx

log = logging.getLogger(__name__)

# Module-level executor pool, lazily created. One worker thread is enough for
# the volume Discord webhooks see (start, finish, occasional 429), and serial
# dispatch preserves message ordering. Shared across DiscordWebhook instances
# so we don't spawn one thread per call site.
_executor_lock = threading.Lock()
_executor: ThreadPoolExecutor | None = None


def _get_executor() -> ThreadPoolExecutor:
    global _executor
    with _executor_lock:
        if _executor is None:
            _executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="discord-notifier"
            )
            atexit.register(_shutdown_executor)
        return _executor


def _shutdown_executor() -> None:
    """Best-effort drain on process exit. Registered via atexit."""
    global _executor
    with _executor_lock:
        if _executor is not None:
            # wait=True ensures pending sends flush before the process dies.
            # Timeout is implicit (no timeout arg); rely on httpx's own 5s budget.
            _executor.shutdown(wait=True)
            _executor = None


def _do_post(url: str, payload: dict[str, Any]) -> None:
    """Actually issue the webhook POST. Runs on the background executor."""
    try:
        httpx.post(url, json=payload, timeout=5.0)
    except Exception as e:
        log.warning("Discord webhook send failed: %s", e)


@dataclass(frozen=True)
class DiscordWebhook:
    """Fire-and-forget Discord webhook.

    `send()` returns immediately. The actual HTTP POST happens on a shared
    background thread. On process exit, an atexit hook drains pending sends.
    """

    url: str
    # Held internally so callers can pass DiscordWebhook(url) positionally
    # without worrying about the executor. Excluded from repr/comparison.
    _executor: ThreadPoolExecutor = field(
        default_factory=_get_executor, repr=False, compare=False
    )

    def send(
        self, *, content: str | None = None, embed: dict[str, Any] | None = None
    ) -> None:
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
            self._executor.submit(_do_post, self.url, payload)
        except RuntimeError:
            # Executor was already shut down (e.g. process is mid-exit).
            log.debug("Discord webhook dispatch skipped: executor closed")


def shutdown_notifier_executor(wait: bool = True) -> None:
    """Public helper for tests / explicit cleanup. Idempotent."""
    global _executor
    with _executor_lock:
        if _executor is not None:
            _executor.shutdown(wait=wait)
            _executor = None
