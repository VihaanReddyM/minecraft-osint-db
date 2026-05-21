"""Tests for the background-thread Discord notifier (Phase 5b)."""

from __future__ import annotations

import time
from unittest.mock import patch

from mcosint.notify import discord as notify_discord
from mcosint.notify.discord import (
    DiscordWebhook,
    shutdown_notifier_executor,
)


def setup_function(_fn) -> None:
    # Each test gets a fresh executor.
    shutdown_notifier_executor(wait=True)


def teardown_function(_fn) -> None:
    shutdown_notifier_executor(wait=True)


class TestDiscordWebhookFireAndForget:
    def test_send_returns_immediately(self) -> None:
        webhook = DiscordWebhook("https://discord.example/webhooks/test")

        def slow_post(url, json, timeout):  # noqa: ARG001
            time.sleep(0.5)

        with patch("mcosint.notify.discord.httpx.post", side_effect=slow_post):
            t0 = time.monotonic()
            webhook.send(content="hello")
            elapsed = time.monotonic() - t0
        # The background submit must be fire-and-forget.
        assert elapsed < 0.1, f"send() blocked for {elapsed:.3f}s"

    def test_empty_url_is_noop(self) -> None:
        webhook = DiscordWebhook("")
        with patch("mcosint.notify.discord.httpx.post") as post:
            webhook.send(content="hello")
            time.sleep(0.05)
        post.assert_not_called()

    def test_empty_payload_is_noop(self) -> None:
        webhook = DiscordWebhook("https://discord.example/webhooks/test")
        with patch("mcosint.notify.discord.httpx.post") as post:
            webhook.send()  # neither content nor embed
            time.sleep(0.05)
        post.assert_not_called()

    def test_send_dispatches_to_executor(self) -> None:
        webhook = DiscordWebhook("https://discord.example/webhooks/test")
        calls: list[tuple] = []

        def capture_post(url, json, timeout):
            calls.append((url, json, timeout))

        with patch("mcosint.notify.discord.httpx.post", side_effect=capture_post):
            webhook.send(embed={"title": "hi"})
            shutdown_notifier_executor(wait=True)
        assert len(calls) == 1
        assert calls[0][0] == "https://discord.example/webhooks/test"
        assert calls[0][1] == {"embeds": [{"title": "hi"}]}

    def test_exception_in_post_is_swallowed(self) -> None:
        webhook = DiscordWebhook("https://discord.example/webhooks/test")

        def raise_post(url, json, timeout):  # noqa: ARG001
            raise RuntimeError("boom")

        with patch("mcosint.notify.discord.httpx.post", side_effect=raise_post):
            webhook.send(content="hello")
            shutdown_notifier_executor(wait=True)
        # Test passes if the worker thread crashed without taking us down.

    def test_send_after_shutdown_is_silent(self) -> None:
        webhook = DiscordWebhook("https://discord.example/webhooks/test")
        # Force the executor to exist, then shut it down.
        with patch("mcosint.notify.discord.httpx.post"):
            webhook.send(content="warm-up")
            shutdown_notifier_executor(wait=True)

        # The dataclass instance still references the now-closed executor.
        # A subsequent send() should log and return, not raise.
        with patch("mcosint.notify.discord.httpx.post") as post:
            webhook.send(content="late")
        post.assert_not_called()


class TestExecutorSingleton:
    def test_executor_is_reused_across_instances(self) -> None:
        a = DiscordWebhook("https://x/1")
        b = DiscordWebhook("https://x/2")
        assert a._executor is b._executor

    def test_shutdown_resets_executor(self) -> None:
        DiscordWebhook("https://x/1")  # force-create
        assert notify_discord._executor is not None
        shutdown_notifier_executor(wait=True)
        assert notify_discord._executor is None
