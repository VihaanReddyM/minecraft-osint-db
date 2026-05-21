from __future__ import annotations

from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from mcosint.proxy.pool import ProxyConfig


def make_worker_client(
    proxy: ProxyConfig | None,
    *,
    user_agent: str,
    timeout_seconds: float,
) -> httpx.Client:
    """Create an isolated httpx.Client for one worker thread.

    Each worker should call this once and own the returned client exclusively —
    do not share across threads.

    If proxy is provided, all requests route through it. Supports socks5, http, and https
    proxy URLs. SOCKS5 requires the httpx[socks] extra (socksio).
    """
    kwargs: dict = {
        "headers": {"User-Agent": user_agent, "Content-Type": "application/json"},
        "timeout": httpx.Timeout(timeout_seconds),
    }
    if proxy:
        kwargs["proxy"] = proxy.httpx_url()
    return httpx.Client(**kwargs)
