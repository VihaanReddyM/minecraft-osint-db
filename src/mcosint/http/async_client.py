from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from mcosint.http.flaresolverr import FlareSolverrClient, FlareSolverrError

log = logging.getLogger(__name__)


class RetryableHTTPStatus(RuntimeError):
    def __init__(self, status_code: int, message: str = "") -> None:
        super().__init__(message or f"Retryable HTTP status {status_code}")
        self.status_code = status_code


@dataclass(frozen=True)
class FetcherConfig:
    timeout_seconds: float = 30.0
    user_agent: str = "mcosint/0.1.0"
    flaresolverr_url: str | None = None
    flaresolverr_max_timeout_ms: int = 60_000


class AsyncFetcher:
    """Async HTTP fetcher with optional FlareSolverr integration."""

    def __init__(self, cfg: FetcherConfig) -> None:
        self._cfg = cfg
        self._client: httpx.AsyncClient | None = None
        self._fs: FlareSolverrClient | None = None

    async def __aenter__(self) -> AsyncFetcher:
        headers = {"User-Agent": self._cfg.user_agent}
        self._client = httpx.AsyncClient(headers=headers, timeout=self._cfg.timeout_seconds)

        if self._cfg.flaresolverr_url:
            self._fs = FlareSolverrClient(
                base_url=self._cfg.flaresolverr_url,
                max_timeout_ms=self._cfg.flaresolverr_max_timeout_ms,
            )
            await self._fs.__aenter__()

        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.aclose()

        if self._fs is not None:
            await self._fs.__aexit__(exc_type, exc, tb)

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("AsyncFetcher must be used as an async context manager")
        return self._client

    def _ensure_fs(self) -> FlareSolverrClient:
        if self._fs is None:
            raise RuntimeError(
                "FlareSolverr is not configured. Set FetcherConfig.flaresolverr_url "
                "or pass --flaresolverr-url"
            )
        return self._fs

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=0.5, max=10.0),
        retry=retry_if_exception_type(
            (
                httpx.TransportError,
                httpx.TimeoutException,
                RetryableHTTPStatus,
            )
        ),
    )
    async def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        use_flaresolverr: bool = False,
    ) -> Any:
        """GET a URL and parse JSON.

        Retries on network errors, timeouts, 429, and 5xx.
        """

        text = await self.get_text(
            url,
            params=params,
            headers=headers,
            use_flaresolverr=use_flaresolverr,
        )
        return json.loads(text)

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=0.5, max=10.0),
        retry=retry_if_exception_type(
            (
                httpx.TransportError,
                httpx.TimeoutException,
                RetryableHTTPStatus,
            )
        ),
    )
    async def get_text(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        use_flaresolverr: bool = False,
    ) -> str:
        if use_flaresolverr and self._cfg.flaresolverr_url:
            return await self._get_text_via_flaresolverr(url, headers=headers)
        return await self._get_text_direct(url, params=params, headers=headers)

    async def _get_text_direct(
        self,
        url: str,
        *,
        params: dict[str, Any] | None,
        headers: dict[str, str] | None,
    ) -> str:
        client = self._ensure_client()
        resp = await client.get(url, params=params, headers=headers)

        if resp.status_code == 429:
            raise RetryableHTTPStatus(429, "Rate limited")
        if 500 <= resp.status_code <= 599:
            raise RetryableHTTPStatus(resp.status_code, "Server error")

        resp.raise_for_status()
        return resp.text

    async def _get_text_via_flaresolverr(
        self,
        url: str,
        *,
        headers: dict[str, str] | None,
    ) -> str:
        # Note: FlareSolverr is used as a secondary transport to solve challenges.
        # It returns the page body as a string.
        fs = self._ensure_fs()
        try:
            sol = await fs.request_get(url, headers=headers)
        except FlareSolverrError as e:
            raise RetryableHTTPStatus(503, f"FlareSolverr failed: {e}") from e

        if sol.status == 429:
            raise RetryableHTTPStatus(429, "Rate limited")
        if 500 <= sol.status <= 599:
            raise RetryableHTTPStatus(sol.status, "Server error")

        # FlareSolverr returns raw text (HTML/JSON/etc.)
        return sol.response
