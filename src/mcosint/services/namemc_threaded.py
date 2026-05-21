from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any

import httpx

from mcosint.util.uuid_tools import normalize_uuid_str

log = logging.getLogger(__name__)


_JSON_ARRAY_RE = re.compile(r"\[\s*\{.*?\}\s*\]", re.DOTALL)
_JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)
_HTML_TAG_RE = re.compile(r"<[^<]+?>")


def _extract_json_text(text: str) -> str:
    """Best-effort extraction of JSON from FlareSolverr HTML wrappers."""

    s = text.strip()
    if not s:
        return s

    if s.startswith("[") or s.startswith("{"):
        return s

    m = _JSON_ARRAY_RE.search(s)
    if m:
        return m.group(0)

    m = _JSON_OBJECT_RE.search(s)
    if m:
        return m.group(0)

    # Last resort: strip HTML tags and return.
    return _HTML_TAG_RE.sub("", s).strip()


def _normalize_flaresolverr_base_url(url: str) -> str:
    u = url.strip().rstrip("/")
    if u.endswith("/v1"):
        u = u[: -len("/v1")]
    return u


def _parse_retry_after(headers: dict[str, Any] | httpx.Headers) -> float | None:
    try:
        ra = headers.get("Retry-After")  # type: ignore[attr-defined]
    except Exception:
        ra = None
    if not ra:
        return None
    try:
        return float(ra)
    except Exception:
        return None


@dataclass(frozen=True)
class ThreadedNameMCConfig:
    user_agent: str = "mcosint/0.1.0"
    timeout_seconds: float = 30.0
    flaresolverr_url: str | None = None  # base URL, e.g. http://host:8191
    flaresolverr_max_timeout_ms: int = 60_000
    proxy_url: str | None = None  # SOCKS5/HTTP proxy for direct (non-FlareSolverr) requests


class ThreadedNameMCClient:
    """Blocking NameMC client with optional FlareSolverr and/or SOCKS5 proxy."""

    def __init__(self, cfg: ThreadedNameMCConfig) -> None:
        self._cfg = cfg
        client_kwargs: dict = {
            "headers": {"User-Agent": cfg.user_agent, "Content-Type": "application/json"},
            "timeout": httpx.Timeout(cfg.timeout_seconds),
        }
        if cfg.proxy_url:
            client_kwargs["proxy"] = cfg.proxy_url
        self._client = httpx.Client(**client_kwargs)

    def close(self) -> None:
        self._client.close()

    def get_friends(self, uuid: str, *, use_flaresolverr: bool) -> list[dict[str, Any]]:
        uuid = normalize_uuid_str(uuid)
        url = f"https://api.namemc.com/profile/{uuid}/friends"

        if use_flaresolverr:
            fs_url = self._cfg.flaresolverr_url
            if not fs_url:
                raise RuntimeError("FlareSolverr is enabled but no flaresolverr_url is configured")

            fs_base = _normalize_flaresolverr_base_url(fs_url)

            payload = {
                "cmd": "request.get",
                "url": url,
                "maxTimeout": int(self._cfg.flaresolverr_max_timeout_ms),
            }

            resp = self._client.post(fs_base + "/v1", json=payload)
            if resp.status_code == 429:
                raise RateLimitedError(_parse_retry_after(resp.headers))
            resp.raise_for_status()

            data = resp.json()
            if data.get("status") != "ok":
                raise RuntimeError(f"FlareSolverr error: {data.get('message')}")

            sol = data.get("solution") or {}
            sol_status = int(sol.get("status") or 0)
            sol_headers = sol.get("headers") or {}
            if sol_status == 429:
                raise RateLimitedError(_parse_retry_after(sol_headers))
            if sol_status and sol_status >= 400:
                raise httpx.HTTPStatusError(
                    f"Upstream returned {sol_status}",
                    request=resp.request,
                    response=resp,
                )

            body = str(sol.get("response") or "")
            body = _extract_json_text(body)
            parsed = json.loads(body)
        else:
            resp = self._client.get(url)
            if resp.status_code == 429:
                raise RateLimitedError(_parse_retry_after(resp.headers))
            resp.raise_for_status()
            parsed = resp.json()

        if isinstance(parsed, dict):
            log.warning("NameMC returned dict response: %s", parsed)

            # Handle embedded rate limit
            status = parsed.get("status")

            if status == 429:
                raise RateLimitedError(
                    float(parsed.get("Retry-After", 10))
                )

            # Empty/no-friends cases
            if parsed.get("friends") == []:
                return []

            raise ValueError(f"Unexpected NameMC dict response: {parsed}")

        if not isinstance(parsed, list):
            raise ValueError(
                f"Unexpected NameMC response type: {type(parsed)} body={parsed}"
            )

        return [x for x in parsed if isinstance(x, dict)]


class RateLimitedError(RuntimeError):
    def __init__(self, retry_after_seconds: float | None = None, message: str = "Rate limited") -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds

    pass
