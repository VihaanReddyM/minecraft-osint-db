from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import httpx


class FlareSolverrError(RuntimeError):
    pass


@dataclass(frozen=True)
class FlareSolverrSolution:
    url: str
    status: int
    headers: dict[str, str]
    response: str


class FlareSolverrClient:
    """Minimal async client for FlareSolverr's /v1 API."""

    def __init__(self, *, base_url: str, max_timeout_ms: int = 60_000) -> None:
        self._base_url = base_url.rstrip("/")
        self._max_timeout_ms = max_timeout_ms
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "FlareSolverrClient":
        self._client = httpx.AsyncClient(base_url=self._base_url)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.aclose()

    async def health(self) -> bool:
        """Best-effort health check.

        FlareSolverr doesn't have a strict health endpoint across all versions,
        so we just try hitting the base URL.
        """

        if self._client is None:
            raise RuntimeError("FlareSolverrClient must be used as an async context manager")

        resp = await self._client.get("/")
        return 200 <= resp.status_code < 500

    async def request_get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        max_timeout_ms: int | None = None,
    ) -> FlareSolverrSolution:
        if self._client is None:
            raise RuntimeError("FlareSolverrClient must be used as an async context manager")

        payload: dict[str, Any] = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": int(max_timeout_ms or self._max_timeout_ms),
        }
        if headers:
            payload["headers"] = headers

        resp = await self._client.post("/v1", json=payload)
        data = resp.json()

        if data.get("status") != "ok":
            raise FlareSolverrError(f"FlareSolverr error: {data.get('message')}")

        sol = data.get("solution") or {}
        return FlareSolverrSolution(
            url=sol.get("url") or url,
            status=int(sol.get("status") or 0),
            headers={str(k): str(v) for k, v in (sol.get("headers") or {}).items()},
            response=str(sol.get("response") or ""),
        )

    @staticmethod
    def solution_json(solution: FlareSolverrSolution) -> Any:
        """Parse solution.response as JSON (raises json.JSONDecodeError on failure)."""

        return json.loads(solution.response)
