from __future__ import annotations

import logging

from mcosint.http.async_client import AsyncFetcher

log = logging.getLogger(__name__)


class NameMCClient:
    """NameMC API client.

    Notes:
      - This currently only wraps the friends endpoint used in your prototype.
      - Extend this module with more endpoints as you grow the tool.
    """

    def __init__(self, fetcher: AsyncFetcher) -> None:
        self._fetcher = fetcher

    async def get_friend_uuids(self, uuid: str, *, use_flaresolverr: bool = False) -> list[str]:
        url = f"https://api.namemc.com/profile/{uuid}/friends"

        data = await self._fetcher.get_json(url, use_flaresolverr=use_flaresolverr)

        if not isinstance(data, list):
            raise ValueError(f"Unexpected NameMC response type: {type(data)}")

        uuids: list[str] = []
        for item in data:
            if isinstance(item, dict) and "uuid" in item:
                uuids.append(str(item["uuid"]))
        return uuids
