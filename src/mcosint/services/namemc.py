from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from mcosint.http.async_client import AsyncFetcher
from mcosint.util.uuid_tools import normalize_uuid_str

log = logging.getLogger(__name__)


class NameMCClient:
    def __init__(self, fetcher: AsyncFetcher) -> None:
        self._fetcher = fetcher

    async def get_friends(
        self,
        uuid: str,
        *,
        use_flaresolverr: bool = False
    ) -> list[dict[str, Any]]:

        uuid = normalize_uuid_str(uuid)

        async def do_request(target_uuid: str):
            url = f"https://api.namemc.com/profile/{target_uuid}/friends"
            return await self._fetcher.get_json(
                url,
                use_flaresolverr=use_flaresolverr,
            )

        try:
            data = await do_request(uuid)

        except httpx.HTTPStatusError as e:
            if e.response.status_code != 429:
                raise

            log.info("429 received for %s, sending decoy request", uuid)

            try:
                # Send garbage request to rotate/reset rate limit behavior
                await do_request("00000000-0000-0000-0000-000000000000")
            except Exception:
                pass

            await asyncio.sleep(2)

            log.info("Retrying actual UUID %s", uuid)

            data = await do_request(uuid)

        if not isinstance(data, list):
            raise ValueError(f"Unexpected NameMC response type: {type(data)}")

        return [item for item in data if isinstance(item, dict)]

    async def get_friend_uuids(
        self,
        uuid: str,
        *,
        use_flaresolverr: bool = False
    ) -> list[str]:

        friends = await self.get_friends(
            uuid,
            use_flaresolverr=use_flaresolverr,
        )

        uuids: list[str] = []

        for item in friends:
            if "uuid" in item:
                uuids.append(str(item["uuid"]))

        return uuids