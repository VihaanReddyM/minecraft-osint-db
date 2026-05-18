from __future__ import annotations

import asyncio
import logging
from collections import deque

from mcosint.services.namemc import NameMCClient

log = logging.getLogger(__name__)


async def build_friend_mesh(
    *,
    namemc: NameMCClient,
    start_uuid: str,
    max_depth: int = 2,
    delay_seconds: float = 1.0,
    max_total_calls: int = 10,
    max_friends_per_user: int = 3,
    use_flaresolverr: bool = False,
) -> dict[str, list[str]]:
    """Build a sample friend mesh (BFS) using the NameMC API.

    This mirrors your original `main.py` behavior, but is async and injectable.
    """

    visited: set[str] = set()
    mesh: dict[str, list[str]] = {}
    queue: deque[tuple[str, int]] = deque([(start_uuid, 0)])

    calls_made = 0

    log.info(
        "Starting mesh for %s (max_depth=%s, max_total_calls=%s, max_friends_per_user=%s)",
        start_uuid,
        max_depth,
        max_total_calls,
        max_friends_per_user,
    )

    while queue:
        if calls_made >= max_total_calls:
            log.info("Reached max_total_calls=%s; stopping.", max_total_calls)
            break

        current_uuid, depth = queue.popleft()
        if depth > max_depth:
            continue
        if current_uuid in visited:
            continue

        calls_made += 1
        log.info("[%s/%s] Fetching %s (depth=%s)", calls_made, max_total_calls, current_uuid, depth)

        try:
            friends = await namemc.get_friend_uuids(current_uuid, use_flaresolverr=use_flaresolverr)
        except Exception:
            log.exception("Failed to fetch friends for %s", current_uuid)
            visited.add(current_uuid)
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
            continue

        friends = friends[:max_friends_per_user]
        mesh[current_uuid] = friends
        visited.add(current_uuid)

        for f_uuid in friends:
            if f_uuid not in visited:
                queue.append((f_uuid, depth + 1))

        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)

    return mesh
