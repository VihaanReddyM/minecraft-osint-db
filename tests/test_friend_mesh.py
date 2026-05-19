import asyncio

from mcosint.graph.friend_mesh import build_friend_mesh
from mcosint.util.uuid_tools import normalize_uuid_str


class DummyNameMC:
    def __init__(self, mapping: dict[str, list[str]]):
        self.mapping = mapping
        self.calls: list[str] = []

    async def get_friend_uuids(self, uuid: str, *, use_flaresolverr: bool = False) -> list[str]:
        self.calls.append(uuid)
        return self.mapping.get(uuid, [])

    async def get_friends(self, uuid: str, *, use_flaresolverr: bool = False) -> list[dict]:
        # Mirror NameMC shape enough for build_friend_mesh's normalization.
        self.calls.append(uuid)
        return [{"uuid": u} for u in self.mapping.get(uuid, [])]


async def test_build_friend_mesh_respects_limits() -> None:
    mapping = {
        "a": ["b", "c", "x"],
        "b": ["d"],
        "c": ["e"],
        "d": ["f"],
    }

    namemc = DummyNameMC(mapping)

    mesh = await build_friend_mesh(
        namemc=namemc,  # duck-typed
        start_uuid="a",
        max_depth=1,
        delay_seconds=0,
        max_friends_per_user=2,
        pool=None,
    )

    assert mesh["a"] == ["b", "c"]
    # Depth=1 means we can fetch friends for b and c, but not for their friends.
    assert set(mesh.keys()).issubset({"a", "b", "c"})


async def test_build_friend_mesh_max_total_calls_stops() -> None:
    mapping = {"a": ["b"], "b": ["c"], "c": ["d"]}
    namemc = DummyNameMC(mapping)

    mesh = await build_friend_mesh(
        namemc=namemc,
        start_uuid="a",
        max_depth=10,
        delay_seconds=0,
        max_total_calls=2,
        max_friends_per_user=10,
        pool=None,
    )

    assert len(mesh) <= 2
    assert namemc.calls == ["a", "b"]


async def test_build_friend_mesh_default_is_unlimited() -> None:
    # Chain longer than the old default cap (=10).
    mapping = {"a": ["b"]}
    for i in range(0, 14):
        mapping[chr(ord("a") + i)] = [chr(ord("a") + i + 1)]
    mapping["o"] = []

    namemc = DummyNameMC(mapping)

    mesh = await build_friend_mesh(
        namemc=namemc,
        start_uuid="a",
        max_depth=20,
        delay_seconds=0,
        max_friends_per_user=10,
        pool=None,
    )

    # Should not stop at 10 by default.
    assert len(mesh) > 10


def test_normalize_uuid_str_handles_bytes_repr() -> None:
    assert (
        normalize_uuid_str("b'0097e01a-c6f9-4606-801f-80f9587a180e'")
        == "0097e01a-c6f9-4606-801f-80f9587a180e"
    )


def test_normalize_uuid_str_handles_real_bytes() -> None:
    assert (
        normalize_uuid_str(b"0097e01a-c6f9-4606-801f-80f9587a180e")
        == "0097e01a-c6f9-4606-801f-80f9587a180e"
    )


def test_async_friend_mesh_tests_run() -> None:
    # Avoid relying on pytest-asyncio in minimal environments.
    asyncio.run(test_build_friend_mesh_respects_limits())
    asyncio.run(test_build_friend_mesh_max_total_calls_stops())
    asyncio.run(test_build_friend_mesh_default_is_unlimited())
