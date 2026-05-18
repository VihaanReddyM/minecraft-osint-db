import pytest

from mcosint.graph.friend_mesh import build_friend_mesh


class DummyNameMC:
    def __init__(self, mapping: dict[str, list[str]]):
        self.mapping = mapping
        self.calls: list[str] = []

    async def get_friend_uuids(self, uuid: str, *, use_flaresolverr: bool = False) -> list[str]:
        self.calls.append(uuid)
        return self.mapping.get(uuid, [])


@pytest.mark.asyncio
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
        max_total_calls=10,
        max_friends_per_user=2,
    )

    assert mesh["a"] == ["b", "c"]
    # Depth=1 means we can fetch friends for b and c, but not for their friends.
    assert set(mesh.keys()).issubset({"a", "b", "c"})


@pytest.mark.asyncio
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
    )

    assert len(mesh) <= 2
    assert namemc.calls == ["a", "b"]
