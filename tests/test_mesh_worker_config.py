from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from mcosint.workers.namemc_friends_mesh_worker import (
    WorkerConfig,
    _build_proxy_pool,
    load_worker_config_from_env,
)


def _base_env(tmp_path: Path) -> dict[str, str]:
    """Minimum env required for load_worker_config_from_env to succeed."""
    seeds = tmp_path / "seeds.txt"
    seeds.write_text("00000000-0000-0000-0000-000000000001\n")
    return {"UUID_LIST_FILE_PATH": str(seeds)}


class TestBurstDefaults:
    """Phase 5a bug #3: ensure BurstConfig defaults align with .env.example."""

    def test_default_min_api_wait_time_is_15(self, tmp_path: Path) -> None:
        env = _base_env(tmp_path)
        with (
            patch("mcosint.workers.namemc_friends_mesh_worker.load_dotenv"),
            patch.dict("os.environ", env, clear=True),
        ):
            cfg = load_worker_config_from_env()
        assert cfg.burst.min_api_wait_time == 15.0

    def test_default_max_api_wait_time_is_45(self, tmp_path: Path) -> None:
        env = _base_env(tmp_path)
        with (
            patch("mcosint.workers.namemc_friends_mesh_worker.load_dotenv"),
            patch.dict("os.environ", env, clear=True),
        ):
            cfg = load_worker_config_from_env()
        assert cfg.burst.max_api_wait_time == 45.0

    def test_default_min_burst_calls_is_3(self, tmp_path: Path) -> None:
        env = _base_env(tmp_path)
        with (
            patch("mcosint.workers.namemc_friends_mesh_worker.load_dotenv"),
            patch.dict("os.environ", env, clear=True),
        ):
            cfg = load_worker_config_from_env()
        assert cfg.burst.min_burst_calls == 3

    def test_default_max_burst_calls_is_8(self, tmp_path: Path) -> None:
        env = _base_env(tmp_path)
        with (
            patch("mcosint.workers.namemc_friends_mesh_worker.load_dotenv"),
            patch.dict("os.environ", env, clear=True),
        ):
            cfg = load_worker_config_from_env()
        assert cfg.burst.max_burst_calls == 8

    def test_env_overrides_defaults(self, tmp_path: Path) -> None:
        env = _base_env(tmp_path)
        env.update(
            {
                "MIN_API_WAIT_TIME": "1.0",
                "MAX_API_WAIT_TIME": "2.0",
                "MIN_BURST_CALLS": "5",
                "MAX_BURST_CALLS": "10",
            }
        )
        with (
            patch("mcosint.workers.namemc_friends_mesh_worker.load_dotenv"),
            patch.dict("os.environ", env, clear=True),
        ):
            cfg = load_worker_config_from_env()
        assert cfg.burst.min_api_wait_time == 1.0
        assert cfg.burst.max_api_wait_time == 2.0
        assert cfg.burst.min_burst_calls == 5
        assert cfg.burst.max_burst_calls == 10


class TestProxyEnvConfig:
    """Phase 5a bug #2: friends-mesh reads PROXY_FILE_PATH / VPN_DIR / VPN_COUNT."""

    def test_no_proxy_env_defaults_to_none(self, tmp_path: Path) -> None:
        env = _base_env(tmp_path)
        with (
            patch("mcosint.workers.namemc_friends_mesh_worker.load_dotenv"),
            patch.dict("os.environ", env, clear=True),
        ):
            cfg = load_worker_config_from_env()
        assert cfg.proxy_file_path is None
        assert cfg.vpn_dir is None
        assert cfg.vpn_count is None

    def test_proxy_file_path_env_loaded(self, tmp_path: Path) -> None:
        env = _base_env(tmp_path)
        proxy_file = tmp_path / "proxies.txt"
        env["PROXY_FILE_PATH"] = str(proxy_file)
        with (
            patch("mcosint.workers.namemc_friends_mesh_worker.load_dotenv"),
            patch.dict("os.environ", env, clear=True),
        ):
            cfg = load_worker_config_from_env()
        assert cfg.proxy_file_path == proxy_file

    def test_vpn_dir_and_count_env_loaded(self, tmp_path: Path) -> None:
        env = _base_env(tmp_path)
        vpn_dir = tmp_path / "vpn"
        env["VPN_DIR"] = str(vpn_dir)
        env["VPN_COUNT"] = "3"
        with (
            patch("mcosint.workers.namemc_friends_mesh_worker.load_dotenv"),
            patch.dict("os.environ", env, clear=True),
        ):
            cfg = load_worker_config_from_env()
        assert cfg.vpn_dir == vpn_dir
        assert cfg.vpn_count == 3


def _wcfg(**overrides) -> WorkerConfig:
    """Build a WorkerConfig with sensible defaults for testing _build_proxy_pool."""
    from mcosint.workers.namemc_friends_mesh_worker import BurstConfig

    defaults = dict(
        uuid_list_file_path=Path("/tmp/seeds.txt"),
        max_depth=2,
        max_total_calls=None,
        max_friends_per_user=None,
        output_path=Path("/tmp/out.json"),
        use_flaresolverr=False,
        flaresolverr_url=None,
        init_db=False,
        threads=5,
        rate_limit_backoff_seconds=10.0,
        max_retries_per_uuid=3,
        discord_webhook_url=None,
        burst=BurstConfig(15.0, 45.0, 3, 8),
        proxy_file_path=None,
        vpn_dir=None,
        vpn_count=None,
    )
    defaults.update(overrides)
    return WorkerConfig(**defaults)


class TestBuildProxyPool:
    def test_no_config_returns_none(self) -> None:
        pool, mgr = _build_proxy_pool(_wcfg())
        assert pool is None
        assert mgr is None

    def test_proxy_file_returns_static_pool(self, tmp_path: Path) -> None:
        from mcosint.proxy.pool import StaticProxyPool

        f = tmp_path / "proxies.txt"
        f.write_text("socks5://127.0.0.1:1080\nsocks5://127.0.0.1:1081\n")
        pool, mgr = _build_proxy_pool(_wcfg(proxy_file_path=f))
        assert isinstance(pool, StaticProxyPool)
        assert len(pool.all_proxies()) == 2
        assert mgr is None

    def test_empty_proxy_file_returns_none(self, tmp_path: Path) -> None:
        f = tmp_path / "proxies.txt"
        f.write_text("# all comments\n\n")
        pool, mgr = _build_proxy_pool(_wcfg(proxy_file_path=f))
        assert pool is None
        assert mgr is None

    def test_vpn_dir_takes_precedence_over_proxy_file(self, tmp_path: Path) -> None:
        from mcosint.proxy.vpn import VPNProxyProvider

        f = tmp_path / "proxies.txt"
        f.write_text("socks5://127.0.0.1:1080\n")
        vpn_dir = tmp_path / "vpn"
        vpn_dir.mkdir()

        # Patch VPNTunnelManager.start_all so we don't actually touch netns / openvpn.
        with patch(
            "mcosint.proxy.vpn.VPNTunnelManager.start_all", return_value=[]
        ), patch("mcosint.proxy.vpn._require_linux"):
            pool, mgr = _build_proxy_pool(
                _wcfg(proxy_file_path=f, vpn_dir=vpn_dir, vpn_count=2)
            )
        assert isinstance(pool, VPNProxyProvider)
        assert mgr is not None
