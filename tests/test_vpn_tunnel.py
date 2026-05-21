from __future__ import annotations

import json
import platform
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from mcosint.proxy.pool import ProxyConfig


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_tunnel(index: int = 0, state: str = "running"):
    """Build a VPNTunnel without going through VPNTunnelManager (no Linux required)."""
    from mcosint.proxy.vpn import VPNTunnel
    t = VPNTunnel(
        index=index,
        ovpn_path=Path(f"/vpn/config{index}.ovpn"),
        ns_name=f"mcosint-vpn-{index}",
        veth_host=f"mcosint-h{index}",
        veth_ns=f"mcosint-n{index}",
        host_ip=f"10.200.{index}.1",
        ns_ip=f"10.200.{index}.2",
        socks_port=1080,
        log_path=Path(f"/tmp/tunnel-{index}.log"),
        state=state,
        openvpn_pid=1000 + index,
        socks_pid=2000 + index,
    )
    return t


# ── VPNTunnel ─────────────────────────────────────────────────────────────────

class TestVPNTunnel:
    def test_proxy_config(self) -> None:
        t = _make_tunnel(3)
        p = t.proxy_config
        assert p.host == "10.200.3.2"
        assert p.port == 1080
        assert p.protocol == "socks5"

    def test_proxy_config_httpx_url(self) -> None:
        t = _make_tunnel(0)
        assert t.proxy_config.httpx_url() == "socks5://10.200.0.2:1080"

    def test_health_check_reachable(self) -> None:
        t = _make_tunnel(0)
        mock_sock = MagicMock()
        mock_sock.__enter__ = MagicMock(return_value=mock_sock)
        mock_sock.__exit__ = MagicMock(return_value=False)
        with patch("socket.create_connection", return_value=mock_sock):
            assert t.health_check() is True

    def test_health_check_unreachable(self) -> None:
        t = _make_tunnel(0)
        with patch("socket.create_connection", side_effect=OSError("connection refused")):
            assert t.health_check() is False

    def test_round_trip_serialisation(self, tmp_path: Path) -> None:
        t = _make_tunnel(2, state="running")
        d = t.to_dict()
        t2 = type(t).from_dict(d)
        assert t2.index == 2
        assert t2.state == "running"
        assert t2.ns_ip == "10.200.2.2"
        assert t2.ovpn_path == Path("/vpn/config2.ovpn")

    def test_state_file_json_round_trip(self, tmp_path: Path) -> None:
        tunnels = [_make_tunnel(i) for i in range(3)]
        data = {"tunnels": [t.to_dict() for t in tunnels]}
        state_file = tmp_path / "vpn_state.json"
        state_file.write_text(json.dumps(data))
        loaded = json.loads(state_file.read_text())
        assert len(loaded["tunnels"]) == 3
        assert loaded["tunnels"][1]["ns_ip"] == "10.200.1.2"


# ── VPNTunnelManager (platform-gated) ────────────────────────────────────────

class TestVPNTunnelManagerLayout:
    """Test the make_tunnel geometry without instantiating the manager (which requires Linux)."""

    def _manager(self, tmp_path: Path):
        from mcosint.proxy.vpn import VPNTunnelManager
        with patch("mcosint.proxy.vpn._require_linux"):
            mgr = VPNTunnelManager(tmp_path, socks_port=1080)
        return mgr

    def test_make_tunnel_ips(self, tmp_path: Path) -> None:
        mgr = self._manager(tmp_path)
        t = mgr._make_tunnel(5, Path("/ovpn/config5.ovpn"))
        assert t.host_ip == "10.200.5.1"
        assert t.ns_ip == "10.200.5.2"
        assert t.ns_name == "mcosint-vpn-5"
        assert t.veth_host == "mcosint-h5"
        assert t.veth_ns == "mcosint-n5"

    def test_make_tunnel_socks_port(self, tmp_path: Path) -> None:
        mgr = self._manager(tmp_path)
        t = mgr._make_tunnel(0, Path("/ovpn/config0.ovpn"))
        assert t.socks_port == 1080

    def test_list_configs_empty(self, tmp_path: Path) -> None:
        mgr = self._manager(tmp_path)
        assert mgr.list_configs() == []

    def test_list_configs_finds_ovpn(self, tmp_path: Path) -> None:
        (tmp_path / "a.ovpn").touch()
        (tmp_path / "b.ovpn").touch()
        (tmp_path / "readme.txt").touch()
        mgr = self._manager(tmp_path)
        names = [p.name for p in mgr.list_configs()]
        assert names == ["a.ovpn", "b.ovpn"]

    def test_start_all_raises_no_configs(self, tmp_path: Path) -> None:
        mgr = self._manager(tmp_path)
        with patch("mcosint.proxy.vpn._check_binaries"):
            with pytest.raises(RuntimeError, match="No .ovpn files"):
                mgr.start_all(1)

    def test_start_all_raises_count_exceeds_configs(self, tmp_path: Path) -> None:
        (tmp_path / "only.ovpn").touch()
        mgr = self._manager(tmp_path)
        with patch("mcosint.proxy.vpn._check_binaries"):
            with pytest.raises(RuntimeError, match="only 1"):
                mgr.start_all(3)

    def test_state_file_save_and_load(self, tmp_path: Path) -> None:
        from mcosint.proxy.vpn import VPNTunnelManager

        state_file = tmp_path / "state.json"
        with patch("mcosint.proxy.vpn._require_linux"):
            mgr = VPNTunnelManager(tmp_path, state_file=state_file)
        tunnels = [_make_tunnel(i) for i in range(2)]
        mgr._tunnels = tunnels
        mgr._save_state()
        assert state_file.exists()
        data = json.loads(state_file.read_text())
        assert len(data["tunnels"]) == 2

        with patch("mcosint.proxy.vpn._require_linux"):
            mgr2 = VPNTunnelManager.from_state_file(state_file)
        assert len(mgr2.tunnels) == 2
        assert mgr2.tunnels[0].ns_ip == "10.200.0.2"

    def test_remove_state(self, tmp_path: Path) -> None:
        from mcosint.proxy.vpn import VPNTunnelManager

        state_file = tmp_path / "state.json"
        state_file.write_text("{}")
        with patch("mcosint.proxy.vpn._require_linux"):
            mgr = VPNTunnelManager(tmp_path, state_file=state_file)
        mgr._remove_state()
        assert not state_file.exists()


# ── VPNProxyProvider ──────────────────────────────────────────────────────────

class TestVPNProxyProvider:
    def _provider(self, states: list[str]):
        from mcosint.proxy.vpn import VPNProxyProvider, VPNTunnelManager

        tunnels = [_make_tunnel(i, state=s) for i, s in enumerate(states)]
        with patch("mcosint.proxy.vpn._require_linux"):
            mgr = MagicMock(spec=VPNTunnelManager)
        mgr.tunnels = tunnels
        return VPNProxyProvider(mgr)

    def test_get_proxy_running_tunnel(self) -> None:
        prov = self._provider(["running"])
        p = prov.get_proxy(0)
        assert p is not None
        assert isinstance(p, ProxyConfig)
        assert p.host == "10.200.0.2"

    def test_get_proxy_non_running_returns_none(self) -> None:
        prov = self._provider(["error"])
        assert prov.get_proxy(0) is None

    def test_get_proxy_stopped_returns_none(self) -> None:
        prov = self._provider(["stopped"])
        assert prov.get_proxy(0) is None

    def test_get_tunnel_round_robin(self) -> None:
        prov = self._provider(["running", "running", "running"])
        assert prov.get_tunnel(0).index == 0
        assert prov.get_tunnel(1).index == 1
        assert prov.get_tunnel(2).index == 2
        assert prov.get_tunnel(3).index == 0  # wraps
        assert prov.get_tunnel(7).index == 1  # 7 % 3

    def test_get_tunnel_empty_returns_none(self) -> None:
        from mcosint.proxy.vpn import VPNProxyProvider, VPNTunnelManager
        with patch("mcosint.proxy.vpn._require_linux"):
            mgr = MagicMock(spec=VPNTunnelManager)
        mgr.tunnels = []
        prov = VPNProxyProvider(mgr)
        assert prov.get_tunnel(0) is None
        assert prov.get_proxy(0) is None

    def test_all_proxies_only_running(self) -> None:
        prov = self._provider(["running", "error", "running", "stopped"])
        proxies = prov.all_proxies()
        assert len(proxies) == 2

    def test_manager_property(self) -> None:
        from mcosint.proxy.vpn import VPNProxyProvider, VPNTunnelManager
        with patch("mcosint.proxy.vpn._require_linux"):
            mgr = MagicMock(spec=VPNTunnelManager)
        mgr.tunnels = []
        prov = VPNProxyProvider(mgr)
        assert prov.manager is mgr


# ── Platform guard ────────────────────────────────────────────────────────────

class TestPlatformGuard:
    def test_require_linux_raises_on_non_linux(self) -> None:
        from mcosint.proxy.vpn import _require_linux
        with patch("mcosint.proxy.vpn._is_linux", return_value=False):
            with pytest.raises(RuntimeError, match="Linux"):
                _require_linux()

    def test_require_linux_passes_on_linux(self) -> None:
        from mcosint.proxy.vpn import _require_linux
        with patch("mcosint.proxy.vpn._is_linux", return_value=True):
            _require_linux()  # should not raise

    def test_check_binaries_raises_missing(self) -> None:
        from mcosint.proxy.vpn import _check_binaries
        with patch("shutil.which", return_value=None):
            with pytest.raises(RuntimeError, match="Missing required binaries"):
                _check_binaries("openvpn", "microsocks")

    def test_check_binaries_passes_when_found(self) -> None:
        from mcosint.proxy.vpn import _check_binaries
        with patch("shutil.which", return_value="/usr/bin/openvpn"):
            _check_binaries("openvpn")  # should not raise
