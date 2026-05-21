from __future__ import annotations

import json
import logging
import os
import platform
import shutil
import signal
import socket
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mcosint.proxy.pool import ProxyConfig

log = logging.getLogger(__name__)

_OPENVPN_READY = "Initialization Sequence Completed"
_OPENVPN_FAIL_PATTERNS = ("AUTH_FAILED", "TLS Error", "Cannot resolve host address")


def _is_linux() -> bool:
    return platform.system() == "Linux"


def _require_linux(feature: str = "VPN tunnel management") -> None:
    if not _is_linux():
        raise RuntimeError(
            f"{feature} requires Linux (ip netns, openvpn, microsocks). "
            f"Current platform: {platform.system()}. "
            "Run on a Linux VM — see docs/vpn-setup.md."
        )


def _check_binaries(*names: str) -> None:
    missing = [n for n in names if shutil.which(n) is None]
    if missing:
        raise RuntimeError(
            f"Missing required binaries: {', '.join(missing)}. "
            "Install them before using VPN tunnels — see docs/vpn-setup.md."
        )


def _run(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(list(args), check=check, capture_output=True, text=True)


def _kill_pid(pid: int | None, sig: int = signal.SIGTERM) -> None:
    if pid is None:
        return
    try:
        os.kill(pid, sig)
    except (ProcessLookupError, OSError):
        pass


@dataclass
class VPNTunnel:
    """One OpenVPN tunnel inside a Linux network namespace with a co-located SOCKS5 proxy.

    Architecture per tunnel N:
      - Network namespace: mcosint-vpn-N
      - veth pair: mcosint-hN (host) ↔ mcosint-nN (namespace)
      - Host IP: 10.200.N.1/30   Namespace IP: 10.200.N.2/30
      - OpenVPN runs inside the namespace (routes all namespace traffic through VPN)
      - microsocks runs inside the namespace, bound to 10.200.N.2:socks_port
      - Workers connect to socks5://10.200.N.2:socks_port
    """

    index: int
    ovpn_path: Path
    ns_name: str        # e.g. "mcosint-vpn-0"
    veth_host: str      # host-side veth, e.g. "mcosint-h0"
    veth_ns: str        # namespace-side veth, e.g. "mcosint-n0"
    host_ip: str        # host-side IP, e.g. "10.200.0.1"
    ns_ip: str          # namespace IP + microsocks bind IP, e.g. "10.200.0.2"
    socks_port: int
    log_path: Path      # OpenVPN stdout log

    state: str = "stopped"           # stopped | starting | running | error
    openvpn_pid: int | None = None
    socks_pid: int | None = None

    # Subprocess handles — excluded from repr / comparison / __init__
    _openvpn_proc: Any = field(default=None, repr=False, compare=False, init=False)
    _socks_proc: Any = field(default=None, repr=False, compare=False, init=False)

    @property
    def proxy_config(self) -> ProxyConfig:
        return ProxyConfig(host=self.ns_ip, port=self.socks_port, protocol="socks5")

    def health_check(self) -> bool:
        """TCP probe to the SOCKS5 port. Returns True if microsocks is reachable."""
        try:
            with socket.create_connection((self.ns_ip, self.socks_port), timeout=3.0):
                return True
        except OSError:
            return False

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "ovpn_path": str(self.ovpn_path),
            "ns_name": self.ns_name,
            "veth_host": self.veth_host,
            "veth_ns": self.veth_ns,
            "host_ip": self.host_ip,
            "ns_ip": self.ns_ip,
            "socks_port": self.socks_port,
            "log_path": str(self.log_path),
            "state": self.state,
            "openvpn_pid": self.openvpn_pid,
            "socks_pid": self.socks_pid,
        }

    @classmethod
    def from_dict(cls, d: dict) -> VPNTunnel:
        t = cls(
            index=d["index"],
            ovpn_path=Path(d["ovpn_path"]),
            ns_name=d["ns_name"],
            veth_host=d["veth_host"],
            veth_ns=d["veth_ns"],
            host_ip=d["host_ip"],
            ns_ip=d["ns_ip"],
            socks_port=d["socks_port"],
            log_path=Path(d["log_path"]),
            state=d.get("state", "stopped"),
            openvpn_pid=d.get("openvpn_pid"),
            socks_pid=d.get("socks_pid"),
        )
        return t


class VPNTunnelManager:
    """Manages N OpenVPN tunnels, each inside its own Linux network namespace.

    Concurrency note: start_tunnel / stop_tunnel are blocking and designed to be called
    from the main thread (or at most one caller per tunnel). The `tunnels` property is
    thread-safe (returns a snapshot copy under a lock).
    """

    _NS_PREFIX = "mcosint-vpn-"
    _VETH_HOST_PREFIX = "mcosint-h"
    _VETH_NS_PREFIX = "mcosint-n"
    _STARTUP_TIMEOUT = 45.0

    def __init__(
        self,
        ovpn_dir: Path,
        socks_port: int = 1080,
        log_dir: Path | None = None,
        state_file: Path | None = None,
    ) -> None:
        _require_linux()
        self._ovpn_dir = Path(ovpn_dir)
        self._socks_port = socks_port
        self._log_dir = log_dir or (self._ovpn_dir / "logs")
        self._state_file = state_file or Path(
            os.path.expanduser("~/.mcosint/vpn_state.json")
        )
        self._tunnels: list[VPNTunnel] = []
        self._lock = threading.Lock()

    # ── Public API ────────────────────────────────────────────────────────────

    def list_configs(self) -> list[Path]:
        """Return sorted list of .ovpn files in the configured directory."""
        return sorted(self._ovpn_dir.glob("*.ovpn"))

    def start_all(self, count: int) -> list[VPNTunnel]:
        """Create and start `count` tunnels (one .ovpn config each). Returns all tunnels."""
        configs = self.list_configs()
        if not configs:
            raise RuntimeError(f"No .ovpn files found in {self._ovpn_dir}")
        if count > len(configs):
            raise RuntimeError(
                f"Requested {count} tunnels but only {len(configs)} .ovpn configs "
                f"available in {self._ovpn_dir}"
            )
        _check_binaries("openvpn", "microsocks", "ip")
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._state_file.parent.mkdir(parents=True, exist_ok=True)

        tunnels = [self._make_tunnel(i, configs[i]) for i in range(count)]
        with self._lock:
            self._tunnels = tunnels

        for tunnel in tunnels:
            self.start_tunnel(tunnel)

        self._save_state()
        return tunnels

    def start_tunnel(self, tunnel: VPNTunnel) -> bool:
        """Set up namespace, veth, OpenVPN, microsocks. Returns True on success."""
        tunnel.state = "starting"
        log.info("Starting tunnel %d (%s)", tunnel.index, tunnel.ovpn_path.name)
        try:
            self._setup_namespace(tunnel)
            self._launch_openvpn(tunnel)
            if not self._wait_for_openvpn(tunnel):
                log.error(
                    "Tunnel %d: OpenVPN did not connect within %.0fs (log: %s)",
                    tunnel.index,
                    self._STARTUP_TIMEOUT,
                    tunnel.log_path,
                )
                tunnel.state = "error"
                return False
            self._launch_microsocks(tunnel)
            time.sleep(0.5)
            if not tunnel.health_check():
                log.error("Tunnel %d: SOCKS5 proxy unreachable after start", tunnel.index)
                tunnel.state = "error"
                return False
            tunnel.state = "running"
            log.info(
                "Tunnel %d ready — socks5://%s:%s", tunnel.index, tunnel.ns_ip, tunnel.socks_port
            )
            return True
        except Exception:
            tunnel.state = "error"
            log.exception("Tunnel %d start failed", tunnel.index)
            return False

    def stop_tunnel(self, tunnel: VPNTunnel) -> None:
        """Gracefully stop microsocks + OpenVPN, then tear down the namespace."""
        log.info("Stopping tunnel %d", tunnel.index)
        _terminate(tunnel._socks_proc, tunnel.socks_pid, timeout=3.0)
        tunnel._socks_proc = None
        tunnel.socks_pid = None

        _terminate(tunnel._openvpn_proc, tunnel.openvpn_pid, timeout=5.0)
        tunnel._openvpn_proc = None
        tunnel.openvpn_pid = None

        _run("ip", "link", "del", tunnel.veth_host, check=False)
        _run("ip", "netns", "del", tunnel.ns_name, check=False)
        tunnel.state = "stopped"

    def stop_all(self) -> None:
        with self._lock:
            tunnels = list(self._tunnels)
        for tunnel in tunnels:
            self.stop_tunnel(tunnel)
        self._remove_state()

    def restart_tunnel(self, tunnel: VPNTunnel) -> bool:
        """Stop and restart a single tunnel. Returns True if the restart succeeded."""
        self.stop_tunnel(tunnel)
        ok = self.start_tunnel(tunnel)
        self._save_state()
        return ok

    def status_all(self) -> list[dict]:
        with self._lock:
            tunnels = list(self._tunnels)
        return [
            {
                "index": t.index,
                "ovpn": t.ovpn_path.name,
                "ns_name": t.ns_name,
                "proxy": f"socks5://{t.ns_ip}:{t.socks_port}",
                "state": t.state,
                "healthy": t.health_check() if t.state == "running" else False,
                "openvpn_pid": t.openvpn_pid,
                "socks_pid": t.socks_pid,
            }
            for t in tunnels
        ]

    @property
    def tunnels(self) -> list[VPNTunnel]:
        with self._lock:
            return list(self._tunnels)

    # ── State persistence ─────────────────────────────────────────────────────

    def _save_state(self) -> None:
        with self._lock:
            data = {"tunnels": [t.to_dict() for t in self._tunnels]}
        try:
            self._state_file.write_text(json.dumps(data, indent=2))
        except OSError:
            log.debug("Could not write VPN state file %s", self._state_file)

    def _remove_state(self) -> None:
        try:
            self._state_file.unlink(missing_ok=True)
        except OSError:
            pass

    @classmethod
    def from_state_file(cls, state_file: Path) -> VPNTunnelManager:
        """Reconstruct a manager from a saved state file (for `vpn status` / `vpn stop`)."""
        _require_linux()
        data = json.loads(state_file.read_text())
        tunnels = [VPNTunnel.from_dict(d) for d in data.get("tunnels", [])]
        if not tunnels:
            raise RuntimeError(f"No tunnels found in state file: {state_file}")
        mgr = object.__new__(cls)
        mgr._ovpn_dir = tunnels[0].ovpn_path.parent
        mgr._socks_port = tunnels[0].socks_port
        mgr._log_dir = tunnels[0].log_path.parent
        mgr._state_file = state_file
        mgr._lock = threading.Lock()
        mgr._tunnels = tunnels
        return mgr

    # ── Internals ─────────────────────────────────────────────────────────────

    def _make_tunnel(self, index: int, ovpn_path: Path) -> VPNTunnel:
        return VPNTunnel(
            index=index,
            ovpn_path=ovpn_path,
            ns_name=f"{self._NS_PREFIX}{index}",
            veth_host=f"{self._VETH_HOST_PREFIX}{index}",
            veth_ns=f"{self._VETH_NS_PREFIX}{index}",
            host_ip=f"10.200.{index}.1",
            ns_ip=f"10.200.{index}.2",
            socks_port=self._socks_port,
            log_path=self._log_dir / f"tunnel-{index}.log",
        )

    def _setup_namespace(self, tunnel: VPNTunnel) -> None:
        # Idempotent: clean up any previous run before recreating.
        _run("ip", "link", "del", tunnel.veth_host, check=False)
        _run("ip", "netns", "del", tunnel.ns_name, check=False)

        _run("ip", "netns", "add", tunnel.ns_name)
        _run("ip", "link", "add", tunnel.veth_host,
             "type", "veth", "peer", "name", tunnel.veth_ns)
        _run("ip", "link", "set", tunnel.veth_ns, "netns", tunnel.ns_name)

        # Host side
        _run("ip", "addr", "add", f"{tunnel.host_ip}/30", "dev", tunnel.veth_host)
        _run("ip", "link", "set", tunnel.veth_host, "up")

        # Namespace side
        _run("ip", "netns", "exec", tunnel.ns_name,
             "ip", "addr", "add", f"{tunnel.ns_ip}/30", "dev", tunnel.veth_ns)
        _run("ip", "netns", "exec", tunnel.ns_name,
             "ip", "link", "set", tunnel.veth_ns, "up")
        _run("ip", "netns", "exec", tunnel.ns_name,
             "ip", "link", "set", "lo", "up")
        # Pre-VPN default route (OpenVPN will replace this with the tunnel route)
        _run("ip", "netns", "exec", tunnel.ns_name,
             "ip", "route", "add", "default", "via", tunnel.host_ip)
        # Enable IP forwarding so the namespace can reach the internet via the host veth
        _run("sysctl", "-w", "net.ipv4.ip_forward=1", check=False)

    def _launch_openvpn(self, tunnel: VPNTunnel) -> None:
        log_file = open(tunnel.log_path, "w")  # noqa: WPS515
        proc = subprocess.Popen(
            ["ip", "netns", "exec", tunnel.ns_name,
             "openvpn", "--config", str(tunnel.ovpn_path),
             "--script-security", "2",
             "--verb", "3"],
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        tunnel._openvpn_proc = proc
        tunnel.openvpn_pid = proc.pid

    def _wait_for_openvpn(self, tunnel: VPNTunnel) -> bool:
        deadline = time.monotonic() + self._STARTUP_TIMEOUT
        while time.monotonic() < deadline:
            try:
                text = tunnel.log_path.read_text(errors="replace")
            except OSError:
                text = ""
            if _OPENVPN_READY in text:
                return True
            if any(p in text for p in _OPENVPN_FAIL_PATTERNS):
                return False
            if tunnel._openvpn_proc is not None and tunnel._openvpn_proc.poll() is not None:
                return False
            time.sleep(1.0)
        return False

    def _launch_microsocks(self, tunnel: VPNTunnel) -> None:
        proc = subprocess.Popen(
            ["ip", "netns", "exec", tunnel.ns_name,
             "microsocks", "-i", tunnel.ns_ip, "-p", str(tunnel.socks_port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        tunnel._socks_proc = proc
        tunnel.socks_pid = proc.pid


def _terminate(
    proc: Any,
    fallback_pid: int | None,
    timeout: float,
) -> None:
    """Gracefully terminate a subprocess: SIGTERM → wait → SIGKILL if needed."""
    if proc is not None:
        proc.terminate()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    elif fallback_pid is not None:
        _kill_pid(fallback_pid, signal.SIGTERM)
        time.sleep(min(timeout, 1.0))


class VPNProxyProvider:
    """ProxyProvider that maps each worker to a VPN tunnel's SOCKS5 proxy (round-robin).

    Exposes `get_tunnel(worker_id)` for workers that need to trigger a tunnel restart.
    Exposes `manager` so workers can call `manager.restart_tunnel(tunnel)`.
    """

    def __init__(self, manager: VPNTunnelManager) -> None:
        self._manager = manager

    @property
    def manager(self) -> VPNTunnelManager:
        return self._manager

    def get_proxy(self, worker_id: int) -> ProxyConfig | None:
        tunnel = self.get_tunnel(worker_id)
        if tunnel is None or tunnel.state != "running":
            return None
        return tunnel.proxy_config

    def get_tunnel(self, worker_id: int) -> VPNTunnel | None:
        tunnels = self._manager.tunnels
        if not tunnels:
            return None
        return tunnels[worker_id % len(tunnels)]

    def all_proxies(self) -> list[ProxyConfig]:
        return [t.proxy_config for t in self._manager.tunnels if t.state == "running"]
