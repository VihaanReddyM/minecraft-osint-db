"""Tests for the iptables MASQUERADE setup added in Phase 5a bug #1."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from mcosint.proxy.vpn import (
    _MASQUERADE_SUBNET,
    _detect_default_interface,
    _ensure_masquerade,
)


class TestDetectDefaultInterface:
    def test_parses_dev_token_from_ip_output(self) -> None:
        result = MagicMock(returncode=0)
        result.stdout = (
            "8.8.8.8 via 192.168.1.1 dev eth0 src 192.168.1.42 uid 0 \\\\    cache\n"
        )
        with patch("subprocess.run", return_value=result):
            assert _detect_default_interface() == "eth0"

    def test_returns_none_when_no_dev_in_output(self) -> None:
        result = MagicMock(returncode=0)
        result.stdout = "8.8.8.8 unreachable\n"
        with patch("subprocess.run", return_value=result):
            assert _detect_default_interface() is None

    def test_returns_none_on_ip_failure(self) -> None:
        import subprocess

        with patch(
            "subprocess.run",
            side_effect=subprocess.CalledProcessError(1, ["ip"]),
        ):
            assert _detect_default_interface() is None

    def test_returns_none_when_ip_not_installed(self) -> None:
        with patch("subprocess.run", side_effect=FileNotFoundError):
            assert _detect_default_interface() is None


class TestEnsureMasquerade:
    def test_skips_setup_when_no_iface_detected(self) -> None:
        with patch("mcosint.proxy.vpn._detect_default_interface", return_value=None):
            assert _ensure_masquerade(iface=None) is False

    def test_rule_present_is_idempotent(self) -> None:
        # iptables -C returns 0 if the rule already exists; we shouldn't try to add.
        check_result = MagicMock(returncode=0)
        with patch("subprocess.run", return_value=check_result) as run:
            assert _ensure_masquerade(iface="eth0") is True
            # Only one call (the -C check), no -A add.
            assert run.call_count == 1
            args = run.call_args.args[0]
            assert "-C" in args

    def test_rule_added_when_missing(self) -> None:
        # First call: -C returns nonzero (rule missing). Second: -A succeeds.
        check_result = MagicMock(returncode=1)
        # _run inside the module wraps subprocess.run with check=True, capture_output=True;
        # we patch the public subprocess.run to control both.
        with patch("subprocess.run") as run:
            run.return_value = check_result
            run.side_effect = [check_result, MagicMock(returncode=0)]
            assert _ensure_masquerade(iface="eth0") is True
            assert run.call_count == 2
            add_args = run.call_args_list[1].args[0]
            assert "-A" in add_args
            assert _MASQUERADE_SUBNET in add_args
            assert "eth0" in add_args

    def test_subnet_is_class_b(self) -> None:
        # Sanity: the documented subnet matches what the manager passes around.
        assert _MASQUERADE_SUBNET == "10.200.0.0/16"
