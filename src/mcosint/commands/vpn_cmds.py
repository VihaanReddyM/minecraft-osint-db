from __future__ import annotations

import os
import time
from pathlib import Path

import typer

vpn_app = typer.Typer(add_completion=False, help="OpenVPN tunnel management (Linux only)")


@vpn_app.command("list-configs")
def vpn_list_configs(
    ovpn_dir: Path = typer.Argument(..., help="Directory containing .ovpn config files"),
) -> None:
    """List available .ovpn config files in a directory."""
    configs = sorted(ovpn_dir.glob("*.ovpn"))
    if not configs:
        typer.echo(f"No .ovpn files found in {ovpn_dir}")
        raise typer.Exit(1)
    for p in configs:
        typer.echo(str(p))
    typer.echo(f"\n{len(configs)} config(s) found.")


@vpn_app.command("start")
def vpn_start(
    ovpn_dir: Path = typer.Argument(..., help="Directory containing .ovpn config files"),
    count: int = typer.Option(1, "--count", "-n", help="Number of tunnels to start"),
    socks_port: int = typer.Option(1080, "--socks-port", help="microsocks listening port inside each namespace"),
) -> None:
    """Start N VPN tunnels (blocking — Ctrl+C to stop and clean up)."""
    from mcosint.proxy.vpn import VPNTunnelManager

    manager = VPNTunnelManager(ovpn_dir, socks_port=socks_port)
    tunnels = manager.start_all(count)
    running = sum(1 for t in tunnels if t.state == "running")
    failed = count - running

    typer.echo(f"\nStarted {running}/{count} tunnel(s).")
    if failed:
        typer.echo(f"Warning: {failed} tunnel(s) failed to connect — check logs in {ovpn_dir}/logs/")

    for row in manager.status_all():
        mark = "✓" if row["healthy"] else "✗"
        typer.echo(f"  [{mark}] tunnel-{row['index']}  {row['proxy']}  ({row['ovpn']})")

    if running == 0:
        raise typer.Exit(1)

    typer.echo("\nTunnels are running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        typer.echo("\nStopping tunnels...")
        manager.stop_all()
        typer.echo("Done.")


@vpn_app.command("status")
def vpn_status(
    state_file: Path = typer.Option(
        Path(os.path.expanduser("~/.mcosint/vpn_state.json")),
        "--state-file",
        help="Path to the VPN state file written by `vpn start`",
    ),
) -> None:
    """Show health of running tunnels (reads state written by `vpn start`)."""
    from mcosint.proxy.vpn import VPNTunnelManager

    if not state_file.exists():
        typer.echo(f"State file not found: {state_file}")
        typer.echo("Run `mcosint vpn start` first.")
        raise typer.Exit(1)

    manager = VPNTunnelManager.from_state_file(state_file)
    rows = manager.status_all()
    if not rows:
        typer.echo("No tunnels found in state file.")
        raise typer.Exit(1)

    typer.echo(f"{'#':<4} {'proxy':<28} {'state':<10} {'healthy':<8} ovpn")
    typer.echo("-" * 70)
    for row in rows:
        healthy_str = "yes" if row["healthy"] else "no"
        typer.echo(
            f"{row['index']:<4} {row['proxy']:<28} {row['state']:<10} {healthy_str:<8} {row['ovpn']}"
        )


@vpn_app.command("stop")
def vpn_stop(
    state_file: Path = typer.Option(
        Path(os.path.expanduser("~/.mcosint/vpn_state.json")),
        "--state-file",
        help="Path to the VPN state file written by `vpn start`",
    ),
) -> None:
    """Terminate all tracked tunnels and clean up namespaces."""
    from mcosint.proxy.vpn import VPNTunnelManager

    if not state_file.exists():
        typer.echo("No state file found — nothing to stop.")
        return

    manager = VPNTunnelManager.from_state_file(state_file)
    manager.stop_all()
    typer.echo("All tunnels stopped.")
