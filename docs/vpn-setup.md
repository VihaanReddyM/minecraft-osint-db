# VPN Tunnel Setup Guide

`mcosint vpn` manages OpenVPN tunnels for per-worker IP isolation. Each crawl worker is
assigned its own tunnel and SOCKS5 proxy, so rate limits on one IP don't block others.

## Requirements

- **Linux only** (Ubuntu 22.04+ recommended). The VPN module uses `ip netns` (network
  namespaces), which is a Linux kernel feature. Windows and macOS are not supported.
- Root or `CAP_NET_ADMIN` capability (needed for `ip netns`, `ip link`).
- `openvpn` >= 2.5
- `microsocks` — lightweight SOCKS5 proxy daemon

### Install dependencies

```bash
# Ubuntu / Debian
sudo apt update
sudo apt install -y openvpn

# microsocks (from apt on Ubuntu 22.04+)
sudo apt install -y microsocks

# OR build from source (any Linux)
git clone https://github.com/rofl0r/microsocks
cd microsocks && make && sudo cp microsocks /usr/local/bin/
```

## Architecture

```
  Host machine
  +------------------------------------------------------+
  |                                                      |
  |   Worker 0 ---- socks5://10.200.0.2:1080 ----+      |
  |   Worker 1 ---- socks5://10.200.1.2:1080 --+ |      |
  |   Worker 2 ---- socks5://10.200.2.2:1080 -+| |      |
  |                                            || |      |
  |  Namespace mcosint-vpn-2  ----------------+| |      |
  |  +-----------------------------------------+| |      |
  |  | microsocks <- 10.200.2.2                || |      |
  |  | openvpn --> VPN server C                || |      |
  |  +-----------------------------------------+| |      |
  |                                             | |      |
  |  Namespace mcosint-vpn-1  -----------------+ |      |
  |  +-----------------------------------------+  |      |
  |  | microsocks <- 10.200.1.2                |  |      |
  |  | openvpn --> VPN server B                |  |      |
  |  +-----------------------------------------+  |      |
  |                                               |      |
  |  Namespace mcosint-vpn-0  --------------------+      |
  |  +-----------------------------------------+         |
  |  | microsocks <- 10.200.0.2                |         |
  |  | openvpn --> VPN server A                |         |
  |  +-----------------------------------------+         |
  +------------------------------------------------------+
```

Each namespace is isolated. OpenVPN inside the namespace redirects all outbound traffic
through the tunnel. microsocks proxies SOCKS5 connections from the worker into the namespace.

## Preparing .ovpn configs

Each tunnel needs its own `.ovpn` file. Place them in a directory:

```
vpn/
  server-a.ovpn
  server-b.ovpn
  server-c.ovpn
```

Your `.ovpn` files should include `redirect-gateway def1` so all namespace traffic routes
through the VPN. Example minimal config:

```
client
dev tun
proto udp
remote vpn.example.com 1194
redirect-gateway def1
ca ca.crt
cert client.crt
key client.key
```

If your provider requires credentials, add:

```
auth-user-pass /path/to/credentials.txt
```

## Usage

### Standalone tunnel management

```bash
# List available configs
mcosint vpn list-configs ./vpn/

# Start 3 tunnels (blocking -- Ctrl+C to stop)
sudo mcosint vpn start ./vpn/ --count 3

# In another terminal: check tunnel health
mcosint vpn status

# Force-stop tunnels started in the background
sudo mcosint vpn stop
```

### Integrated with crawl commands

```bash
# Start crawl -- VPN tunnels start/stop automatically
sudo mcosint namemc crawl-friends-db <UUID> \
  --vpn-dir ./vpn/ \
  --vpn-count 3 \
  --threads 3 \
  --max-depth 2 \
  --init-db

# friends-mesh with VPN
sudo mcosint namemc friends-mesh <UUID> \
  --vpn-dir ./vpn/ \
  --vpn-count 5 \
  --threads 5
```

When `--vpn-dir` is used, `--proxy` and `--proxy-file` are ignored (VPN takes precedence).

## Tunnel lifecycle during crawl

1. Before crawl: `VPNTunnelManager.start_all(N)` sets up N namespaces, starts OpenVPN +
   microsocks in each, waits up to 45 seconds per tunnel for VPN connection.
2. During crawl: each worker is assigned a tunnel by index (round-robin). All HTTP
   requests go through that worker's SOCKS5 proxy.
3. On transport error: the worker checks if its tunnel is healthy. If the tunnel is down,
   it calls `restart_tunnel()` automatically, recreates its HTTP client, and retries.
4. After crawl (or on Ctrl+C): `stop_all()` gracefully terminates all processes and
   deletes all network namespaces.

## Troubleshooting

**`RuntimeError: Missing required binaries: microsocks`**
Install microsocks -- see Installation section above.

**`RuntimeError: VPN tunnel management requires Linux`**
You're on Windows/macOS. Phase 2 is Linux-only. Use `--proxy` instead for manual SOCKS5.

**Tunnel stuck in `starting` state**
Check the log: `cat ./vpn/logs/tunnel-0.log`. Common causes:
- `AUTH_FAILED` -- wrong credentials in `auth-user-pass`
- `TLS Error` -- certificate issues
- `Cannot resolve host address` -- DNS not working in the namespace

**`ip: command not found`**
Install `iproute2`: `sudo apt install iproute2`

**Permission denied**
Run with `sudo` or grant `CAP_NET_ADMIN`:
```bash
sudo setcap cap_net_admin+eip $(which mcosint)
```

## Network addressing

Each tunnel N uses the `/30` subnet `10.200.N.0/30`:
- Host-side veth IP: `10.200.N.1`
- Namespace-side veth IP: `10.200.N.2` (microsocks binds here)

This supports up to 256 tunnels (`10.200.0.x` through `10.200.255.x`).

Interface names (must be <=15 chars):
- `mcosint-hN` -- host-side veth
- `mcosint-nN` -- namespace-side veth
- Namespace: `mcosint-vpn-N`
