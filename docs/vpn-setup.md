# VPN Tunnel Setup Guide (Linux only)

`mcosint vpn` manages one OpenVPN tunnel per `.ovpn` config file, each inside
its own Linux network namespace with a colocated `microsocks` SOCKS5 proxy.
Each crawler worker thread is assigned a tunnel by index, so rate limits on
one IP don't pause the others.

---

## Requirements

- **Linux only** (Ubuntu 22.04+ recommended). Uses `ip netns`, `ip link`,
  `openvpn`, `microsocks`, `iptables`. Windows / macOS are not supported.
- Root or `CAP_NET_ADMIN` (needed for namespace + veth manipulation).
- `openvpn >= 2.5`
- `microsocks` (lightweight SOCKS5 daemon).
- `iproute2` (provides `ip`).
- `iptables` (for the host MASQUERADE rule — **you must add this manually**;
  the manager does not install it. Tracked as bug #1 in
  `docs/ARCHITECTURE.md`.)

### Install dependencies

```bash
sudo apt update
sudo apt install -y openvpn microsocks iproute2 iptables

# OR build microsocks from source on older distros:
git clone https://github.com/rofl0r/microsocks
cd microsocks && make && sudo cp microsocks /usr/local/bin/
```

### Host iptables NAT rule (automated as of Phase 5a)

Each tunnel namespace `mcosint-vpn-N` uses subnet `10.200.N.0/30`. The host
must MASQUERADE namespace traffic out its main interface so that OpenVPN
inside the namespace can reach the VPN server (the control-channel packets
need a real source IP, not the namespace's private `10.200.N.2`).

As of Phase 5a, `VPNTunnelManager.start_all()` does this automatically:

1. Auto-detects the host's default outbound interface via
   `ip -o route get 8.8.8.8` (parses the `dev <iface>` token).
2. Checks whether the rule already exists with `iptables -t nat -C`.
3. If missing, installs:
   ```
   iptables -t nat -A POSTROUTING -s 10.200.0.0/16 -o <iface> -j MASQUERADE
   ```
4. The rule is intentionally **not removed** by `stop_all()` so subsequent
   sessions don't have a "no NAT" window. If the operator wants to clean up
   manually:
   ```bash
   sudo iptables -t nat -D POSTROUTING -s 10.200.0.0/16 -o <iface> -j MASQUERADE
   ```

If detection fails (e.g. `ip` not installed, no default route, exotic
networking setup), a warning is logged and the rule must be added manually
with the same command. The tunnels will not start successfully without it.

---

## Architecture

```
  Host machine
  +------------------------------------------------------+
  |   Worker 0 ── socks5://10.200.0.2:1080 ──┐           |
  |   Worker 1 ── socks5://10.200.1.2:1080 ──┼─┐         |
  |   Worker 2 ── socks5://10.200.2.2:1080 ──┘ │         |
  |                                            │         |
  |  Namespace mcosint-vpn-2  ─────────────────┘         |
  |  ┌────────────────────────────────────┐              |
  |  │ microsocks  ← 10.200.2.2:1080      │              |
  |  │ openvpn    → VPN server C          │              |
  |  └────────────────────────────────────┘              |
  |  Namespace mcosint-vpn-1                              |
  |  Namespace mcosint-vpn-0                              |
  +------------------------------------------------------+
```

Each namespace gets:

- A veth pair (`mcosint-hN` on host ↔ `mcosint-nN` in namespace).
- Host IP `10.200.N.1/30`, namespace IP `10.200.N.2/30`.
- OpenVPN inside the namespace, set to `redirect-gateway def1` so all
  namespace traffic exits through the VPN.
- `microsocks` bound to `10.200.N.2:1080` inside the namespace.

Worker threads on the host connect to `socks5://10.200.N.2:1080`.

---

## Preparing .ovpn configs

Place one `.ovpn` file per tunnel in a directory:

```
vpn/
  server-a.ovpn
  server-b.ovpn
  server-c.ovpn
```

Minimum required directives:

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

If the provider needs credentials:

```
auth-user-pass /path/to/credentials.txt
```

---

## Usage

### Standalone tunnel management

```bash
# List configs
mcosint vpn list-configs ./vpn/

# Start N tunnels (blocking; Ctrl+C to stop and clean up)
sudo mcosint vpn start ./vpn/ --count 3

# In another shell: check health
mcosint vpn status

# Force-stop tunnels started in another session (reads ~/.mcosint/vpn_state.json)
sudo mcosint vpn stop
```

### Integrated with a crawl command

```bash
# VPN tunnels start before the crawl and stop after
sudo mcosint namemc crawl-friends-db <UUID> \
  --vpn-dir ./vpn/ \
  --vpn-count 3 \
  --threads 3 \
  --max-depth 2 \
  --init-db
```

When `--vpn-dir` is provided, `--proxy` and `--proxy-file` are ignored.

`mcosint namemc friends-mesh` reads its VPN configuration from env (Phase 5a):

```bash
# in .env:
VPN_DIR=./vpn/
VPN_COUNT=3
THREADS=3

sudo -E mcosint namemc friends-mesh
```

`VPN_DIR` takes precedence over `PROXY_FILE_PATH` when both are set.

---

## Tunnel lifecycle during a crawl

1. **Before:** `VPNTunnelManager.start_all(N)` creates N namespaces, runs
   OpenVPN + microsocks in each, and waits up to 45 s per tunnel for OpenVPN
   to log `Initialization Sequence Completed`. Tunnels that fail to connect
   end in `state="error"` and are skipped by `VPNProxyProvider.get_proxy()`.
2. **During:** Each crawl worker is assigned one tunnel via
   `worker_id % len(tunnels)`. All HTTP for that worker goes through its
   SOCKS5 proxy. On a transport error, the worker calls
   `tunnel.health_check()` (TCP probe to the SOCKS5 port) and, if it fails,
   calls `manager.restart_tunnel(tunnel)` automatically.
3. **After (or on Ctrl+C / SIGINT):** `stop_all()` SIGTERMs microsocks and
   OpenVPN in each namespace (SIGKILL after timeout), deletes the veth
   pair, and removes the namespace.

---

## Troubleshooting

**`RuntimeError: Missing required binaries: microsocks`**
Install microsocks (see above).

**`RuntimeError: VPN tunnel management requires Linux`**
You're on Windows/macOS. The VPN module is Linux-only. Use `--proxy` with
external SOCKS5 endpoints instead.

**Tunnel stuck in `state="starting"` until 45 s timeout**
Read the per-tunnel log: `./vpn/logs/tunnel-0.log`. Most common causes:

- *Empty / silent log* — host iptables MASQUERADE rule missing. OpenVPN's
  control packets aren't getting NATted out the host interface. Normally
  `start_all()` installs the rule automatically; if outbound-interface
  detection failed (check the warning in the manager logs), add it manually
  per the "Host iptables NAT rule" section above.
- `AUTH_FAILED` — bad credentials in `auth-user-pass`.
- `TLS Error` — certificate problems.
- `Cannot resolve host address` — DNS not working from inside the namespace.
  Try `sudo ip netns exec mcosint-vpn-0 cat /etc/resolv.conf` and check.

**`ip: command not found`**
`sudo apt install iproute2`.

**Permission denied**
Run with `sudo` or grant `CAP_NET_ADMIN`:

```bash
sudo setcap cap_net_admin+eip $(which mcosint)
```

**Tunnel is `state="running"` but `healthy=False`**
microsocks isn't listening. Check the openvpn log for late errors and verify
`microsocks` is installed inside the namespace's PATH.

**Worker keeps hitting transport errors and restarting tunnels**
The auto-restart on transport failure has no per-tunnel cool-down (Phase 5d
in ARCHITECTURE.md). If a tunnel is genuinely flaky, the worker can restart
it many times per minute. Workaround: stop the affected tunnel manually and
let the crawl run with fewer tunnels.

---

## Network addressing

Each tunnel N uses subnet `10.200.N.0/30`:

- Host-side veth IP: `10.200.N.1`
- Namespace IP (microsocks bind): `10.200.N.2`

Supports up to 256 tunnels (N in `0..255`).

Interface names (must be ≤15 chars):

- `mcosint-hN` — host-side veth
- `mcosint-nN` — namespace-side veth
- `mcosint-vpn-N` — namespace

State is persisted at `~/.mcosint/vpn_state.json` so `vpn status` / `vpn stop`
can find tunnels from a different shell.
