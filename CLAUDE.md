# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

## Project overview

`mcosint` is a Python CLI for harvesting Minecraft player social graphs from
NameMC. It does BFS from seed UUIDs and persists players + friendships to
PostgreSQL, with optional Cloudflare bypass (FlareSolverr), per-thread SOCKS5
proxies, and Linux network-namespace VPN tunnels for outbound IP isolation.

Phase 0-4 of the original roadmap is **implemented**: unified `CrawlEngine`,
pluggable `TaskQueue` (in-process + Postgres-backed), proxy + VPN subsystems,
metrics recorder, distributed `mcosint worker` CLI with heartbeat + GC of stale
claims. See `docs/ARCHITECTURE.md` for the full picture and the remaining work.

## Common commands

### Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env             # then edit DATABASE_URL
mcosint config init
mcosint db init                  # creates players, friendships, crawl_queue, worker_nodes
```

### Development

```bash
ruff check src/ tests/
ruff format src/ tests/
basedpyright                     # type check
pytest tests/                    # 106 tests, all deterministic / mockable
pytest tests/test_friend_mesh.py::test_build_friend_mesh_respects_limits
```

### Running

```bash
# Primary crawl (CLI-driven, supports proxies + VPN)
mcosint namemc crawl-friends-db <UUID> --init-db --threads 5 --max-depth 2

# Env-driven mesh worker (no proxy/VPN support today — see Bug #2 in ARCHITECTURE.md)
mcosint namemc friends-mesh

# Distributed multi-VM worker
mcosint worker seeds --uuids-file inputs/target_uuids.txt
mcosint worker start --concurrency 5 --max-depth 3 --proxy socks5://... --node-id vm-01
mcosint worker status

# VPN tunnels (Linux only)
sudo mcosint vpn start ./vpn/ --count 3
```

## Architecture

Four layers:

```
CLI (Typer)         cli.py
  └── Commands      commands/{config,db,flaresolverr,http,namemc,vpn,worker}_cmds.py
       └── Domain   crawl/  graph/  services/  workers/
            └── Infra http/  db/  proxy/  metrics/  storage/  notify/  util/
```

### Key modules

- **`crawl/engine.py`** — `CrawlEngine.run()`. Unified BFS used by
  `crawl-friends-db` and `worker start`. Builds one `ThreadedNameMCClient` per
  worker thread, drives a `TaskQueue` (in-process or Postgres-backed), updates a
  `MetricsRecorder`, calls a `Notifier` on rate-limit events. Pluggable.

- **`crawl/task_queue.py`** — `TaskQueue` protocol with two implementations:
  - `InProcessTaskQueue` — wraps `queue.Queue`, dedupes by depth.
  - `PostgresTaskQueue` — `SELECT FOR UPDATE SKIP LOCKED`, claim timeout GC,
    heartbeat for `worker_nodes` registration. **Currently busy-polls every
    50ms on `dequeue`** — see ARCHITECTURE.md §6 bug #6.

- **`crawl/rate_limiter.py`** — `WorkerRateLimiter`. Per-thread. Single-owner,
  no lock.

- **`crawl/namemc_friends.py`** — Holds `CrawlConfig`. Also contains the
  legacy `crawl_namemc_friends_to_db()` + `GlobalCooldown` + `RequestBudget`.
  **`crawl_namemc_friends_to_db` is dead code** post-Phase-3; only
  `RequestBudget` and `GlobalCooldown` are imported by `engine.py`.

- **`graph/friend_mesh.py`** — `build_friend_mesh_threaded()` used by the
  `friends-mesh` env-driven worker. Accepts `proxy_pool` + `namemc_cfg` for
  per-worker proxy isolation. As of Phase 5a, the worker entrypoint passes
  these through (reading `PROXY_FILE_PATH` / `VPN_DIR` / `VPN_COUNT` from env).

- **`workers/namemc_friends_mesh_worker.py`** — env-driven entrypoint for
  `mcosint namemc friends-mesh`. Reads `.env` into a `WorkerConfig`, builds a
  shared fallback `ThreadedNameMCClient`, a `BurstRateLimiter`, optionally
  a `StaticProxyPool` or `VPNProxyProvider`, and runs
  `build_friend_mesh_threaded()`. Writes a JSON file and sends Discord
  notifications.

- **`services/namemc_threaded.py`** — `ThreadedNameMCClient`. Sync HTTP via
  `httpx.Client`. Supports FlareSolverr (POST to `/v1`) and SOCKS5 proxies.
  Detects 429 in both raw `httpx` responses and nested FlareSolverr JSON.

- **`services/namemc.py`** — Async `NameMCClient`. **Orphaned** — not wired
  to any CLI command.

- **`proxy/pool.py`** — `ProxyConfig`, `StaticProxyPool` (round-robin),
  `NoProxyPool`, `ProxyProvider` protocol.

- **`proxy/vpn.py`** — `VPNTunnelManager` runs N tunnels, each in its own
  Linux netns with a colocated `microsocks` SOCKS5 proxy. `VPNProxyProvider`
  wraps the manager so `CrawlEngine` workers pick up tunnels round-robin.
  As of Phase 5a, `start_all()` auto-detects the host outbound interface via
  `ip route get 8.8.8.8` and installs the required
  `iptables -t nat MASQUERADE` rule. The rule is intentionally left in place
  on `stop_all()` (operators remove it manually if desired).

- **`db/operations.py`** — All DB writes. `persist_namemc_friend_response()`
  upserts player + discovered friends + edges in one transaction. UUIDs are
  pre-sorted to reduce deadlock probability. Tenacity retries on
  `OperationalError`, `DeadlockDetected`, `SerializationFailure`.

- **`db/schema.py`** — DDL for `players`, `friendships`, `crawl_queue`,
  `worker_nodes`. Idempotent (`CREATE TABLE IF NOT EXISTS`, `ALTER TABLE ADD
  COLUMN IF NOT EXISTS`).

- **`db/connection.py`** — `create_pool(cfg)` is the preferred API. The
  legacy `get_pool()` global is deprecated but `workers/namemc_friends_mesh_worker.py`
  still calls it — see ARCHITECTURE.md §6 bug #8.

- **`metrics/recorder.py`** — `MetricsRecorder` protocol. `Noop` and
  `InProcess` implementations. The `InProcess` recorder tracks per-worker
  AND per-proxy counts (Phase 5b).
- **`metrics/prometheus.py`** — Optional `PrometheusMetricsRecorder` (Phase
  5b). Requires the `prometheus_client` extra (`pip install 'mcosint[metrics]'`).
  Activated by `--metrics-port` on `crawl-friends-db` and `worker start`.

### Two crawl entry points (current reality)

| | `crawl-friends-db` | `friends-mesh` |
|---|---|---|
| Engine | `CrawlEngine.run()` | `workers.namemc_friends_mesh_worker.run()` → `build_friend_mesh_threaded()` |
| Config | CLI flags | `.env` |
| Proxy / VPN | `--proxy`, `--proxy-file`, `--vpn-dir`, `--vpn-count` | `PROXY_FILE_PATH`, `VPN_DIR`, `VPN_COUNT` (Phase 5a) |
| FlareSolverr | Per-worker when no proxy | Process-wide (env-driven) |
| Burst limiter | No | `BurstRateLimiter` |
| Output | Metrics dict | JSON mesh file + Discord notifier |

### Resume / caching

`players.friends_crawled_at` is the resume marker. `is_player_friends_crawled()`
returns True for completed nodes — BFS reads edges from DB instead of calling
the API. `--force-recrawl` (only on `crawl-friends-db`) ignores the cache.

### Concurrency model

- **`crawl-friends-db` / `worker start`** — `ThreadPoolExecutor` + a `TaskQueue`.
  Each worker has its own `ThreadedNameMCClient` (so its own `httpx.Client` and
  outbound IP when proxies are assigned). Shared state: `RequestBudget`,
  `GlobalCooldown` (only when no proxies), `stop_event`, and the metrics
  recorder.

- **`friends-mesh`** — Same shape (`ThreadPoolExecutor` + `queue.Queue` inside
  `build_friend_mesh_threaded`) but workers share **one** `ThreadedNameMCClient`
  → one `httpx.Client` → one outbound IP. Adds a process-wide `BurstRateLimiter`
  that intentionally pauses all threads during cool-down.

### Distributed coordination

PostgreSQL is the coordinator. `crawl_queue` uses `FOR UPDATE SKIP LOCKED`.
`worker_nodes` tracks live nodes via 30s heartbeats. Stale `in_progress`
claims (> 300s) are re-queued by `gc_stale_tasks()` running on every node.
No separate broker, no central coordinator process.

## Known architecture issues

See `docs/ARCHITECTURE.md` §5 and §6 for the full list with severities.

**Resolved in Phase 5a** (this branch):

- Bug #1: VPN MASQUERADE rule auto-installed by `VPNTunnelManager.start_all()`.
- Bug #2: `friends-mesh` accepts `PROXY_FILE_PATH` / `VPN_DIR` / `VPN_COUNT`.
- Bug #3: `BurstRateLimiter` defaults match `.env.example`.
- Bug #4: `PostgresTaskQueue.complete()` filters by `worker_id` + `status='in_progress'`.
- Bug #5: `PostgresTaskQueue.enqueue` uses `LEAST(depth)` and reopens `done` rows when shallower.
- Bug #8: Mesh worker uses `create_pool()` instead of deprecated `get_pool()`.
- Bug #9: Dead code (`crawl_namemc_friends_to_db`, `load_crawl_config_from_env`) removed.

**Resolved in Phase 5b**:

- `notify/discord.py` dispatches via a shared background `ThreadPoolExecutor`;
  `send()` is fire-and-forget. `atexit` drains pending sends.
- `InProcessMetricsRecorder` tracks per-proxy counters + per-proxy duration
  averages alongside per-worker.
- Optional `PrometheusMetricsRecorder` + `/metrics` HTTP endpoint via
  `--metrics-port`. Requires `pip install 'mcosint[metrics]'`.

**Still open** (carried into Phase 5c+):

- `PostgresTaskQueue.dequeue` busy-polls (Phase 5c → LISTEN/NOTIFY).
- `mcosint worker start` requires a fake bootstrap seed (Phase 5d).
- `friends-mesh` does not yet thread a `MetricsRecorder` through
  `build_friend_mesh_threaded` (Phase 6 unification).
- `services/namemc.py` async client is orphaned (delete or wire up).
- No graceful shutdown / immediate claim release on SIGTERM (Phase 5d).
- No Grafana dashboard JSON committed under `ops/grafana/` yet.

## Code style

- Line length: 100 (ruff)
- Ruff rules: E, F, I, UP, B
- Python ≥ 3.10 (`pyproject.toml`). Local venv is 3.11; Dockerfile pins 3.12.
- Source root: `src/mcosint/` — all imports use `mcosint.*`
- `typer.Option(...)` in signatures is allowed (B008 suppressed globally)
- Env vars used as CLI defaults must be resolved **in the function body**
  (after `.env` loading), not in `typer.Option(default=os.getenv(...))` —
  the latter runs at import time, before `.env` is loaded.
- Workers should always create their own `httpx.Client`. Never share clients
  across threads — it defeats SOCKS5 isolation.
- Use `create_pool(...)`, not `get_pool(...)`. The latter is deprecated.

## Testing notes

- 106 tests pass (`pytest tests/`).
- All tests are deterministic / mockable; none require a running Postgres.
- An integration suite (real PG via testcontainers) is on the roadmap
  (Phase 5b, see ARCHITECTURE.md).
