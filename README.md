# minecraft-osint-db

Python CLI toolkit for Minecraft player OSINT. Crawls the NameMC friends API via
breadth-first traversal, persists players + friendship edges to PostgreSQL, and
scales horizontally across VMs via a Postgres-backed task queue, per-worker
SOCKS5 proxies, and (Linux-only) OpenVPN tunnels for outbound IP isolation.

> See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full architecture
> overview, known bugs, and the phased roadmap.

---

## Quickstart (Linux)

```bash
# 1. Install
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -e .

# 2. Configure
cp .env.example .env                # then edit DATABASE_URL
mcosint config init                 # writes a JSON config to ~/.config/mcosint/

# 3. Bring up Postgres + (optional) FlareSolverr
docker compose up -d postgres
# (optional Cloudflare bypass)
docker compose up -d flaresolverr
mcosint flaresolverr health         # → "OK"

# 4. Schema
mcosint db init

# 5. Crawl
mcosint namemc crawl-friends-db <UUID> --init-db --threads 5 --max-depth 2
```

> Windows is partially supported. The crawler and DB layer run on Windows, but
> the VPN namespace work in `mcosint vpn` is Linux-only (uses `ip netns`).

---

## Commands

The CLI is rooted at `mcosint`. The seven sub-apps:

| Sub-app | Purpose |
|---|---|
| `mcosint config` | Write/inspect the JSON config under `~/.config/mcosint/` |
| `mcosint db init` | Create the schema (`players`, `friendships`, `crawl_queue`, `worker_nodes`) |
| `mcosint http` | Generic HTTP helpers (`get`, `batch-get`) |
| `mcosint flaresolverr health` | Probe a FlareSolverr instance |
| `mcosint namemc` | NameMC crawling (`crawl-friends-db`, `friends-mesh`) |
| `mcosint vpn` | Manage OpenVPN tunnels (Linux only) |
| `mcosint worker` | Distributed worker node (`seeds`, `start`, `status`) |

### `mcosint namemc crawl-friends-db`

The primary, recommended crawl command. Uses the unified `CrawlEngine`.

```bash
mcosint namemc crawl-friends-db <START_UUID> \
  --init-db \
  --threads 5 \
  --max-depth 2 \
  --max-total-requests 100
```

Key flags:

| Flag | Default | Notes |
|---|---|---|
| `START_UUID` | — | Optional if `--uuids-file` supplied |
| `--uuids-file FILE` | — | One UUID per line, `#` comments allowed |
| `--threads INT` | `5` | Worker thread count |
| `--max-depth INT` | `2` | BFS depth limit |
| `--max-total-requests INT` | `100` | Total NameMC API budget. `0` = unlimited |
| `--max-friends-per-user INT` | `50` | Per-player branching cap |
| `--request-delay FLOAT` | `1.0` | Seconds to wait after each successful request |
| `--rate-limit-backoff-seconds FLOAT` | `10.0` | Backoff on 429 if no `Retry-After` |
| `--max-retries-per-uuid INT` | `3` | Retries on transport / 429 |
| `--force-recrawl` | off | Ignore DB cache and re-fetch already-crawled players |
| `--init-db` | off | Run `create_schema` before starting |
| `--proxy URL` | — | Repeatable. `socks5://...` or `http://...`. Workers round-robin |
| `--proxy-file FILE` | — | One URL per line, `#` comments allowed. Merged with `--proxy` |
| `--vpn-dir DIR` | — | (Linux) Spin one OpenVPN tunnel per `.ovpn` file; workers get assigned by index |
| `--vpn-count INT` | `--threads` | Override how many tunnels start when `--vpn-dir` is used |
| `--use-flaresolverr / --no-use-flaresolverr` | off | Workers **without** a proxy route via FlareSolverr |
| `--flaresolverr-url URL` | from config | Override FlareSolverr base |
| `--metrics-port INT` | `0` (off) | Expose Prometheus `/metrics` on this port. Requires `pip install 'mcosint[metrics]'` |

Behavior:

- Each thread builds its own `httpx.Client`. SOCKS5 / VPN isolation is per-thread.
- Resume is automatic: a UUID with `friends_crawled_at IS NOT NULL` is expanded
  from the DB, no API call.
- Crashes lose in-progress in-memory queue items but never lose finished work.

### `mcosint namemc friends-mesh`

Env-driven mesh worker. Reads everything from `.env`. Writes a JSON mesh file
in addition to persisting to the DB. Sends Discord notifications if configured.

```bash
mcosint namemc friends-mesh           # all config from .env
```

Required env vars (see `.env.example` for the full list):

- `DATABASE_URL` — Postgres
- `UUID_LIST_FILE_PATH` — path to seeds file (e.g. `inputs/target_uuids.txt`)
- `OUTPUT_PATH` — where to write the JSON mesh
- `MAX_DEPTH`, `THREADS`, `MAX_TOTAL_CALLS`, `MAX_FRIENDS_PER_USER` — traversal
- `USE_FLARESOLVERR`, `FLARESOLVERR_URL` — Cloudflare bypass
- `MIN_API_WAIT_TIME`, `MAX_API_WAIT_TIME`, `MIN_BURST_CALLS`, `MAX_BURST_CALLS`,
  `PER_CALL_MIN_WAIT_SECONDS`, `PER_CALL_MAX_WAIT_SECONDS` — `BurstRateLimiter`
- `DISCORD_WEBHOOK_URL` — optional notifier

Optional env vars for **per-thread IP isolation** (added in Phase 5a):

- `PROXY_FILE_PATH` — path to a file with one `socks5://` / `http://` URL per
  line. Workers are round-robin assigned. `#` comments allowed.
- `VPN_DIR` — directory of `.ovpn` files (Linux only). One OpenVPN tunnel per
  file is started before the crawl and torn down after.
- `VPN_COUNT` — number of tunnels (defaults to `THREADS`).

If both are set, `VPN_DIR` wins. If neither is set, all threads share the
host's outbound IP (legacy behavior).

### `mcosint worker start`

Drain `crawl_queue` as a horizontally-scalable worker node.

```bash
mcosint worker start \
  --concurrency 5 \
  --max-depth 3 \
  --proxy socks5://10.0.0.1:1080 \
  --proxy socks5://10.0.0.2:1080 \
  --node-id vm-01
```

Flags:

| Flag | Default | Notes |
|---|---|---|
| `--concurrency INT` | `5` | Worker thread count |
| `--max-depth INT` | `3` | BFS depth limit |
| `--node-id TEXT` | hostname | Unique identifier in `worker_nodes` |
| `--heartbeat-interval FLOAT` | `30.0` | Seconds between heartbeats |
| `--request-delay FLOAT` | `1.0` | Per-request post-success sleep |
| `--max-total-requests INT` | `0` | `0` = unlimited |
| `--proxy URL`, `--proxy-file FILE` | — | Same as `crawl-friends-db` |
| `--db-url URL` | from `DATABASE_URL` | |
| `--init-db` | off | |
| `--metrics-port INT` | `0` (off) | Expose Prometheus `/metrics` on this port. Requires `pip install 'mcosint[metrics]'` |

Lifecycle:

1. Connect DB pool, optionally `create_schema`.
2. Register a row in `worker_nodes` (upsert), start heartbeat thread (30s).
3. Start GC thread that re-queues `in_progress > 300s` claims (every 60s).
4. Read one `pending` UUID from `crawl_queue` as a bootstrap seed.
5. `CrawlEngine.run()` drains the queue using `PostgresTaskQueue`.
6. On exit: stop heartbeat + GC, deregister node.

To populate the queue:

```bash
mcosint worker seeds <UUID1> <UUID2> ...
# or
mcosint worker seeds --uuids-file inputs/target_uuids.txt --depth 0
```

To inspect:

```bash
mcosint worker status
```

### `mcosint vpn` (Linux only)

Manage OpenVPN tunnels in Linux network namespaces, each colocated with a
`microsocks` SOCKS5 proxy on `10.200.<index>.2:1080`. See
[`docs/vpn-setup.md`](docs/vpn-setup.md) for prerequisites, including the
**iptables MASQUERADE** rule the host must have for namespace traffic to NAT
out the host's main interface.

```bash
mcosint vpn list-configs ./vpn/
sudo mcosint vpn start ./vpn/ --count 3
mcosint vpn status            # reads ~/.mcosint/vpn_state.json
sudo mcosint vpn stop
```

When using `--vpn-dir` on a crawl command, the lifecycle is handled
automatically (start before crawl, stop after).

### `mcosint http` helpers

```bash
mcosint http get <URL> [--json] [--out file.txt] [--use-flaresolverr]
mcosint http batch-get urls.txt --concurrency 50 --out-dir output/http_batch
```

`batch-get` writes each body to a hashed filename and produces an `index.json`
with success/error metadata.

### `mcosint flaresolverr health`

```bash
mcosint flaresolverr health --flaresolverr-url http://localhost:8191
```

Exit code 0 on healthy, 1 otherwise.

---

## Environment variables

Set these in `.env` (loaded by the CLI on startup). CLI flags take precedence
over env vars where both exist.

### Core

| Variable | Default | Used by |
|---|---|---|
| `DATABASE_URL` | — *(required)* | All crawl + worker commands |
| `DB_POOL_MIN_SIZE` | `1` | `psycopg_pool.ConnectionPool` |
| `DB_POOL_MAX_SIZE` | `10` | `psycopg_pool.ConnectionPool`; bump above `threads+2` for write-heavy crawls |
| `MCOSINT_FLARESOLVERR_URL` | `http://localhost:8191` | Default FlareSolverr base URL |
| `MCOSINT_USE_FLARESOLVERR` | `false` | Default for `mcosint http get` only |
| `MCOSINT_DISCORD_WEBHOOK_URL` | — | Default Discord webhook for notifications |

### `crawl-friends-db` (CLI-driven; env vars are not consulted)

`crawl-friends-db` reads only its CLI flags. The legacy
`THREAD_COUNT`, `REQUEST_DELAY`, `MAX_TOTAL_REQUESTS`, etc. env vars do **not**
apply to it.

### `friends-mesh` (env-driven)

| Variable | Default | Notes |
|---|---|---|
| `UUID_LIST_FILE_PATH` | — *(required)* | Path to a `.txt` of seed UUIDs |
| `MAX_DEPTH` | `2` | |
| `MAX_TOTAL_CALLS` | unlimited | `<=0` or unset = unlimited |
| `MAX_FRIENDS_PER_USER` | unlimited | `<=0` or unset = unlimited |
| `OUTPUT_PATH` | `output/friend_mesh.json` | |
| `INIT_DB` | `false` | If `true`, runs `create_schema` at startup |
| `THREADS` | `5` | Overrides `THREAD_COUNT` for this worker |
| `RATE_LIMIT_BACKOFF_SECONDS` | `10` | Fallback when `Retry-After` is missing |
| `MAX_RETRIES_PER_UUID` | `3` | |
| `USE_FLARESOLVERR` | `true` | Overrides `MCOSINT_USE_FLARESOLVERR` for this worker |
| `FLARESOLVERR_URL` | from config | Overrides `MCOSINT_FLARESOLVERR_URL` |
| `DISCORD_WEBHOOK_URL` | — | Overrides `MCOSINT_DISCORD_WEBHOOK_URL` |
| `MIN_API_WAIT_TIME` | `15.0` | Burst cool-down min seconds |
| `MAX_API_WAIT_TIME` | `45.0` | Burst cool-down max seconds |
| `MIN_BURST_CALLS` | `3` | Burst size min |
| `MAX_BURST_CALLS` | `8` | Burst size max |
| `PER_CALL_MIN_WAIT_SECONDS` | `0.3` | Per-call jitter |
| `PER_CALL_MAX_WAIT_SECONDS` | `1.0` | Per-call jitter |
| `PROXY_FILE_PATH` | — | Path to a file of `socks5://` / `http://` URLs (one per line). Round-robin per thread |
| `VPN_DIR` | — | Directory of `.ovpn` files (Linux only). VPN tunnels are started + stopped automatically |
| `VPN_COUNT` | `THREADS` | Number of tunnels when `VPN_DIR` is set |

---

## Resume behavior

Both crawl paths are restart-safe:

- A UUID with `players.friends_crawled_at IS NOT NULL` is expanded from the DB,
  no API call.
- `--force-recrawl` (on `crawl-friends-db`) ignores the cache and re-fetches.
- For the distributed worker path, `crawl_queue.status = 'done'` is the
  resume marker. Reset via `UPDATE crawl_queue SET status='pending'` to
  force-recrawl everything.

---

## Architecture overview

```
CLI (Typer)
  └── commands/  (config_cmds, db_cmds, http_cmds, flaresolverr_cmds,
                  namemc_cmds, vpn_cmds, worker_cmds)
         │
         ├── crawl/
         │   ├── engine.py        — CrawlEngine (unified BFS, used by crawl-friends-db & worker start)
         │   ├── task_queue.py    — InProcessTaskQueue, PostgresTaskQueue
         │   ├── rate_limiter.py  — WorkerRateLimiter (per-thread)
         │   └── namemc_friends.py — CrawlConfig dataclass (and legacy unused crawl loop)
         │
         ├── graph/friend_mesh.py — build_friend_mesh_threaded (used by friends-mesh worker)
         │                          + async build_friend_mesh (tests only)
         │
         ├── workers/namemc_friends_mesh_worker.py — env-driven entrypoint for friends-mesh
         │
         ├── services/
         │   ├── namemc_threaded.py — ThreadedNameMCClient (sync; FlareSolverr + SOCKS5)
         │   └── namemc.py          — async NameMCClient (currently orphaned)
         │
         ├── proxy/
         │   ├── pool.py        — ProxyConfig, StaticProxyPool, NoProxyPool, ProxyProvider protocol
         │   ├── vpn.py         — VPNTunnel, VPNTunnelManager, VPNProxyProvider (Linux only)
         │   └── residential.py — ResidentialProxyProvider (stub)
         │
         ├── db/
         │   ├── connection.py  — create_pool, DbPoolConfig (and deprecated get_pool)
         │   ├── schema.py      — DDL for players, friendships, crawl_queue, worker_nodes
         │   └── operations.py  — persist_namemc_friend_response, upserts, resume checks
         │
         ├── http/
         │   ├── async_client.py — AsyncFetcher (used by `mcosint http`)
         │   ├── flaresolverr.py — FlareSolverrClient
         │   └── worker_client.py — make_worker_client helper
         │
         ├── metrics/recorder.py — MetricsRecorder protocol, InProcess / Noop impls
         ├── notify/             — Notifier protocol, DiscordWebhook
         ├── storage/json_store.py — write_json
         ├── util/uuid_tools.py  — normalize_uuid_str
         ├── config.py           — AppConfig, load_config (JSON + env overrides)
         └── logging.py          — RichHandler setup
```

For the deep version, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Development

```bash
pip install -e ".[dev]"

# Lint
ruff check src/ tests/
ruff format src/ tests/

# Type check
basedpyright

# Tests (106 passing as of Phase 4)
pytest tests/
pytest tests/test_friend_mesh.py::test_build_friend_mesh_respects_limits
```

Source root is `src/mcosint/`. All imports use the `mcosint.*` namespace.

---

## Observability

A Prometheus exporter is available as an optional extra:

```bash
pip install "mcosint[metrics]"
mcosint namemc crawl-friends-db <UUID> --metrics-port 9100
# Prometheus /metrics exposed on :9100
curl localhost:9100/metrics | grep mcosint_
```

Metrics surface (see `docs/ARCHITECTURE.md` §15 for the full list):

- `mcosint_requests_total{worker, proxy, outcome}` — counter
- `mcosint_request_duration_seconds{worker, proxy}` — histogram
- `mcosint_rate_limits_total{worker, proxy}` — counter
- `mcosint_db_writes_total{worker}` — counter
- `mcosint_queue_depth{status}`, `mcosint_active_workers{node_id}` — gauges
  (set by the worker harness, not the engine)

When `--metrics-port` is unset (default), the engine still tracks per-worker
+ per-proxy stats in memory and dumps a one-line summary at end of run.

`friends-mesh` does not currently expose Prometheus (it uses a separate code
path, `build_friend_mesh_threaded`, that doesn't yet take a `MetricsRecorder`).
That convergence is on the roadmap.

## Known limitations

- **`PostgresTaskQueue.dequeue` busy-polls** at 20 Hz per worker. Fine up to
  ~20 nodes; past that, expect noticeable PG idle CPU. LISTEN/NOTIFY upgrade
  is on the roadmap (Phase 5c).
- **Two crawl entry points** (`crawl-friends-db`, `friends-mesh`) duplicate
  some behavior. The unified `CrawlEngine` is the long-term plan;
  `friends-mesh` was wired up to the proxy/VPN subsystem in Phase 5a but still
  takes a different code path (`build_friend_mesh_threaded`) and does not get
  the Prometheus exporter yet.
- **`mcosint worker start` requires a fake bootstrap seed** to satisfy the
  engine's seed validation. Phase 5d will add a `run_until_empty()` API.

For the full picture, see `docs/ARCHITECTURE.md` §6 (current bug list) and
§8 (phased roadmap).
