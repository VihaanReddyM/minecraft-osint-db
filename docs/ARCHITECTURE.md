# mcosint — Architecture & Implementation Roadmap

> **Audience.** Developers working on the crawler, operators running it at scale,
> and reviewers deciding which refactors are worth doing next.
>
> **Status.** Reflects the codebase at the tip of `Base-app` (commit `6c181a4`,
> on top of `4b04374` "Implement distributed crawling architecture (Phases 0-4)").

---

## 1. What the project is

`mcosint` is a Python CLI for harvesting Minecraft player social graphs from NameMC's
friends API. It does breadth-first traversal from one or more seed UUIDs, persists
everything (players + friendship edges) to PostgreSQL, and is being grown into a
distributed, multi-IP crawler so a single NameMC rate limit doesn't stall the whole
operation.

The codebase has reached **Phase 4** of the original roadmap: a unified
`CrawlEngine`, a pluggable `TaskQueue` (in-process **and** Postgres-backed for
multi-node coordination), proxy + VPN abstractions, a metrics recorder, and a
standalone `mcosint worker` CLI for horizontal scaling.

What's currently **not** done: NAT for VPN namespaces, observability beyond a
JSON snapshot, FlareSolverr inside the unified engine when proxies are present,
and a coordinator API.

---

## 2. Current architecture

### 2.1 Layered view

```
CLI (Typer)                      mcosint config | db | http | flaresolverr | namemc | vpn | worker
  │
Commands  ─ src/mcosint/commands/*.py
  │
Domain    ─ crawl/ (engine, task_queue, rate_limiter, namemc_friends)
           graph/ (build_friend_mesh{_threaded})
           services/ (NameMC clients)
           workers/ (env-driven mesh worker)
  │
Infra     ─ http/ (AsyncFetcher, FlareSolverr)
           db/   (pool, schema, operations)
           proxy/ (StaticProxyPool, VPNTunnelManager, ResidentialProxyProvider)
           metrics/ (InProcess/Noop recorders)
           storage/, notify/, util/
```

### 2.2 The two crawl entry points

| Path | `mcosint namemc crawl-friends-db` | `mcosint namemc friends-mesh` |
|---|---|---|
| Engine | `CrawlEngine.run()` | `workers.namemc_friends_mesh_worker.run()` → `build_friend_mesh_threaded()` |
| Config source | CLI flags + `load_config()` | `.env` (`load_worker_config_from_env`) |
| HTTP client | One `ThreadedNameMCClient` per worker (built inside `CrawlEngine.worker`) | One shared `ThreadedNameMCClient` for all workers |
| FlareSolverr | Yes — workers without a proxy route via FS | Yes — env-driven, applied to all workers |
| Proxy/SOCKS5 | Yes — `--proxy`, `--proxy-file`, `--vpn-dir` | **No** — the worker entrypoint does not pass `proxy_pool`/`namemc_cfg` even though `build_friend_mesh_threaded` accepts them |
| Burst limiter | No | Yes — `BurstRateLimiter` shared across threads |
| Output | DB + metrics dict | DB + JSON file + Discord notification |

Both paths share **the same DB schema** (`players`, `friendships`,
`crawl_queue`, `worker_nodes`) and the same `ThreadedNameMCClient`.

### 2.3 The distributed worker path (Phase 4)

```
            ┌────────────────────────────┐
            │   PostgreSQL (single src   │
            │  of truth: crawl_queue,    │
            │  players, friendships,     │
            │  worker_nodes)             │
            └─┬────────────┬────────────┬┘
              │            │            │
       FOR UPDATE    FOR UPDATE   FOR UPDATE
       SKIP LOCKED   SKIP LOCKED  SKIP LOCKED
              │            │            │
      ┌───────▼──┐  ┌──────▼───┐  ┌────▼─────┐
      │ Worker A │  │ Worker B │  │ Worker C │
      │ (1+N     │  │ (1+N     │  │ (1+N     │
      │ threads, │  │ threads, │  │ threads, │
      │ proxies) │  │ proxies) │  │ proxies) │
      └──────────┘  └──────────┘  └──────────┘
        |   |  |     |   |   |     |   |   |
       proxy/VPN per worker (each worker isolates its outbound IP)
```

- `mcosint worker seeds` populates `crawl_queue`.
- `mcosint worker start` registers a row in `worker_nodes`, spins a heartbeat
  thread, starts a GC thread that re-queues `in_progress` tasks claimed
  `> claim_timeout` (default 300s) ago, and runs `CrawlEngine.run()` with a
  `PostgresTaskQueue`.
- Each node's `CrawlEngine` spins `--concurrency` threads. Each thread is assigned
  a proxy round-robin (if `--proxy` / `--proxy-file` provided).
- Cross-node coordination is **only** via PostgreSQL — there is no separate
  message broker, coordinator process, or scheduler. The PG row lock is the lock.

### 2.4 Concurrency primitives (real, current)

| Primitive | Owner | Lock semantics |
|---|---|---|
| `InProcessTaskQueue` | One per process | Internal `threading.Lock` guards `_seen` dict; wraps `queue.Queue` for items |
| `PostgresTaskQueue` | One per process | All operations go through DB transactions; locking is PG's row lock |
| `seen_depth` dict | `build_friend_mesh_threaded` | `seen_depth_lock` (`threading.Lock`) — shallower-depth invariant maintained |
| `mesh` dict | `build_friend_mesh_threaded` | `mesh_lock` |
| `RequestBudget` | One per `CrawlEngine.run()` | `threading.Lock` around an int counter |
| `GlobalCooldown` | One per `CrawlEngine.run()` | `threading.Lock` around a deadline — used **only when no proxies**; redundant when each worker has its own IP |
| `WorkerRateLimiter` | One per worker thread | **No lock** — single-owner by design |
| `BurstRateLimiter` | One per process (mesh worker) | `threading.Lock` held during `time.sleep` (intentionally pauses all threads on cool-down) |
| `InProcessMetricsRecorder` | One per `CrawlEngine.run()` | `threading.Lock` |
| `stop_event` | One per `CrawlEngine.run()` | `threading.Event` |
| Connection pool | `psycopg_pool.ConnectionPool` | Built-in |

### 2.5 The HTTP path

```
Worker thread ──> ThreadedNameMCClient (httpx.Client, possibly socks5 proxy)
                    │
                    ├── direct: GET https://api.namemc.com/profile/<uuid>/friends
                    │
                    └── via FlareSolverr: POST http://flaresolverr:8191/v1
                                                  { "cmd": "request.get", "url": ... }
```

- 429 detection happens in two places: raw `httpx` response status, and inside
  FlareSolverr's wrapped JSON body.
- `Retry-After` is parsed (numeric seconds only — HTTP-date format is not supported).
- A worker's transport-level errors trigger a `vpn_tunnel.health_check()` and an
  automatic `proxy_pool.manager.restart_tunnel(...)` if it's a VPN provider.

### 2.6 The DB path

- All writes go through `persist_namemc_friend_response()`, which wraps player
  upserts + edge inserts in one transaction. UUIDs are pre-sorted to reduce
  deadlock probability.
- Resume marker: `players.friends_crawled_at`. `is_player_friends_crawled` is
  the cache check on the BFS hot path.
- `crawl_queue` uses `SELECT ... FOR UPDATE SKIP LOCKED` for exactly-once
  delivery. `worker_nodes` has a `last_heartbeat` column; nodes whose heartbeat
  is older than ~5 min are presumed dead (but no code currently uses that
  signal to do anything).

### 2.7 The proxy / VPN path

```
mcosint vpn start <ovpn-dir> --count N
        │
        ├── creates Linux netns mcosint-vpn-0..N-1
        ├── veth pair mcosint-hN <-> mcosint-nN  (10.200.N.1/30 <-> 10.200.N.2/30)
        ├── runs `openvpn --config server-N.ovpn` inside the namespace
        └── runs `microsocks -i 10.200.N.2 -p 1080` inside the namespace

mcosint namemc crawl-friends-db --vpn-dir <ovpn-dir> --vpn-count N
        │
        └── VPNTunnelManager.start_all(N) -> VPNProxyProvider
                 │
                 └── each CrawlEngine worker_i gets tunnel[i % N].proxy_config
                       │
                       └── ThreadedNameMCClient(proxy_url="socks5://10.200.i.2:1080")
```

Each VPN worker has its own outbound IP. 429 on one tunnel triggers
`WorkerRateLimiter.trigger(backoff)` on that worker only.

### 2.8 Observability

Currently:

- `InProcessMetricsRecorder` collects per-worker request counts / rate-limit
  counts / db-write counts / latencies in memory, returns one snapshot at end.
- `mcosint worker status` reads `worker_nodes` + `crawl_queue` counts.
- Rich logging via `mcosint.logging.configure_logging` (RichHandler).
- Discord webhook notifications on crawl start/finish + 429 events.

No Prometheus, no time-series DB, no per-tunnel breakdown, no historical view.

---

## 3. Phase 0-4 inventory: what is done, what is missing

| Phase | Plan | Reality |
|---|---|---|
| 0 — baseline DB & threaded crawler | players/friendships schema, `crawl-friends-db` | **Done** |
| 1 — pluggable proxies | `ProxyProvider` protocol, `StaticProxyPool`, per-worker `httpx.Client` | **Done** |
| 2 — VPN tunnels | `VPNTunnelManager`, `VPNProxyProvider`, `mcosint vpn` subcommands | **Done** but missing iptables NAT (see §6 bug list) |
| 3 — durable queue | `crawl_queue` table, `PostgresTaskQueue`, GC for stale claims | **Done** |
| 4 — multi-node coord | `worker_nodes` table, `mcosint worker start/seeds/status`, heartbeat | **Done** |
| 5 — observability | Prometheus, per-tunnel histograms, dashboard | **Not started** |
| 6 — coordinator API | FastAPI service for seeds + status | **Not started** |
| 7 — residential proxy providers | Provider-specific clients | **Stub only** (`ResidentialProxyProvider` exists, no real provider) |

### 3.1 Hidden gaps inside completed phases

After Phases 5a + 5b, these remain open:

| Gap | Where | Impact |
|---|---|---|
| `PostgresTaskQueue.dequeue` busy-polls every 50 ms | `crawl/task_queue.py` | ~20 PG queries/s per worker just to ask "is queue empty?" — Phase 5c |
| `worker start` needs a fake seed to bootstrap | `commands/worker_cmds.py:138-167` | `CrawlEngine.run()` requires at least one seed even when the queue is already full — Phase 5d |
| `friends-mesh` does not get `--metrics-port` | `workers/namemc_friends_mesh_worker.py`, `graph/friend_mesh.py` | `build_friend_mesh_threaded` doesn't accept a `MetricsRecorder` yet — Phase 6 |
| `services/namemc.py` async `NameMCClient` is orphaned | not wired to any CLI command | Either revive or delete |
| No graceful shutdown / immediate claim release on SIGTERM | `commands/worker_cmds.py` | GC's 5-min timeout is the only release path — Phase 5d |
| No Grafana dashboard JSON | `ops/grafana/` (not present) | Operator-facing dashboards — Phase 5b deferred |

Closed in Phase 5a:

- VPN MASQUERADE rule (bug #1), `friends-mesh` proxy/VPN regression (bug #2),
  `BurstRateLimiter` defaults (bug #3), `PostgresTaskQueue` complete/enqueue
  semantics (bugs #4, #5), deprecated `get_pool()` (bug #8), dead code (bug #9).

Closed in Phase 5b:

- Discord notifier blocked on hot path → background ThreadPoolExecutor with
  atexit drain.
- Per-proxy metrics breakdown in both `InProcessMetricsRecorder` (dicts in
  `snapshot()["per_proxy"]`) and `PrometheusMetricsRecorder` (label).
- `mcosint/metrics/prometheus.py` + `--metrics-port` flag on
  `crawl-friends-db` and `worker start`, exposing `/metrics` via
  `prometheus_client.start_http_server`. Optional dep
  (`pip install 'mcosint[metrics]'`).

---

## 4. Architectural strengths

1. **Clean layering.** Commands → engine → services → http/db. No layer reaches over.
2. **Pluggability via Protocols.** `TaskQueue`, `ProxyProvider`, `MetricsRecorder`,
   `Notifier`, `FriendProvider` are all duck-typed protocols, so production code
   and tests can swap implementations freely.
3. **Single source of truth for distributed work.** Postgres-backed queue is the
   simplest possible coordination mechanism — no broker to operate.
4. **Per-worker HTTP client.** `CrawlEngine` builds one `httpx.Client` per worker
   thread inside the worker closure. Connection pools are not shared across
   workers, which means SOCKS5 isolation actually holds.
5. **Resume-friendly DB cache.** `friends_crawled_at` makes re-runs nearly free
   for already-crawled subgraphs.
6. **Sensible test coverage on the deterministic bits.** 106 tests pass on the
   non-network-dependent modules (queue, rate limiter, proxy, metrics, VPN
   geometry, worker CLI).

---

## 5. Architectural weaknesses & scaling blockers

| Issue | Where | Cost |
|---|---|---|
| Two divergent crawl entry points | `crawl-friends-db` (engine) vs `friends-mesh` (worker module) | Maintenance: every bug fix must be applied twice or in only one of two |
| `BurstRateLimiter` blocks **all threads** while sleeping (lock held over `time.sleep`) | `workers/namemc_friends_mesh_worker.py:107` | By design, but cripples throughput when bursts cool down |
| `PostgresTaskQueue.dequeue` is busy-poll | `crawl/task_queue.py` | Wastes 20 round-trips/sec per worker idle |
| `PostgresTaskQueue.join` polls every 0.5s | `crawl/task_queue.py:130` | Same shape; cheap but wasteful |
| No connection-pool sharing across `CrawlEngine.run` invocations | Pool created per CLI invocation | Fine for one-shot crawls; would be wrong if engine ever embedded |
| `_pool` global in `db/connection.py` only used by `get_pool()` | `db/connection.py:16` | Already deprecated, but still imported and used by `workers/...` |
| No back-pressure on `crawl_queue` growth | `crawl/task_queue.py` | A crawl with very dense graphs can balloon the queue table; no soft cap |
| No deduplication for `crawl_queue` enqueues across depth changes | `PostgresTaskQueue.enqueue` | `ON CONFLICT (uuid) DO NOTHING` — keeps the **first** depth seen, even if a shallower depth comes later. In-process queue handles this; Postgres queue does not |
| No per-tunnel metrics | `metrics/recorder.py` | Can't tell which VPN is taking the hit |
| `mcosint worker status` is text-only | `commands/worker_cmds.py:244` | Fine for ops, useless for dashboards |
| No retry budget per worker / per tunnel | `crawl/engine.py:202-` | A single bad UUID can burn `max_retries_per_uuid` attempts and a backoff per attempt |
| No backoff on consecutive 429s within a tunnel | `WorkerRateLimiter.trigger` extends, doesn't multiply | Pathological case: tunnel under sustained 429 keeps being slammed |
| Discord notifier blocks for 5s on any send | `notify/discord.py:30` | Synchronous httpx.post in the hot path |
| `gc_stale_tasks()` runs from every node | `crawl/task_queue.py:207` | OK because UPDATE is idempotent, but redundant work across N nodes |

---

## 6. Bugs catalog

> Severity: **H** = breaks behavior or correctness, **M** = silent regression
> or perf cost, **L** = cosmetic / future-proofing.
>
> Status: **fixed** = resolved in Phase 5a, **open** = still outstanding.

1. **[H, fixed in Phase 5a]** ~~VPN namespace can't reach the internet without
   iptables NAT.~~ `VPNTunnelManager.start_all()` now auto-detects the host's
   default outbound interface via `ip route get 8.8.8.8` and idempotently
   installs `iptables -t nat -A POSTROUTING -s 10.200.0.0/16 -o <iface>
   -j MASQUERADE` before starting tunnels. The rule is intentionally left in
   place on `stop_all()` so subsequent sessions don't have a setup window;
   operators clean it up manually if they care.

2. **[H, fixed in Phase 5a]** ~~`friends-mesh` silently lost `--proxy` /
   `--vpn-dir` after the worker refactor.~~ `WorkerConfig` now reads
   `PROXY_FILE_PATH`, `VPN_DIR`, `VPN_COUNT` from env. `run()` builds a
   `StaticProxyPool` or `VPNProxyProvider` and forwards it (and the
   `namemc_cfg` template) to `build_friend_mesh_threaded`. VPN tunnels are
   stopped in the `finally` block.

3. **[H, fixed in Phase 5a]** ~~`BurstRateLimiter` defaults disagree with
   documented defaults.~~ Python defaults now match `.env.example`
   (`15.0, 45.0, 3, 8`).

4. **[M, fixed in Phase 5a]** ~~`PostgresTaskQueue.complete()` does not filter
   by worker_id.~~ Now `WHERE uuid=%s AND worker_id=%s AND status='in_progress'`,
   using the queue's own `_node_id`. A stale worker whose claim was GC-reaped
   no-ops cleanly.

5. **[M, fixed in Phase 5a]** ~~`PostgresTaskQueue.enqueue` ignores depth
   improvements.~~ Now `DO UPDATE SET depth = LEAST(...)`, and if a `done`
   row's depth would improve, status is reset to `pending` for re-traversal
   (the `players.friends_crawled_at` cache still skips the HTTP call).

6. **[M, open]** `PostgresTaskQueue.dequeue` busy-polls. Replace
   `time.sleep(0.05)` loop with `LISTEN/NOTIFY` or longer back-off + early
   exit on `stop_event`. Phase 5c.

7. **[M, open]** `mcosint worker start` requires a fake seed. It reads one
   pending row from `crawl_queue` to satisfy `CrawlEngine.run()`'s seed
   validation. Cleaner: split engine init from a `run_until_empty()` method
   that doesn't require seeds. Phase 5d.

8. **[M, fixed in Phase 5a]** ~~`workers/namemc_friends_mesh_worker.py` uses
   deprecated `get_pool()`.~~ Now uses `create_pool()`.

9. **[M, fixed in Phase 5a]** ~~Dead code: `crawl_namemc_friends_to_db()`,
   `load_crawl_config_from_env()`.~~ Removed. `CrawlConfig`, `GlobalCooldown`,
   `RequestBudget` remain (used by `CrawlEngine`).

10. **[M] `httpx.Timeout(cfg.timeout_seconds)`** in `ThreadedNameMCClient`
    applies the same timeout to connect / read / write / pool. For FlareSolverr
    requests this is OK because the body wait is set via `maxTimeout`. For
    direct requests, separate connect (5s) vs read (30s) would be safer.

11. **[L, fixed in Phase 5b]** ~~`notify/discord.py` blocks on
    httpx.post(timeout=5).~~ Module-level `ThreadPoolExecutor(max_workers=1)`
    serializes sends on a background thread; `send()` returns immediately.
    `atexit` drains pending sends on process exit.

12. **[L] `NameMCClient` (async, in `services/namemc.py`) is orphaned.**
    Used by zero CLI commands. Either wire it to something or delete it.

13. **[L] `pyproject.toml` says `python>=3.10`** but the venv shipped with the
    repo is 3.11.2 and the Dockerfile pins 3.12. No bug, just inconsistent.

14. **[L] `Discord webhook send`** swallows the response and never re-raises.
    A misconfigured webhook URL silently fails, which is fine, but at least
    `log.warning` could include the URL host for debugging.

15. **[L] `_OPENVPN_FAIL_PATTERNS`** is checked by substring in the log file —
    a tunnel that prints "TLS Error" once but recovers will be marked failed.

---

## 7. Target architecture (post-Phase 6)

```
                ┌──────────────────────────────────────────────┐
                │ Coordinator service (FastAPI, optional)      │
                │  - POST /seeds  - GET /status  - WS /live    │
                └─────────────┬────────────────────────────────┘
                              │ thin client (heartbeats, RPC)
              ┌───────────────┴───────────────┐
              │       PostgreSQL              │
              │  crawl_queue • players •      │
              │  friendships • worker_nodes   │
              │  + LISTEN/NOTIFY channels     │
              └───────────────┬───────────────┘
                              │
        ┌────────────────┬────┴────────────────┐
        │                │                     │
   ┌────▼─────┐    ┌─────▼────┐         ┌──────▼────┐
   │ Worker A │    │ Worker B │         │ Worker C  │
   │ (VM-1)   │    │ (VM-2)   │         │ (VM-3)    │
   └─┬────────┘    └─┬────────┘         └─┬─────────┘
     │               │                    │
     │ Per-worker proxy/VPN isolation     │
     ▼               ▼                    ▼
   netns 0..N     netns 0..N          residential pool
    socks5         socks5              (rotating IP)

   metrics:  Prometheus exporter on each worker → central Prometheus → Grafana
```

Key invariants:

- **One worker process : one node row in `worker_nodes`.** Heartbeats keep the
  row fresh; on shutdown the row goes inactive.
- **One thread : one HTTP client : one outbound IP** (when proxies are configured).
- **`crawl_queue` row state machine:** `pending → in_progress → done`.
  GC moves stale `in_progress` back to `pending`. There is no `failed` state —
  too many retries simply leave the UUID `in_progress` until GC takes over.
- **Coordinator is optional.** The DB-only model continues to work; the
  coordinator just provides an HTTP/WebSocket front end.

---

## 8. Phased roadmap

Each phase is sized to be deployable on its own.

### Phase 5a — Correctness pass *(done)*

Goal: clean up the bugs in §6 so the existing architecture is reliable before
adding more surface.

- ✅ Restore proxy/VPN support to `friends-mesh` (Bug #2). New env vars:
  `PROXY_FILE_PATH`, `VPN_DIR`, `VPN_COUNT`.
- ✅ Align `BurstRateLimiter` defaults with `.env.example` (Bug #3).
- ✅ Auto-install iptables MASQUERADE in `VPNTunnelManager.start_all` (Bug #1).
- ✅ `PostgresTaskQueue.complete` filters by `worker_id + status` (Bug #4).
- ✅ `PostgresTaskQueue.enqueue` uses `LEAST(depth)` and reopens `done` rows on
  shallower discovery (Bug #5).
- ✅ Replace `get_pool()` with `create_pool()` in the mesh worker (Bug #8).
- ✅ Delete dead code (`crawl_namemc_friends_to_db`,
  `load_crawl_config_from_env`) (Bug #9).
- ✅ 24 new mock-based tests covering the changes (130 tests total, all passing).
- ⏳ Integration test for `PostgresTaskQueue` against a live PG via
  `testcontainers` — deferred to Phase 5b.

### Phase 5b — Observability *(done)*

Goal: see what the crawler is doing without grepping logs.

- ✅ **Prometheus exporter.** `mcosint/metrics/prometheus.py` implements
  `PrometheusMetricsRecorder` using a private `CollectorRegistry`. Counters
  `mcosint_requests_total{worker, proxy, outcome}`, `mcosint_rate_limits_total`,
  `mcosint_db_writes_total`. Histogram `mcosint_request_duration_seconds`.
  Gauges `mcosint_queue_depth{status}`, `mcosint_active_workers{node_id}`.
  Optional dep — `pip install 'mcosint[metrics]'`.
- ✅ **`/metrics` endpoint on each worker.** Backed by
  `prometheus_client.start_http_server(port, registry=...)`. Off by default;
  opt in via `--metrics-port` on `crawl-friends-db` and `worker start`.
- ✅ **Per-proxy / per-tunnel breakdown.** Promoted in both
  `InProcessMetricsRecorder` (per-proxy dicts in `snapshot()`) and the
  Prometheus labels.
- ✅ **Discord notifier moved to a background thread.** Module-level
  `ThreadPoolExecutor(max_workers=1)` shared across `DiscordWebhook` instances,
  drained on process exit via `atexit`.
- ⏳ **Grafana dashboards** under `ops/grafana/` — deferred. Will be done
  alongside a real test deployment; no point committing JSON I can't validate.

23 new tests added (153 total, all passing).

### Phase 5c — DB & queue scaling *(week)*

1. **PgBouncer recommended config.** Document in `docs/scaling.md`
   (already partially done). Add a compose service.
2. **`PostgresTaskQueue.dequeue` via LISTEN/NOTIFY.** `enqueue` issues
   `NOTIFY crawl_queue_new`; `dequeue` blocks on the channel with a timeout.
   Falls back to polling if `LISTEN` not available.
3. **Soft cap on queue growth.** When `qsize() > N`, workers stop enqueuing
   discovered friends until the queue drains under a low-water mark. Avoids
   pathological queue table sizes on dense graphs.
4. **Partitioned tables** (optional, only if `players`/`friendships` grow past
   ~100M rows). Range partition on `min_depth` or hash partition on `uuid`.

### Phase 5d — Worker lifecycle hardening *(week)*

1. **Graceful shutdown.** `mcosint worker start` traps SIGTERM; releases all
   in-progress claims back to `pending`, deregisters the node, then exits.
2. **`run_until_empty()` API on `CrawlEngine`.** Removes the
   "fake seed" workaround in `worker_cmds.py`.
3. **Dead-node detection.** A separate `mcosint worker reap` command (or a
   chosen leader node) marks `worker_nodes` rows inactive when
   `last_heartbeat < NOW() - INTERVAL '5 min'` and re-queues their stale claims.
4. **Tunnel restart back-off.** Currently a flapping tunnel restarts on every
   transport error. Add a per-tunnel cool-down.

### Phase 6 — Optional coordinator service *(2 weeks, if needed)*

Only worth doing past ~20 worker nodes or if a web UI is required.

1. FastAPI service: `POST /seeds`, `GET /status`, `WS /live`, `POST /nodes/{id}/pause`.
2. Worker registers + sends heartbeats to the coordinator (in addition to DB).
3. The DB-only path keeps working — coordinator is a façade, not a dependency.

### Phase 7 — Residential / datacenter proxy integration *(week per provider)*

1. Flesh out `ResidentialProxyProvider._refresh_proxies()` per provider
   (Bright Data, Smartproxy, NetNut). One file per provider under
   `proxy/providers/`.
2. Health-check + rotation policy.
3. Cost-aware budget: hard cap on bandwidth or request count per provider.

### Phase 8 — Multi-region distribution *(month)*

Only relevant past ~50 worker nodes.

1. Read replica of `players`/`friendships` per region for query workloads.
2. `crawl_queue` stays single-master (it's tiny and write-heavy).
3. Per-region S3 bucket for periodic JSON snapshots of the graph.

---

## 9. Networking / proxy subsystem design

The current shape (`ProxyProvider` protocol + `StaticProxyPool` + `VPNProxyProvider`
+ `ResidentialProxyProvider` stub) is the right one. Extensions should slot in
under `mcosint/proxy/providers/`:

```python
# proxy/providers/brightdata.py
class BrightDataProvider:
    """Fetches a rotating pool of residential proxies from Bright Data."""
    def get_proxy(self, worker_id: int) -> ProxyConfig | None: ...
    def all_proxies(self) -> list[ProxyConfig]: ...
    # plus provider-specific refresh / quota logic
```

Conventions:

- **Round-robin by worker_id** for stable mappings (a worker keeps the same
  proxy across retries — helps with sticky-session services).
- **Never share an `httpx.Client` across workers.** Each worker owns one.
- **Health check is optional but encouraged.** Expose a `health_check()` method
  if the provider supports per-proxy probing.
- **`get_tunnel(worker_id)` is for providers that own restartable transports.**
  Workers check `hasattr(provider, "get_tunnel")` and, if present, can request
  a restart on transport failure. Don't make this part of the `ProxyProvider`
  protocol — it's optional.

Future enhancements:

- **Tiered providers.** Try datacenter first, fall back to residential on 429.
- **Pool size adaptive.** Provider tells the engine how many concurrent IPs it
  can supply; engine clamps `--concurrency` accordingly.

---

## 10. Worker lifecycle design

A worker process should follow this lifecycle:

```
start
  ├── parse CLI / load config
  ├── connect DB pool (create_pool)
  ├── (optional) ensure schema (create_schema)
  ├── load proxy pool (CLI / file / vpn-dir / residential)
  ├── start VPN tunnels if configured (blocking until "running")
  ├── register node row in worker_nodes
  ├── start heartbeat thread (30s default)
  ├── start GC thread (60s default)
  ├── build CrawlEngine + run / run_until_empty
  │     └── inside: ThreadPoolExecutor with N worker threads
  │            each thread:
  │              - own ThreadedNameMCClient (proxy assigned at startup)
  │              - own WorkerRateLimiter
  │              - own retry / backoff
  │
  ├── SIGTERM:
  │     - stop_event.set()
  │     - wait for threads to drain in-flight requests
  │     - release in_progress claims back to pending (worker_id-scoped)
  │     - stop GC + heartbeat
  │     - deregister node
  │     - stop VPN tunnels (if managed by this process)
  └── exit 0
```

Today the lifecycle is roughly that, with three gaps: signal handling is missing
(SIGTERM just kills threads mid-claim), the in-progress release is done by the
GC's 5-minute timeout instead of immediately, and `CrawlEngine.run()` insists
on at least one seed.

---

## 11. Distributed coordination strategy

PostgreSQL is the coordinator. No broker. The two tables that matter:

```sql
crawl_queue (
  uuid           UUID PRIMARY KEY,
  depth          INTEGER NOT NULL DEFAULT 0,
  status         TEXT NOT NULL DEFAULT 'pending',   -- pending|in_progress|done
  enqueued_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  claimed_at     TIMESTAMPTZ NULL,
  worker_id      TEXT NULL                          -- "{hostname}-{pid}"
)
worker_nodes (
  node_id        TEXT PRIMARY KEY,
  host           TEXT NOT NULL,
  last_heartbeat TIMESTAMPTZ NOT NULL,
  worker_count   INTEGER NOT NULL,
  proxy_count    INTEGER NOT NULL,
  status         TEXT NOT NULL                      -- active|inactive
)
```

Invariants (some are enforced, some are convention):

| Invariant | Enforcement |
|---|---|
| Each `uuid` exists in `crawl_queue` at most once | PK constraint |
| Only one worker claims a `pending` row at a time | `FOR UPDATE SKIP LOCKED` |
| `in_progress` rows go back to `pending` after `claim_timeout` | `gc_stale_tasks()` |
| `done` is terminal | Convention; no GC moves it back |
| Each node heartbeats every 30s | `_heartbeat_thread` |

Operationally:

- **Adding capacity** = start another `mcosint worker start` somewhere with
  `DATABASE_URL` pointing at the same PG. No coordination needed.
- **Draining a node** = SIGTERM; the worker should release its claims (Phase 5d).
  Until then, claims are released only by GC's 5-min timeout.
- **Force-recrawl all** = `UPDATE crawl_queue SET status='pending'`.
  (Currently doc'd; no CLI for it yet.)

---

## 12. Database scaling strategy

| Load | Recommended setup |
|---|---|
| < 1 VM, < 5 threads | Single PG, no PgBouncer, default pool |
| 1-5 VMs, 5-20 threads each | Single PG, no PgBouncer, `DB_POOL_MAX_SIZE = threads + 2` |
| 5-50 VMs | Add PgBouncer in **transaction-pooling mode** between workers and PG |
| 50+ VMs / 1M+ players | Promote PG to dedicated instance; consider read replica for `players` queries; partition `friendships` by hash(player_uuid) |

PgBouncer transaction-pool config (already in `docs/scaling.md`):

```ini
[databases]
mcosint = host=postgres dbname=mcosint
[pgbouncer]
pool_mode = transaction
max_client_conn = 500
default_pool_size = 20
```

Caveat: **transaction-pool mode disallows session-level features**
(`LISTEN/NOTIFY`, `SET LOCAL`, prepared statements that persist across
transactions). The Phase 5c LISTEN/NOTIFY work will need session pooling or a
dedicated direct connection per worker for that channel.

---

## 13. Concurrency model

### Current (correct, but redundant in places)

- One process : one `CrawlEngine.run()` : N worker threads (`ThreadPoolExecutor`).
- Each thread:
  - Owns one `ThreadedNameMCClient` (so one `httpx.Client`, one outbound IP).
  - Owns one `WorkerRateLimiter` (single-owner, no lock).
  - Shares the engine-level `RequestBudget`, `GlobalCooldown`, `stop_event`.
  - Shares the `task_queue` (which has its own internal locking).
- The `BurstRateLimiter` (mesh worker only) is process-wide and intentionally
  pauses **all** workers when a burst cools down.

### Recommended cleanups

1. **`GlobalCooldown` is redundant when proxies are configured.** It's already
   only triggered if `not has_proxies`, so it's safe to keep but the engine
   could elide creating it entirely.
2. **`BurstRateLimiter` should release its lock during `time.sleep`.** Pause
   all workers via an `Event.wait(timeout)` instead of `lock + sleep`.
   That way only the *first* thread to enter the cool-down window pays the
   wait; the rest see the gate already closed.
3. **Reduce `seen_depth` contention** in `build_friend_mesh_threaded`. A
   `dict[str, int]` under a single mutex is fine up to ~50 threads; past that,
   consider sharding by `hash(uuid) % 16`.
4. **Heartbeat + GC threads currently use `Event.wait(timeout)`** — already
   good, no change needed.

---

## 14. Fault-tolerance strategy

| Failure | Current behavior | Desired behavior |
|---|---|---|
| 429 from NameMC (one worker) | `WorkerRateLimiter.trigger(retry_after)` pauses that worker only | Keep |
| 429 sustained (one tunnel) | Worker retries up to `max_retries_per_uuid`, then drops the UUID | Add exponential per-tunnel cool-down + alarm |
| Transport error (e.g. tunnel down) | `vpn_tunnel.health_check()` + `manager.restart_tunnel()` | Add a max-restarts-per-minute guard |
| FlareSolverr 503 | Caught as transport error, retried | Add specific health-check on `mcosint flaresolverr health` before crawling |
| DB transient error | Tenacity retries 5 times with jitter | Keep |
| Worker crash mid-claim | `gc_stale_tasks()` re-queues after 300s | Add SIGTERM handler that releases claims immediately |
| Coordinator (PG) down | Workers fail health/heartbeat, retry indefinitely | Add a "PG unreachable for > X" hard stop with notifier alert |
| OOM during very wide crawl | Process dies; in-process queue is lost (Postgres queue safe) | Document; encourage `PostgresTaskQueue` for wide crawls |

---

## 15. Metrics & monitoring

Per Phase 5b, the target set of metrics:

| Metric | Type | Labels |
|---|---|---|
| `mcosint_requests_total` | Counter | `worker_id`, `proxy`, `outcome=ok|err|rate_limited` |
| `mcosint_request_duration_seconds` | Histogram | `worker_id`, `proxy` |
| `mcosint_db_writes_total` | Counter | `worker_id`, `table` |
| `mcosint_db_write_duration_seconds` | Histogram | `worker_id` |
| `mcosint_queue_depth` | Gauge | `status=pending|in_progress|done` |
| `mcosint_active_workers` | Gauge | `node_id` |
| `mcosint_proxy_health` | Gauge (0/1) | `proxy` |
| `mcosint_burst_cooldown_seconds_total` | Counter | (none) |

Alerts worth wiring:

- `rate_of_429s > 0.1` for 5m → notify Discord.
- `mcosint_active_workers == 0 AND queue_depth > 0` → notify Discord.
- `mcosint_proxy_health == 0` for any proxy for 5m → notify Discord.

---

## 16. Testing strategy

Current: 106 tests, all on deterministic / mockable surfaces. Coverage of the
non-network bits is good.

Recommended additions:

1. **Integration test for `PostgresTaskQueue`** (via `testcontainers` or a
   dockerized PG). Covers: enqueue → claim → GC re-queue → claim again →
   complete; concurrent claims across two processes.
2. **End-to-end `CrawlEngine.run()` test** with a mocked HTTP layer and an
   in-process pool (or sqlite shim?) that hits real `persist_namemc_friend_response`.
3. **`BurstRateLimiter` timing test.** Currently zero coverage on the burst
   logic.
4. **`worker start` smoke test.** Boot a worker against an empty `crawl_queue`,
   confirm clean exit + node deregistration.
5. **FlareSolverr-shaped 429 parsing test.** The nested JSON path in
   `ThreadedNameMCClient` has zero tests.

Test layout suggestion: a `tests/integration/` directory gated by an env var
(`MCOSINT_INTEGRATION=1`) so CI can run unit tests in milliseconds and a
slower nightly suite hits a real PG.

---

## 17. Deployment strategy

### Single VM (today)

```bash
mcosint db init
mcosint worker seeds --uuids-file inputs/target_uuids.txt
mcosint worker start --concurrency 5 --max-depth 3
```

### Multiple VMs (today, manual)

Same `DATABASE_URL` on every VM. Run `mcosint worker start` on each.
Pass `--node-id` per VM and `--proxy` lists per VM (so each VM has its own pool).

### Docker compose (today)

```bash
docker compose up -d                          # postgres + flaresolverr + 1 worker
docker compose up -d --scale worker=3         # 3 workers, same DATABASE_URL
docker compose exec worker mcosint worker seeds --uuids-file /seeds.txt
```

Limitations: every Docker worker shares the host's outbound IP unless you
mount `--privileged` and run VPN tunnels inside each container, or attach
each container to a unique macvlan / VPN side-car.

### Future (recommended for > 5 VMs)

Per VM:

- PgBouncer in transaction mode between the worker and Postgres.
- VPN side-cars (`mcosint vpn start ./vpn/ --count N`) before the worker.
- A Prometheus node-exporter + the `mcosint /metrics` endpoint.
- A graceful-shutdown systemd unit that runs `kill -TERM` on the worker pid
  and waits up to 60s for it to flush claims.

---

## 18. Extensibility strategy

| Concern | Extension point |
|---|---|
| New proxy provider | `mcosint/proxy/providers/<name>.py`, implements `ProxyProvider` |
| New notifier (Slack, PagerDuty) | Implement the `Notifier` protocol in `mcosint/notify/<name>.py` |
| New metrics backend | Implement `MetricsRecorder` (e.g. `mcosint/metrics/prometheus.py`) |
| New task queue backend (Redis, NATS) | Implement `TaskQueue` protocol |
| New persistence target (S3 snapshot, ClickHouse) | New module under `mcosint/storage/`; consume the DB read API in `db/operations.py` |
| New crawl target (not NameMC) | New service client under `mcosint/services/`, with a `BlockingFriendProvider`-shaped interface |

Anti-patterns to keep out of the codebase:

- **Global singletons** that depend on env vars at import time. The lingering
  `_pool` in `db/connection.py` is the last one and is already deprecated.
- **HTTP clients shared across threads.** Always one-per-worker, owned by the
  worker.
- **Sync HTTP inside an async path.** Pick one. The async `NameMCClient` is
  currently unused; if it gets revived, keep it strictly async.

---

## 19. Documentation state

After the Phase 5a pass, all docs reflect the actual code:

| File | Content |
|---|---|
| `README.md` | Quickstart + command reference + env table, links to ARCHITECTURE.md |
| `CLAUDE.md` | Operating manual for working in the codebase, with Phase 5a fix summary |
| `.env.example` | Sectioned by command; `PROXY_FILE_PATH` / `VPN_DIR` / `VPN_COUNT` added |
| `docs/scaling.md` | Implemented vs planned scaling story |
| `docs/vpn-setup.md` | Linux prerequisites, MASQUERADE explanation (now automated), troubleshooting |
| `docs/ARCHITECTURE.md` | This document |
