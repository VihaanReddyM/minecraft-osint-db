# Scaling mcosint

This document describes the **current operational story** (what's implemented
and tested) and the **planned scaling path** (what's on the roadmap in
`ARCHITECTURE.md`).

---

## Single-VM limits

A single VM running `mcosint namemc crawl-friends-db` (or a single
`mcosint worker start`) is constrained by:

1. **One outbound IP** — without `--proxy` / `--vpn-dir`, all worker threads
   share the host's IP. NameMC rate-limits by source IP, so 429 stalls the
   whole node.
2. **Single process memory** — when using `InProcessTaskQueue` (the default
   for `crawl-friends-db` without `--proxy`), the BFS frontier lives in
   process RAM. Wide crawls (depth 4+, dense graphs) can use significant
   memory.
3. **Single point of failure** — a crash loses the in-memory frontier
   (already-crawled nodes are safe in DB). Use the worker + `PostgresTaskQueue`
   path for crash-safe queues.

---

## Vertical scaling first — proxies / VPN on one VM (Phases 1-2, implemented)

Before you bring up multiple VMs, you can get most of the way by giving each
worker thread its own outbound IP on a single VM.

### Option A — pre-existing SOCKS5 proxies

```bash
mcosint namemc crawl-friends-db <UUID> \
  --threads 5 \
  --proxy socks5://10.0.0.1:1080 \
  --proxy socks5://10.0.0.2:1080 \
  --proxy socks5://10.0.0.3:1080 \
  --proxy socks5://10.0.0.4:1080 \
  --proxy socks5://10.0.0.5:1080
```

Each worker thread is assigned one proxy by index (round-robin). 429 on one
proxy only pauses that worker.

### Option B — OpenVPN tunnels (Linux only)

`mcosint vpn` creates Linux network namespaces, runs OpenVPN inside each, and
colocates a `microsocks` SOCKS5 proxy. Each tunnel gets its own outbound IP.

```bash
# Put .ovpn configs in ./vpn/
sudo mcosint namemc crawl-friends-db <UUID> \
  --vpn-dir ./vpn/ \
  --vpn-count 5 \
  --threads 5 \
  --init-db
```

See `docs/vpn-setup.md` for prerequisites (including the host iptables
MASQUERADE rule that the manager does **not** install automatically — bug #1
in `docs/ARCHITECTURE.md`).

> Limitation: `mcosint namemc friends-mesh` does not currently accept
> `--proxy` / `--vpn-dir` (the worker entrypoint doesn't forward them). Use
> `crawl-friends-db` for IP isolation. Tracked as bug #2 in ARCHITECTURE.md.

---

## Horizontal scaling — multiple VMs (Phases 3-4, implemented)

The Postgres-backed worker coordination model lets you bring up additional
worker VMs without any central orchestrator. `crawl_queue` and `worker_nodes`
tables in Postgres are the single source of truth.

```
VM-01 workers ─┐
VM-02 workers ─┼─► crawl_queue (PostgreSQL) ◄─ worker_nodes heartbeats
VM-03 workers ─┘
```

### Quick start

**1. Initialize schema (once, from any node):**

```bash
mcosint db init
```

**2. Load seed UUIDs (from any node):**

```bash
mcosint worker seeds 069a79f4-44e9-4726-a5be-fca90e38aaf5 --depth 0
# or from a file:
mcosint worker seeds --uuids-file inputs/target_uuids.txt
```

**3. On each VM, start a worker:**

```bash
mcosint worker start \
  --concurrency 5 \
  --max-depth 3 \
  --proxy socks5://10.0.0.1:1080 \
  --proxy socks5://10.0.0.2:1080 \
  --node-id vm-01
```

Run the same command on each VM, changing `--node-id` and `--proxy` per VM.

**4. Monitor:**

```bash
mcosint worker status
```

Output: a table of active `worker_nodes` rows (with last heartbeat) and the
queue depth broken down by status (`pending` / `in_progress` / `done`).

### How coordination works

- `crawl_queue.status` transitions: `pending → in_progress → done`.
- Each worker thread claims one row at a time using
  `UPDATE crawl_queue SET status='in_progress', worker_id=$node, claimed_at=NOW()
   WHERE uuid = (SELECT uuid FROM crawl_queue WHERE status='pending'
                 ORDER BY enqueued_at FOR UPDATE SKIP LOCKED LIMIT 1)`.
- A background GC thread on every node re-queues rows whose `claimed_at` is
  older than `claim_timeout` (default 300s), so a crashed worker's claims
  don't sit forever.
- A background heartbeat thread on every node updates `worker_nodes.last_heartbeat`
  every 30s.

### Known caveats

- `PostgresTaskQueue.dequeue` busy-polls every 50 ms when the queue is empty.
  With 20+ workers across nodes, this becomes noticeable PG idle traffic. A
  `LISTEN/NOTIFY` upgrade is on the Phase 5c roadmap.
- `PostgresTaskQueue.complete()` does not filter by `worker_id`, so a stale
  worker can mark `done` after GC re-queued. Tracked as bug #4 in
  ARCHITECTURE.md.
- `PostgresTaskQueue.enqueue()` uses `ON CONFLICT DO NOTHING`, so it does
  not improve `depth` when a UUID is rediscovered at a shallower depth.
  Bug #5 in ARCHITECTURE.md.

---

## PgBouncer (recommended past ~5 VMs)

For 5+ VMs with ~5-20 threads each, put PgBouncer in **transaction-pooling
mode** between workers and PostgreSQL:

```ini
[databases]
mcosint = host=localhost dbname=mcosint

[pgbouncer]
pool_mode = transaction
max_client_conn = 500
default_pool_size = 20
```

Set `DATABASE_URL` to PgBouncer's port (default 6432). Workers hold DB
connections only for the duration of one transaction, drastically reducing PG
connection pressure.

> **Important caveat for the Phase 5c roadmap.** Transaction-pool mode
> disallows session-level features (`LISTEN/NOTIFY`, prepared statements,
> `SET LOCAL`). When we move `PostgresTaskQueue.dequeue` to LISTEN/NOTIFY,
> the listener will need a session-pooled or direct connection.

---

## Docker compose

The `docker-compose.yml` at the project root defines three services:

| Service | Purpose |
|---|---|
| `postgres` | PostgreSQL 15 with persistent volume |
| `flaresolverr` | Cloudflare bypass (optional) |
| `worker` | An `mcosint worker start` replica |

### Scale workers horizontally

```bash
docker compose up -d --scale worker=3
docker compose exec worker mcosint worker seeds --uuids-file /seeds.txt
docker compose logs -f worker
```

Each replica shares the same `DATABASE_URL` and registers itself in
`worker_nodes` via the container's hostname.

### Limitations

- Docker workers share the **host's** outbound IP. To get per-thread IP
  isolation in containers, you need one of:
  - `--cap-add NET_ADMIN` + run `mcosint vpn` inside the container,
  - dedicated VPN side-cars (one container per tunnel),
  - or external SOCKS5 endpoints passed via `--proxy`.
- The compose file does not currently wire `--proxy` flags to the worker
  service. Edit the `command:` block (or use an env-driven entrypoint
  wrapper) to do this.

---

## Force-recrawl

To re-fetch nodes you've already crawled, reset the queue:

```sql
-- Reset the queue
UPDATE crawl_queue SET status='pending', claimed_at=NULL, worker_id=NULL;

-- Force re-fetch of friends (otherwise the BFS resumes from DB cache)
UPDATE players SET friends_crawled_at=NULL;
```

Or, for a single-shot crawl: `mcosint namemc crawl-friends-db --force-recrawl ...`.

---

## Future scaling (planned, not yet implemented)

See `docs/ARCHITECTURE.md` §8 for the full roadmap. Highlights:

- **Phase 5b — Observability.** Prometheus exporter per worker, Grafana
  dashboards, per-proxy / per-tunnel breakdown of metrics.
- **Phase 5c — DB & queue scaling.** LISTEN/NOTIFY-based `dequeue`, back-pressure
  on `crawl_queue` growth, optional table partitioning.
- **Phase 5d — Worker lifecycle hardening.** SIGTERM handler that releases
  claims immediately (today, GC waits 5 min). `run_until_empty()` API on
  `CrawlEngine` (today, `worker start` reads a fake bootstrap seed).
- **Phase 6 — Optional FastAPI coordinator** (`POST /seeds`, `GET /status`,
  `WS /live`). DB-only path keeps working; coordinator is a façade.
- **Phase 7 — Residential / datacenter proxy providers.** `ResidentialProxyProvider`
  exists as a stub today; concrete provider clients live under
  `mcosint/proxy/providers/` once implemented.
