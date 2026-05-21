# Scaling mcosint to Multiple VMs

## Single-VM Limits

A single VM running `mcosint namemc crawl-friends-db` is constrained by:

- **One outbound IP** — NameMC rate-limits by source IP. Even with many threads,
  all requests share the same address, so hitting 429 stalls the whole node.
- **Single process memory** — the BFS frontier lives in RAM. Very wide crawls
  (depth 4+, thousands of nodes) can exhaust available memory.
- **Single point of failure** — a crash or reboot loses in-flight work (mitigated
  by the resume marker, but the queue is gone).

## Multi-VM Setup with `mcosint worker`

Phase 4 introduces a DB-backed coordination model. PostgreSQL's `crawl_queue`
table is the single source of truth. Multiple VMs (or containers) each run
`mcosint worker start`, claiming tasks via `SELECT FOR UPDATE SKIP LOCKED`.
No central coordinator process is needed at runtime.

### Quick Start

**1. Initialize the schema (once, from any node):**

```powershell
mcosint db init
```

**2. Load seed UUIDs:**

```powershell
mcosint worker seeds <UUID1> <UUID2> ...
# or from a file:
mcosint worker seeds --uuids-file seeds.txt
```

**3. Start worker nodes (each VM/container):**

```powershell
mcosint worker start \
  --concurrency 5 \
  --max-depth 3 \
  --proxy socks5://10.0.0.1:1080 \
  --proxy socks5://10.0.0.2:1080 \
  --node-id vm-01
```

Run the same command on each VM, changing `--node-id` and `--proxy` values.

**4. Monitor progress:**

```powershell
mcosint worker status
```

### PostgreSQL-Backed Queue Coordination Pattern

```
VM-01 workers ─┐
VM-02 workers ─┼─► crawl_queue (PostgreSQL) ◄─ worker_nodes heartbeats
VM-03 workers ─┘
```

- `crawl_queue.status` transitions: `pending` → `in_progress` → `done`
- `SELECT FOR UPDATE SKIP LOCKED` ensures exactly-once delivery across workers
- Crashed workers leave tasks `in_progress`; the GC thread re-queues them after
  `claim_timeout` (default 300 s) via `gc_stale_tasks()`
- `worker_nodes` table tracks live nodes via 30-second heartbeats; stale entries
  (last_heartbeat > 5 min ago) indicate crashed/stopped nodes

### PgBouncer Guidance

For large deployments (10+ VMs, 50+ threads each), add PgBouncer in
transaction-pooling mode between workers and PostgreSQL:

```ini
[databases]
mcosint = host=localhost dbname=mcosint

[pgbouncer]
pool_mode = transaction
max_client_conn = 500
default_pool_size = 20
```

Set `DATABASE_URL` to point at PgBouncer's port (default 6432). Each worker
thread holds a DB connection only for the duration of a single transaction,
reducing PostgreSQL connection pressure significantly.

### Docker Compose Usage

The `docker-compose.yml` at the project root defines three services:

| Service | Purpose |
|---------|---------|
| `postgres` | PostgreSQL 15 with persistent volume |
| `flaresolverr` | Cloudflare bypass (optional) |
| `worker` | mcosint worker node |

**Scale workers horizontally:**

```bash
docker compose up -d --scale worker=3
```

Each replica gets a unique hostname (used as `--node-id`) and connects to the
shared `postgres` service. Increase `replicas` in `docker-compose.yml` or use
`--scale` at runtime.

**Bring up the stack:**

```bash
docker compose up -d
# Load seeds from the host:
docker compose exec worker mcosint worker seeds <UUID>
# Watch logs:
docker compose logs -f worker
```

### Submitting Seeds and Monitoring Progress

```powershell
# Add seeds (idempotent — ON CONFLICT DO NOTHING)
mcosint worker seeds 069a79f4-44e9-4726-a5be-fca90e38aaf5 --depth 0

# Check queue and node status
mcosint worker status

# Force-recrawl all nodes by resetting the queue
# (update crawl_queue SET status='pending' WHERE status='done')
```

### Future: REST Coordinator API (Not Implemented)

A future `mcosint coordinator` FastAPI service could expose:
- `POST /seeds` — bulk seed ingestion with deduplication
- `GET /status` — JSON dashboard of queue depth and node health
- `POST /nodes/{node_id}/pause` — remote pause of a worker node

This is intentionally left unimplemented for Phase 4. The DB-only model is
sufficient for deployments up to ~50 VMs; the coordinator API would add value
for larger fleets or when a web UI is needed.
