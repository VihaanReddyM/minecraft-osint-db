# minecraft-osint-db

A modular Python CLI toolkit for Minecraft OSINT workflows. Crawls the NameMC friends API with
multi-threaded BFS, persistent PostgreSQL storage, FlareSolverr Cloudflare bypass, and Discord
notifications. Designed to grow toward distributed, multi-IP crawling at scale.

## Quickstart

### 1. Install

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -U pip
.\.venv\Scripts\pip install -e .
```

### 2. Configure

```powershell
mcosint config init
copy .env.example .env
# Edit .env and set DATABASE_URL
```

### 3. Initialize the database schema

```powershell
mcosint db init
```

### 4. Run a crawl

```powershell
# DB-backed multi-threaded crawl (recommended)
mcosint namemc crawl-friends-db 57a0be0c-30fa-4ab0-aa2b-40091dbdf7e0 ^
  --init-db ^
  --threads 5 ^
  --max-depth 2 ^
  --max-total-requests 100

# Threaded mesh crawl (also writes JSON output)
mcosint namemc friends-mesh 57a0be0c-30fa-4ab0-aa2b-40091dbdf7e0 ^
  --max-depth 2 ^
  --no-use-flaresolverr ^
  --out output/mesh.json
```

## Commands

### `mcosint namemc crawl-friends-db`

Multi-threaded DB-persisting crawl. Resumes from the last run automatically.

```
Options:
  START_UUID                   Starting player UUID
  --uuids-file FILE            Text file with one UUID per line (# comments supported)
  --threads INT                Worker thread count [default: 5]
  --request-delay FLOAT        Seconds to wait after each successful request [default: 1.0]
  --max-depth INT              BFS depth limit [default: 2]
  --max-total-requests INT     Total API call budget [default: 100]
  --max-friends-per-user INT   Max friends to expand per player [default: 50]
  --rate-limit-backoff-seconds FLOAT  Backoff on 429 [default: 10.0]
  --max-retries-per-uuid INT   Retries per UUID on error [default: 3]
  --force-recrawl              Ignore DB cache and re-fetch already-crawled players
  --init-db                    Create schema before crawling
```

### `mcosint namemc friends-mesh`

Threaded BFS that builds a mesh dict and writes JSON. Also persists to DB.
Requires `DATABASE_URL`.

```
Options:
  START_UUID                   Starting player UUID (required)
  --max-depth INT              BFS depth limit [default: 2]
  --delay-seconds FLOAT        Per-request sleep [default: 1.0]
  --max-total-calls INT        Optional API call cap
  --max-friends-per-user INT   Optional per-player branching cap
  --out PATH                   Output file [default: output/friend_mesh.json]
  --use-flaresolverr / --no-use-flaresolverr  Route via FlareSolverr [default: enabled]
  --flaresolverr-url TEXT      Override FlareSolverr base URL
  --threads INT                Worker thread count (env: THREAD_COUNT) [default: 5]
  --rate-limit-backoff-seconds FLOAT  (env: RATE_LIMIT_BACKOFF_SECONDS) [default: 10]
  --max-retries-per-uuid INT   (env: MAX_RETRIES_PER_UUID) [default: 3]
  --discord-webhook-url TEXT   Override Discord webhook (env: MCOSINT_DISCORD_WEBHOOK_URL)
  --init-db                    Create schema before crawling
```

### `mcosint http batch-get`

Fetch many URLs concurrently. Writes response bodies to files and produces an `index.json`.

```powershell
mcosint http batch-get .\urls.txt --concurrency 50 --out-dir output\http_batch
```

### `mcosint flaresolverr health`

Check if a FlareSolverr instance is reachable.

```powershell
mcosint flaresolverr health --flaresolverr-url http://localhost:8191
```

## Resume behavior

Both crawl commands are restart-safe:
- If a player UUID was successfully crawled before, the crawler reads their friends from
  the DB instead of calling the API again.
- Pass `--force-recrawl` to ignore cached results and re-fetch.

## Multiple seeds

```powershell
# seeds.txt: one UUID per line, # comments allowed
mcosint namemc crawl-friends-db --uuids-file seeds.txt --init-db --threads 10 --max-depth 3
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | PostgreSQL connection string (required for all crawl commands) |
| `DB_POOL_MIN_SIZE` | `1` | Connection pool minimum size |
| `DB_POOL_MAX_SIZE` | `thread_count + 2` | Connection pool maximum size |
| `THREAD_COUNT` | `5` | Worker threads for `friends-mesh` |
| `RATE_LIMIT_BACKOFF_SECONDS` | `10` | 429 backoff for `friends-mesh` |
| `MAX_RETRIES_PER_UUID` | `3` | Per-UUID retry limit for `friends-mesh` |
| `MCOSINT_FLARESOLVERR_URL` | `http://localhost:8191` | FlareSolverr base URL |
| `MCOSINT_USE_FLARESOLVERR` | `false` | Enable FlareSolverr globally for `http get` |
| `MCOSINT_DISCORD_WEBHOOK_URL` | — | Discord webhook for crawl notifications |

> **Note**: `THREAD_COUNT`, `RATE_LIMIT_BACKOFF_SECONDS`, and `MAX_RETRIES_PER_UUID` are
> only used by `friends-mesh` as fallback defaults. All crawler parameters can also be set
> via CLI flags, which take precedence.

## FlareSolverr (Cloudflare bypass)

Start FlareSolverr via Docker:

```powershell
docker compose up -d
mcosint flaresolverr health
```

`friends-mesh` routes via FlareSolverr by default (`--use-flaresolverr`). Use
`--no-use-flaresolverr` to bypass it. For `http get` and `http batch-get`, pass
`--use-flaresolverr` explicitly or set `MCOSINT_USE_FLARESOLVERR=true`.

## Architecture overview

```
CLI (Typer)
  ├── commands/config_cmds.py     — config init/path
  ├── commands/db_cmds.py         — db init (schema creation)
  ├── commands/http_cmds.py       — http get / batch-get
  ├── commands/flaresolverr_cmds.py — flaresolverr health
  └── commands/namemc_cmds.py     — friends-mesh / crawl-friends-db
         │
         ├── crawl/namemc_friends.py    — CrawlConfig, crawl_namemc_friends_to_db()
         │     └── [workers create own httpx.Client per thread]
         │
         ├── graph/friend_mesh.py       — build_friend_mesh_threaded()
         │     └── services/namemc_threaded.py  — ThreadedNameMCClient (FlareSolverr)
         │
         └── db/
               ├── connection.py   — ConnectionPool management
               ├── schema.py       — DDL (players, friendships tables)
               └── operations.py   — CRUD + tenacity retry decorators
```

**Two crawl paths coexist** (planned convergence in a future release):
- `crawl-friends-db` → `crawl_namemc_friends_to_db`: per-worker `httpx.Client`, no FlareSolverr
- `friends-mesh` → `build_friend_mesh_threaded`: shared `ThreadedNameMCClient`, FlareSolverr supported

## Known limitations

- **Single outbound IP**: All workers share the host machine's IP. High thread counts or
  aggressive crawling will trigger 429 rate limits. Per-worker SOCKS5 proxy isolation is
  planned (Phase 1 of roadmap).
- **In-process BFS queue**: The crawl frontier lives in process memory. A crash loses
  queued-but-not-yet-crawled UUIDs (already-crawled nodes are safe in DB). A
  Postgres-backed durable queue is planned (Phase 3 of roadmap).
- **`crawl-friends-db` has no FlareSolverr support**: FlareSolverr routing is only
  available via `friends-mesh`. This will be unified in a future release.

## Development

```powershell
pip install -e ".[dev]"

# Lint
ruff check src/ tests/
ruff format src/ tests/

# Type check
basedpyright

# Tests
pytest tests/
pytest tests/test_friend_mesh.py::test_build_friend_mesh_respects_limits  # single test
```

## Project layout

```
src/mcosint/
  cli.py               — Typer root app + .env loading
  config.py            — AppConfig (FlareSolverr + HTTP settings)
  commands/            — CLI command modules
  crawl/               — DB-persisting crawler engine
  graph/               — BFS mesh builder
  http/                — AsyncFetcher + FlareSolverr client
  services/            — NameMC API clients (sync + async)
  db/                  — PostgreSQL pool, schema, CRUD operations
  storage/             — JSON output helper
  notify/              — Discord webhook
  util/                — UUID normalization
tests/                 — pytest test suite
scripts/               — FlareSolverr Docker start/stop helpers
docker-compose.yml     — FlareSolverr service
```
