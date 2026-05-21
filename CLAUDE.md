# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**minecraft-osint-db** is a Python CLI toolkit for Minecraft OSINT workflows. It crawls Minecraft player social networks via the NameMC friends API, building "friend meshes" (social graphs) with persistent PostgreSQL storage, multithreaded BFS, FlareSolverr Cloudflare bypass, and Discord notifications. The project is heading toward distributed, multi-IP crawling via OpenVPN + SOCKS5 proxies.

## Commands

### Setup
```powershell
python -m venv .venv
.\.venv\Scripts\pip install -e .
mcosint config init
```

### Development
```powershell
# Lint
ruff check src/ tests/
ruff format src/ tests/

# Type check
basedpyright

# Run all tests
pytest tests/

# Run a single test
pytest tests/test_friend_mesh.py::test_build_friend_mesh_respects_limits
```

### CLI Usage
```powershell
# DB-backed multi-threaded crawl (recommended)
mcosint namemc crawl-friends-db <UUID> --init-db --threads 5 --max-depth 2

# Threaded mesh crawl with JSON output
mcosint namemc friends-mesh <UUID> --no-use-flaresolverr --max-depth 2 --out output/mesh.json

# Initialize DB schema only
mcosint db init

# Start FlareSolverr (Docker) and check health
docker compose up -d
mcosint flaresolverr health
```

### Environment
Copy `.env.example` to `.env`. Key variables:
- `DATABASE_URL` — PostgreSQL connection string (required for all crawl commands)
- `MCOSINT_FLARESOLVERR_URL` — FlareSolverr base URL (default: `http://localhost:8191`)
- `THREAD_COUNT`, `RATE_LIMIT_BACKOFF_SECONDS`, `MAX_RETRIES_PER_UUID` — fallback defaults for `friends-mesh` only; resolved at runtime after `.env` loading
- `MCOSINT_USE_FLARESOLVERR` — enables FlareSolverr globally for `http get` only; `friends-mesh` has its own `--use-flaresolverr` flag
- `MCOSINT_DISCORD_WEBHOOK_URL` — Discord notifications

## Architecture

The project has four layers:

```
CLI Layer (Typer) → cli.py
Commands Layer    → commands/command_*.py
Domain Logic      → services/, graph/, crawl/
Infrastructure    → http/, db/, storage/, notify/
```

### Key Modules

**`crawl/namemc_friends.py`** — `crawl_namemc_friends_to_db()`: the main DB crawler used by `crawl-friends-db`. Each worker creates its own `httpx.Client`. Uses `GlobalCooldown` (shared 429 backoff) and `RequestBudget` (total request cap). No FlareSolverr support in this path.

**`services/namemc_threaded.py`** — `ThreadedNameMCClient`: sync NameMC HTTP client with FlareSolverr support, 429 detection, and `Retry-After` parsing. Used by `friends-mesh`.

**`graph/friend_mesh.py`** — `build_friend_mesh_threaded()`: threaded BFS used by `friends-mesh`. All workers share one `ThreadedNameMCClient` (shared socket pool / single IP). Also contains `build_friend_mesh()` (async, used only in tests).

**`db/operations.py`** — All DB queries. `persist_namemc_friend_response()` wraps player upsert + friend upserts + edge inserts in one transaction. UUIDs are sorted before insert to reduce deadlock probability.

**`db/schema.py`** — DDL for `players` (with `friends_crawled_at` resume marker and `min_depth`) and `friendships` (edge table).

**`db/connection.py`** — Global singleton `ConnectionPool`. `get_pool(cfg)` lazily creates it once per process.

**`services/namemc.py`** — Async `NameMCClient` wrapping `AsyncFetcher`. Currently **not wired to any CLI command** (orphaned; planned for future unified crawl engine).

### Two Crawl Paths (Current State)

| | `crawl-friends-db` | `friends-mesh` |
|---|---|---|
| Engine | `crawl_namemc_friends_to_db()` | `build_friend_mesh_threaded()` |
| HTTP client | Per-worker `httpx.Client` | Shared `ThreadedNameMCClient` |
| FlareSolverr | Not supported | Supported |
| `force-recrawl` | Supported | Not supported |
| Output | Metrics dict | JSON mesh file |

### Resume / Caching Pattern

The crawler marks completion by writing `friends_crawled_at = NOW()` to the `players` table. On restart, `is_player_friends_crawled()` returns `True` for completed nodes — BFS reads edges from DB instead of calling the API. Pass `--force-recrawl` to override (only `crawl-friends-db`).

### Concurrency

Two concurrency patterns coexist:
1. **`crawl-friends-db`**: `ThreadPoolExecutor` + `queue.Queue`. Each worker creates its own `httpx.Client`. Shared state: `seen_depth` dict (protected by `seen_lock`), `GlobalCooldown`, `RequestBudget`.
2. **`friends-mesh`**: `ThreadPoolExecutor` + `queue.Queue`. All workers share one `ThreadedNameMCClient` (one `httpx.Client` → same outbound IP). Shared state: `visited` set, `mesh` dict (both locked).

### Known Architecture Issues (Planned for Resolution)

- **Single outbound IP**: Both paths use the host's IP. Per-worker SOCKS5/VPN proxy isolation is the next major feature.
- **Two divergent crawl paths**: Will be unified into a single `CrawlEngine` class in a future refactor.
- **Global `_pool` singleton** in `db/connection.py`: Makes testing harder; planned for dependency injection.
- **`load_crawl_config_from_env()`** in `crawl/namemc_friends.py`: Exists but is never called from CLI (dead code path). CLI passes values directly.

## Code Style

- Line length: 100 (ruff)
- Ruff rules enabled: E, F, I, UP, B
- Python ≥ 3.10 (pyproject.toml), type annotations on all public functions
- Source root: `src/mcosint/` — all imports use the `mcosint.*` namespace
- `typer.Option()` in function signatures suppresses ruff B008 (configured in pyproject.toml)
- Env vars that serve as CLI defaults must be resolved **in the function body** (after `.env` loading), not in `typer.Option(default=...)` (import-time evaluation)
