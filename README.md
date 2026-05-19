# minecraft-osint-db

A modular Python CLI skeleton for Minecraft OSINT workflows.

## Quickstart

### 1) Create a venv + install (editable)

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -U pip
.\.venv\Scripts\pip install -e .
```

### 2) Initialize config

```powershell
mcosint config init
```

### 3) Build a NameMC friend mesh

```powershell
mcosint namemc friends-mesh 57a0be0c-30fa-4ab0-aa2b-40091dbdf7e0 --max-depth 2 --max-total-calls 10 --max-friends-per-user 3 --delay-seconds 1.5 --out output/friend_mesh_sample.json
```

## Bulk HTTP (lots of API calls)

If you want to blast a lot of GET requests, put one URL per line in a text file and run:

```powershell
mcosint http batch-get .\urls.txt --concurrency 50 --out-dir output\http_batch
```

It will write response bodies to files and generate an `index.json` mapping URLs to output files.

## FlareSolverr (optional)

If you want to route requests through FlareSolverr (useful for Cloudflare-protected targets), start it via Docker:

```powershell
docker compose up -d
```

Then use `--use-flaresolverr`:

```powershell
mcosint http get https://example.com --use-flaresolverr
mcosint namemc friends-mesh <UUID> --use-flaresolverr
```

You can also set environment variables (see `.env.example`):

- `MCOSINT_FLARESOLVERR_URL`
- `MCOSINT_USE_FLARESOLVERR`

## Database-backed crawl (PostgreSQL)

1) Set `DATABASE_URL` (see `.env.example`).

2) Initialize schema:

```/dev/null/commands.txt#L1-1
mcosint db init
```

3) Run the multi-threaded, DB-persisting crawl:

```/dev/null/commands.txt#L1-9
mcosint namemc crawl-friends-db 57a0be0c-30fa-4ab0-aa2b-40091dbdf7e0 \
  --init-db \
  --threads 5 \
  --request-delay 1.0 \
  --max-depth 2 \
  --max-total-requests 100 \
  --max-friends-per-user 50 \
  --rate-limit-backoff-seconds 10 \
  --max-retries-per-uuid 3
```

### Resume-friendly behavior

This crawler is designed to be *restart-safe*:
- If a UUID was already crawled previously, the crawler **does not call the API again** (unless you pass `--force-recrawl`).
- Instead it reads the stored edges from `friendships` and continues BFS expansion from the DB.

### Multiple seeds from a text file

Create `seeds.txt` (one UUID per line, `#` comments allowed), then:

```/dev/null/commands.txt#L1-2
mcosint namemc crawl-friends-db --uuids-file seeds.txt --init-db
mcosint namemc crawl-friends-db --uuids-file seeds.txt --threads 10 --max-depth 3
```

## Project layout

- `src/mcosint/cli.py`: CLI entrypoint
- `src/mcosint/commands/`: individual command modules
- `src/mcosint/http/`: HTTP + FlareSolverr integration
- `src/mcosint/services/`: API wrappers (e.g., NameMC)
- `src/mcosint/graph/`: graph/mesh builders
- `src/mcosint/storage/`: output helpers

## Notes

This is a skeleton meant to grow:
- add more services under `src/mcosint/services/`
- add bulk/batched crawling commands under `src/mcosint/commands/`
- add persistence (SQLite, etc.) under `src/mcosint/storage/`
