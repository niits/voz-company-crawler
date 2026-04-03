# Voz Company Crawler

Data pipeline crawling IT company reviews from the Voz.vn forum.

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Dagster + dagster-dlt |
| Ingestion | dlt (`dlt[postgres]`) |
| Storage | PostgreSQL |
| HTTP / CF bypass | FlareSolverr (Chrome headless sidecar) |
| Transformation | dbt (scaffold ready) |
| Dependency mgmt | uv |

## Project Layout

```
voz_crawler/
  core/
    ingestion/
      html_source/
        html_parser.py    BeautifulSoup post extractor (extract_posts)
        pagination.py     XenForo URL builder + page count discovery
  defs/
    assets/ingestion.py   @dlt_assets: voz_page_posts_assets (partitioned)
    jobs/ingestion.py     crawl_page_job + discover_pages_job
    ops/ingestion.py      discover_pages_op (registers dynamic partitions)
    resources/
      crawler_resource.py CrawlerResource (thread_url, flaresolverr_url, timeout)
      postgres_resource.py PostgresResource (assembles SQLAlchemy URL)
    sensors/ingestion.py  voz_discover_sensor + voz_crawl_sensor
  dlt/
    sources/voz_thread.py dlt source: voz_page_source + FlareSolverr fetch helpers
  definitions.py          Dagster Definitions entry point
docs/
  design-decisions.md     Architectural decisions with rationale
```

## Running Locally

```bash
cp .env.example .env      # fill in real values
uv sync
docker compose up postgres pgadmin -d   # Postgres :5432, pgAdmin :5050
uv run dagster dev                      # Dagit UI :3000
```

`dagster dev` loads `.env` automatically — no dotenv needed.

```bash
uv run ruff check . && uv run ruff format .   # lint + format
uv run pytest                                  # tests
uv add <package>                               # add dependency
```

## Database Schema

| Schema | Owner | Purpose |
|---|---|---|
| `raw` | dlt | Raw ingestion — never hand-edited |
| `staging` | dbt | Normalized views (`stg_*`) |
| `marts` | dbt | Final tables (`dim_*`, `fct_*`) |

Key table (auto-created by dlt on first run):

| Table | Primary key | Description |
|---|---|---|
| `raw.voz__posts` | `post_id_on_site` | One row per forum post, globally unique |

## Crawl Strategy

Two sensors work in tandem:

**`voz_discover_sensor`** (every 6 hours) triggers `discover_pages_job`:
1. Fetches page 1 via FlareSolverr to discover `total_pages`
2. Registers new page numbers into `DynamicPartitionsDefinition("voz_pages")`

**`voz_crawl_sensor`** (`run_status_sensor`, fires after `discover_pages_job` succeeds):
1. Checks Dagster's asset catalog for already-materialized partitions
2. Queues `RunRequest` for each unmaterialized page + the last page (always re-crawled daily)

Each run materializes one partition → calls `voz_page_source(page_url)` → parses posts → merges into `raw.voz__posts` by `post_id_on_site`. No dlt cursor state needed.

On Cloudflare block (403/429/503), the asset raises `RuntimeError`. Safe — next sensor evaluation retries automatically.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `VOZ_THREAD_URL` | — | Full URL to the Voz thread |
| `FLARESOLVERR_URL` | — | FlareSolverr sidecar URL (e.g. `http://flaresolverr:8191`) |
| `HTTP_DELAY_SECONDS` | `2` | Retained for env compat (unused in asset) |
| `HTTP_TIMEOUT_SECONDS` | `60` | Per-request timeout |
| `OPENAI_API_KEY` | — | Future LLM extraction |

## Adding dbt Models

1. Copy `dbt/profiles.yml.example` → `~/.dbt/profiles.yml`
2. Add models under `dbt/models/staging/` or `dbt/models/marts/`
3. Sources pre-defined in `dbt/models/sources.yml` pointing at `raw`
4. `cd dbt && uv run dbt run`

## Design Decisions

See [docs/design-decisions.md](docs/design-decisions.md).
