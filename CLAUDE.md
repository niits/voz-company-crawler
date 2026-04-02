# Voz Company Crawler

Data pipeline crawling IT company reviews from the Voz.vn forum.

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Dagster + dagster-dlt |
| Ingestion | dlt (`dlt[postgres]`) |
| Storage | PostgreSQL |
| HTTP / CF bypass | FlareSolverr (sidecar) |
| Transformation | dbt (scaffold ready) |
| Dependency mgmt | uv |

## Project Layout

```
voz_crawler/
  definitions.py              Thin re-export of defs (entry point for Dagster)
  defs/
    __init__.py               Assembles Definitions from sub-modules
    ingestion/
      assets.py               @dlt_assets: voz_page_posts_assets (partitioned)
      jobs.py                 crawl_page_job, discover_pages_job, discover_pages_op
      sensors.py              voz_discover_sensor, voz_crawl_sensor
      resources.py            PostgresResource, CrawlerResource + pre-configured instances
      partitions.py           DynamicPartitionsDefinition("voz_pages")
    transformation/
      __init__.py             Placeholder for future dbt definitions
  sources/
    voz_thread.py             dlt source: voz_page_source (single-page, stateless)
  utils/
    html_parser.py            BeautifulSoup post extractor
    pagination.py             XenForo URL builder + page count discovery
docs/
  README.md                   Index of all documentation
  design-decisions.md         Why key trade-offs were made (alternatives rejected)
  features/
    crawl-pipeline.md         End-to-end crawl pipeline: components, schema, operations
    reply-graph.md            Reply graph pipeline design (ArangoDB, embeddings, LLM)
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

A **sensor** (`voz_crawl_sensor`) runs hourly:
1. Fetches page 1 to discover `total_pages`
2. Adds new page numbers to `DynamicPartitionsDefinition("voz_pages")`
3. Checks Dagster's asset catalog for already-materialized partitions
4. Queues `RunRequest` for each unmaterialized page + the last page (always re-crawled daily)

Each run materializes one partition → calls `voz_page_source(page_url)` → parses posts → merges into `raw.voz__posts` by `post_id_on_site`. No dlt cursor state needed.

On Cloudflare block (403/429/503), the sensor returns `SkipReason`; the asset raises `RuntimeError`. Both are safe — next evaluation retries automatically.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `VOZ_THREAD_URL` | — | Full URL to the Voz thread |
| `HTTP_DELAY_SECONDS` | `2` | Retained for env compat (unused in asset) |
| `HTTP_TIMEOUT_SECONDS` | `30` | Per-request timeout |
| `OPENAI_API_KEY` | — | Future LLM extraction |

## Adding dbt Models

1. Copy `dbt/profiles.yml.example` → `~/.dbt/profiles.yml`
2. Add models under `dbt/models/staging/` or `dbt/models/marts/`
3. Sources pre-defined in `dbt/models/sources.yml` pointing at `raw`
4. `cd dbt && uv run dbt run`

## Documentation

See [docs/README.md](docs/README.md) for the full index.
