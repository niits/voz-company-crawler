# Crawl Pipeline

Crawls all pages of a XenForo thread on Voz.vn and loads post records into PostgreSQL via dlt.

---

## Data Flow

```
FlareSolverr (Chrome sidecar)
    │  HTTP GET via /v1 API
    ▼
voz_page_source (dlt source)
    │  yields post dicts
    ▼
dlt pipeline  ──write_disposition="merge"──►  raw.voz__posts (PostgreSQL)
    │
    ▼ (managed by Dagster)
DagsterDltResource
    │  emits MaterializeResult + row counts
    ▼
Dagit Partitions tab (green = materialized)
```

---

## Components

### FlareSolverr Integration

`sources/voz_thread.py` — `fetch_via_flaresolverr(url, flaresolverr_url, timeout)`

Voz.vn runs Cloudflare Bot Management with JS challenges. FlareSolverr operates a real
Chrome browser (via undetected-chromedriver) that solves the challenge and returns the
final HTML response. The crawler never speaks directly to Voz.vn.

```
POST {FLARESOLVERR_URL}/v1
  { "cmd": "request.get", "url": "...", "maxTimeout": 60000 }

Response: { "solution": { "status": 200, "response": "<html>..." } }
```

Cloudflare blocks are detected by checking status codes (403/429/503) plus HTML
markers (`"Just a moment"`, `"cf-browser-verification"`) in the first 2000 characters.
A detected block raises `RuntimeError`, surfacing as a failed Dagster run.

**Environment**: `FLARESOLVERR_URL` (default `http://localhost:8191` local,
`http://flaresolverr:8191` inside Docker).

---

### HTML Parser

`utils/html_parser.py` — `extract_posts(html) -> list[dict]`

Parses XenForo thread HTML with BeautifulSoup (`lxml`). Each post is an
`<article class="message" data-author="..." id="js-post-XXXXX">` element.

Extracted fields per post:

| Field | Source |
|---|---|
| `post_id_on_site` | `article#js-post-{ID}` |
| `post_position` | 1-based index within page |
| `author_username` | `article[data-author]` |
| `author_id_on_site` | `a.username[data-user-id]` |
| `posted_at_raw` | `time.u-dt[datetime]` — ISO 8601 with TZ offset |
| `raw_content_html` | `div.bbWrapper` inner HTML |
| `raw_content_text` | `div.bbWrapper` plain text |

`utils/pagination.py` — `discover_total_pages(html) -> int`

Reads the XenForo pagination nav (`.pageNav`). For large threads XenForo renders
a windowed nav like `1 2 3 … 48 49 50`; the function finds the highest page number
across all visible `li.pageNav-page a` links.

---

### dlt Source

`sources/voz_thread.py` — `voz_page_source(page_url, flaresolverr_url, ...)`

Single-page, stateless dlt source. No cursor — idempotency is handled entirely by
`write_disposition="merge"` keyed on `post_id_on_site`. Posts with a non-integer or
missing `post_id_on_site` are silently dropped.

The source is called with a placeholder URL at module-load time (required by
`@dlt_assets` to discover asset keys without HTTP). The real source is constructed
inside the asset function body at run time.

---

### Dagster Asset

`defs/ingestion/assets.py` — `voz_page_posts_assets`

`@dlt_assets` decorator wraps the dlt source as a partitioned Dagster asset.
Each partition = one page number (e.g. `"42"`).

Key implementation details:
- The injected `DagsterDltResource` is named `dagster_dlt` (not `dlt`) to avoid
  shadowing `import dlt`.
- After `yield from dagster_dlt.run(...)`, the SQLAlchemy engine is disposed in a
  `try/finally` block to release connection pool resources. Without this, each run
  leaks up to 15 connections, exhausting PostgreSQL's `max_connections`.

---

### Jobs

`defs/ingestion/jobs.py`

| Job | Type | Purpose |
|---|---|---|
| `discover_pages_job` | `@job` (op-based) | Fetches page 1, discovers `total_pages`, registers new partition keys |
| `crawl_page_job` | `define_asset_job` | Materializes one page partition |

`discover_pages_op` is idempotent — it only adds partition keys that don't exist yet.
On Cloudflare block it raises `RuntimeError` so the run shows as failed in Dagit.

---

### Sensors

`defs/ingestion/sensors.py`

| Sensor | Type | Interval | Role |
|---|---|---|---|
| `voz_discover_sensor` | `@sensor` | Every 6 hours | Triggers `discover_pages_job` |
| `voz_crawl_sensor` | `@run_status_sensor` | On `discover_pages_job` SUCCESS | Queues crawl runs for unmaterialized pages |

`voz_crawl_sensor` partition scheduling:
- **Historical pages**: `run_key=f"page-{N}"` — stable key, fires exactly once per page.
- **Last page**: `run_key=f"page-{N}-{YYYY-MM-DD}"` — daily key, re-fires every day to
  accumulate new posts as the thread grows.

Splitting discovery and crawl into two sensors ensures a Cloudflare block on discovery
does not affect the crawl queue, and eliminates any race between partition registration
and `RunRequest` submission.

---

## Database Output

Table: `raw.voz__posts` (schema managed by dlt)

| Column | Type | Notes |
|---|---|---|
| `post_id_on_site` | `bigint` | Primary key — globally unique Voz post ID |
| `page_url` | `text` | Source page URL |
| `author_username` | `text` | |
| `author_id_on_site` | `text` | |
| `posted_at_raw` | `text` | ISO 8601, e.g. `2024-01-15T10:30:00+07:00` |
| `raw_content_html` | `text` | Full bbWrapper HTML |
| `raw_content_text` | `text` | Plain text extracted from HTML |
| `_dlt_id` | `text` | dlt internal row ID |
| `_dlt_load_id` | `text` | dlt load batch reference |

dlt also creates `raw._dlt_loads` and `raw._dlt_pipeline_state` tracking tables.
These are internal and should not be read directly.

---

## Operations

### First Deploy / Bootstrap

```bash
# 1. Ensure FlareSolverr is running
docker compose up flaresolverr -d

# 2. Start Dagster
uv run dagster dev

# 3. In Dagit: Launchpad → discover_pages_job → Launch Run
#    This registers all existing pages as partition keys.

# 4. voz_crawl_sensor fires automatically after discover_pages_job succeeds,
#    queueing crawl runs for all unmaterialized pages.
```

### Monitoring

- **Partitions tab** on `voz_page_posts` asset: green = materialized, grey = pending.
- Each run logs `Crawling page N: <url>` and the dlt row-count summary.

### Handling Cloudflare Blocks

- Block during discovery → `discover_pages_job` fails; sensor skips next evaluation.
  Recovery: wait for block to lift, then relaunch `discover_pages_job` manually.
- Block during crawl → individual partition run fails. Recovery: re-run that partition
  from the Partitions tab, or wait for `voz_crawl_sensor` to re-queue it the next day.

### Manual Backfill

Re-run any specific page from Dagit's Partitions tab. The `write_disposition="merge"`
ensures re-running a page is safe — existing posts are upserted, not duplicated.
