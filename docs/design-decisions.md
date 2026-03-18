# Design Decisions

Architectural and implementation decisions for the Voz Company Crawler.

---

### 1. dlt over custom psycopg2 ingestion

**Decision**: Use dlt (`dlt[postgres]`) as the ingestion layer instead of raw psycopg2 with custom upsert SQL.

**Rationale**:
- dlt handles schema creation, column type inference, upserts (`ON CONFLICT … DO UPDATE`), pipeline state, and load history out of the box — eliminating ~200 lines of boilerplate.
- `write_disposition="merge"` + `primary_key` replaces hand-written `execute_values` + `ON CONFLICT` SQL.

**Trade-off**: dlt owns the `raw` schema DDL, so table names follow dlt conventions (`voz__posts` with source prefix).

---

### 2. dagster-dlt integration (`@dlt_assets`) with dynamic partitions

**Decision**: Use `dagster-dlt`'s `@dlt_assets` decorator with `DynamicPartitionsDefinition`, one partition per page number.

**Rationale**:
- Each page becomes an independent Dagster asset partition — materialization history, re-runs, and backfills are handled natively by Dagster.
- `DagsterDltResource` handles context propagation, structured logging, and emits `MaterializeResult` with row counts automatically.
- Progress is visible in Dagit's Partitions tab: green = materialized, grey = pending.

**Placeholder source pattern**: `@dlt_assets` needs a `dlt_source` at decoration time (module-load) to discover asset keys. dlt source construction is lazy — calling `voz_page_source(page_url="https://placeholder")` builds the resource graph without HTTP requests. The real source and pipeline are created inside the function body and passed to `dagster_dlt.run()` as runtime overrides.

**Naming note**: The injected `DagsterDltResource` parameter is named `dagster_dlt` (not `dlt`) to avoid shadowing `import dlt`. The resource key in `Definitions` must match: `"dagster_dlt": DagsterDltResource()`.

---

### 3. FlareSolverr for Cloudflare bypass

**Decision**: Delegate all HTTP fetching to a FlareSolverr sidecar service instead of using an in-process HTTP library.

**Rationale**:
- Voz.vn uses Cloudflare Bot Management with JS challenges. FlareSolverr runs a real Chrome browser (via undetected-chromedriver) that executes the challenge and returns the solved response — no library-level TLS or JS emulation needed.
- All crawler code simply does `requests.post(flaresolverr_url/v1, json=payload)` — HTTP complexity is fully contained in the sidecar.
- FlareSolverr is declared as a service in `docker-compose.yml` (`ghcr.io/flaresolverr/flaresolverr:latest`, port 8191), co-located with the pipeline.

**Trade-off**: Adds an external service dependency. `FLARESOLVERR_URL` must be reachable at runtime (default `http://localhost:8191` for local dev, `http://flaresolverr:8191` inside Docker).

---

### 4. Dagster-native incrementality: two-sensor pipeline + dynamic partitions

**Decision**: Track crawl progress via Dagster asset partition materialization history, not dlt internal state. Discovery and crawl triggering are split into two sensors with distinct responsibilities.

**Previous approach** (removed): two-level dlt cursors — `last_page_crawled` in `dlt.current.resource_state()` (page level) + `dlt.sources.incremental("post_id_on_site")` (post level).

**Current approach — two-stage pipeline**:

1. **`voz_discover_sensor`** (regular sensor, 6-hour interval) → triggers `discover_pages_job`.
2. **`discover_pages_op`** (op inside the job) → fetches page 1 via FlareSolverr, discovers `total_pages`, registers new partition keys via `instance.add_dynamic_partitions()`. Raises `RuntimeError` on Cloudflare block so the run shows as failed.
3. **`voz_crawl_sensor`** (`run_status_sensor`, fires on `discover_pages_job` SUCCESS) → reads all registered partition keys, calls `instance.get_materialized_partitions(asset_key)` to find un-materialized pages, then submits `RunRequest`s to `crawl_page_job`:
   - Historical pages: `run_key=f"page-{N}"` — stable key, fires exactly once.
   - Last page: `run_key=f"page-{N}-{YYYY-MM-DD}"` — daily key, re-fires every day to accumulate new posts.
4. The asset key used for materialization checks is derived at module load from `voz_page_posts_assets.keys` — no hardcoded translator naming convention.

**Why split into two sensors instead of one**:
- The discovery sensor can fail on Cloudflare block without affecting the crawl sensor's logic.
- `run_status_sensor` guarantees the crawl queue is only computed after partition registration completes — no race condition between partition creation and `RunRequest` submission.
- `discover_pages_job` can be run manually to bootstrap partitions on first deploy.

**Per-page dlt asset**: stateless fetch → parse → merge by `post_id_on_site`. No cursor.

**Progress visibility**: Dagit Partitions tab shows green = materialized, grey = pending. Backfill any specific page by re-running that partition from Dagit.

---

### 5. No RetryPolicy; failures bubble naturally

**Decision**: Do not configure `RetryPolicy` on dlt assets; let failures propagate to Dagster for manual retry.

**Rationale**:
- Cloudflare blocks are sustained; retrying after 60 s rarely helps.
- Failed historical pages can be re-run from Dagit's Partitions view; last page re-queues automatically the next day.

**Alternative considered**: `RetryPolicy(max_retries=2, delay=60)` — removed because block durations are unpredictable.

---

### 6. No python-dotenv dependency

**Decision**: Remove `python-dotenv`; load `.env` exclusively via Dagster's native mechanism.

**Rationale**:
- `dagster dev` automatically loads `.env` from the project root (Dagster 1.5+).
- In Docker, variables are injected by `docker-compose.yml` directly.

---

### 7. Three-schema design (raw / staging / marts)

**Decision**: Separate ingestion schema (`raw`, dlt-owned) from transformation schemas (`staging`, `marts`, dbt-owned).

**Rationale**:
- `raw` is upsert-only — a stable contract for dbt to read from.
- dbt models never touch `raw`; transformation changes don't risk raw data.

---

### 8. sql/init.sql only pre-creates staging and marts schemas

**Decision**: `sql/init.sql` creates only `staging` and `marts`; `raw` is fully managed by dlt.

**Rationale**: dlt creates the `raw` schema and tracking tables (`_dlt_loads`, `_dlt_pipeline_state`) on first run. Manually pre-creating them would conflict with dlt's DDL management.

---

### 9. ConfigurableResource + EnvVar instead of os.environ

**Decision**: Use `ConfigurableResource` and `EnvVar` / `EnvVar.int()` instead of `os.environ` in asset bodies.

**Rationale**:
- `EnvVar` resolves at runtime — Dagster webserver/daemon loads the module without requiring env vars at startup.
- `ConfigurableResource` schema is visible in Dagit's resource configuration panel.
- Resource injection by parameter name makes asset functions testable with mock resources.

**Current resources**:
- `PostgresResource`: `user`, `password`, `host`, `port`, `db` → `.url` property builds SQLAlchemy connection string.
- `CrawlerResource`: `thread_url`, `flaresolverr_url`, `http_timeout_seconds`, `http_delay_seconds` (str, retained for env compat, unused in asset). No `EnvVar.float()` exists, so `http_delay_seconds` is typed `str` with a `.delay` property converting to `float`.

---

### 10. PostgreSQL connection management: max_connections=300 + engine disposal

**Decision**: Set PostgreSQL `max_connections=300` in docker-compose and explicitly dispose the dlt pipeline's SQLAlchemy engine after each asset run.

**Problem**: Default PostgreSQL `max_connections=100` is exhausted when Dagster runs many sequential page partitions. Each `dlt.pipeline(...)` call creates a new SQLAlchemy `QueuePool` (default: pool_size=5, max_overflow=10 → up to 15 connections). Without explicit disposal, these pools linger until GC while Dagster's own storage also holds connection pools (daemon + webserver).

**Fix 1 — docker-compose**: `command: ["postgres", "-c", "max_connections=300"]`

**Fix 2 — definitions.py**: Wrap `yield from dagster_dlt.run(...)` in `try/finally` and call `client.sql_client._engine.dispose()` via the pipeline's `destination_client()` context manager. Errors are suppressed (best-effort cleanup).

**Why not PgBouncer**: dlt uses `COPY` commands, session-level advisory locks, and multi-statement transactions — incompatible with PgBouncer's transaction-mode pooling (the only mode that reduces connection count). Session-mode PgBouncer would not reduce connections enough to justify the added complexity.
