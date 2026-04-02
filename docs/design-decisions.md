# Design Decisions

Key trade-offs and alternatives rejected. For how each feature works, see
[docs/features/](features/).

---

## Ingestion

### Why dlt instead of raw psycopg2

`write_disposition="merge"` + `primary_key` replaces ~200 lines of hand-written
`execute_values` + `ON CONFLICT` SQL. dlt also manages schema creation, column type
inference, and load history out of the box.

**Trade-off**: dlt owns the `raw` schema DDL, so table names follow dlt conventions
(`voz__posts` with source prefix).

---

### Why FlareSolverr instead of an in-process HTTP library

Voz.vn uses Cloudflare Bot Management with JS challenges. Library-level approaches
(TLS fingerprinting, JS emulation) require constant maintenance as Cloudflare updates
its challenge. FlareSolverr delegates to a real Chrome browser, solving the challenge
once per session with no in-process complexity.

**Trade-off**: Adds an external service dependency (`FLARESOLVERR_URL`).

---

### Why dynamic partitions instead of dlt cursors

dlt internal state (`last_page_crawled` cursor + `dlt.sources.incremental`) couples
progress tracking to dlt's state backend, which is opaque in Dagit. Dagster
`DynamicPartitionsDefinition` surfaces materialization history natively: green/grey
partitions in Dagit, clean backfill UI, no cursor state to reset.

---

### Why two sensors instead of one

Splitting discovery (`voz_discover_sensor` → `discover_pages_job`) from crawl
scheduling (`voz_crawl_sensor` → `crawl_page_job`) has two benefits:

1. A Cloudflare block during discovery fails only that job; the crawl sensor is
   unaffected and continues serving already-registered partitions.
2. `run_status_sensor` on `discover_pages_job` SUCCESS guarantees partition keys are
   fully registered before `RunRequest`s are submitted — no race condition.

---

### Why no RetryPolicy on dlt assets

Cloudflare blocks are sustained (minutes to hours); a 60-second retry rarely helps.
Failed historical pages are re-run from Dagit's Partitions view. The last page
re-queues automatically the next day via its daily `run_key`.

**Alternative rejected**: `RetryPolicy(max_retries=2, delay=60)`.

---

## Infrastructure

### PostgreSQL: max_connections=300 + engine disposal

Each `dlt.pipeline(...)` creates a SQLAlchemy `QueuePool` (default: pool_size=5,
max_overflow=10 → up to 15 connections). Without explicit engine disposal, pools
linger until GC, exhausting PostgreSQL's default `max_connections=100` across
sequential partition runs.

Fix: `max_connections=300` in docker-compose + `client.sql_client._engine.dispose()`
in a `try/finally` block after `dagster_dlt.run(...)`.

**Why not PgBouncer**: dlt uses `COPY` commands, session-level advisory locks, and
multi-statement transactions — incompatible with PgBouncer transaction-mode pooling
(the only mode that reduces connection count).

---

### sql/init.sql only pre-creates staging and marts

`raw` schema is fully managed by dlt (creates `raw`, `_dlt_loads`,
`_dlt_pipeline_state` on first run). Manually pre-creating `raw` conflicts with dlt's
DDL management.

---

### No python-dotenv dependency

`dagster dev` auto-loads `.env` from the project root (Dagster 1.5+). In Docker,
variables are injected by `docker-compose.yml`. A separate dotenv library adds no value.

---

## Configuration

### Three-schema design (raw / staging / marts)

`raw` is dlt-owned and upsert-only — a stable read contract for dbt. dbt models never
write to `raw`, so transformation changes cannot corrupt raw data.

---

### ConfigurableResource + EnvVar instead of os.environ

`EnvVar` resolves at runtime: the Dagster webserver loads the module at startup without
requiring env vars to be present. `ConfigurableResource` fields are visible in Dagit's
resource configuration panel and enable resource injection for testing.
