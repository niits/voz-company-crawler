# Core Refactor Design

**Date:** 2026-04-03
**Status:** Approved

## Context

The codebase currently mixes business logic and Dagster orchestration code together. Utility modules (`utils/`) are not clearly separated from Dagster-specific code, and heavy business logic (3-stage implicit edge detection, embedding batch processing, ArangoDB sync) lives directly inside Dagster asset function bodies. This makes the logic hard to test independently and difficult to reason about in isolation.

The goal is to extract all non-Dagster code into a `core/` domain layer with service classes, leaving `defs/` as thin orchestration (call core services, yield Dagster events). Resources become factories via `get_instance()` that return cached core instances, enabling proper connection pooling and shared state within a run.

---

## Directory Layout

```
voz_crawler/
  definitions.py                    ← unchanged Dagster entry point

  core/                             ← pure Python, zero Dagster imports
    __init__.py
    crawl/
      __init__.py
      fetcher.py                    # Fetcher class (FlareSolverr HTTP + requests.Session)
      parser.py                     # extract_posts, extract_quote_edges
      pagination.py                 # build_page_url, discover_total_pages
      source.py                     # @dlt.resource + @dlt.source (dlt ≠ Dagster → core)
    graph/
      __init__.py
      client.py                     # ArangoGraphClient (pooled, cached schema init)
      schema.py                     # ensure_schema (idempotent DDL)
      sync.py                       # GraphSyncService (post sync + explicit edge logic)
      retrieval.py                  # get_thread_context (AQL traversal)
    llm/
      __init__.py
      embeddings.py                 # EmbeddingService (OpenAI batch embeddings)
      implicit_edges.py             # ImplicitEdgeDetector (3-stage: ANN → dedup → LLM)
    db/
      __init__.py
      postgres.py                   # PostgresClient (SQLAlchemy engine + pool config)
      posts.py                      # PostsRepository (SQL queries over raw.voz__posts)

  defs/                             ← all Dagster code
    __init__.py
    assets/
      ingestion.py                  # thin @dlt_assets wrapper (unchanged structure)
      reply_graph.py                # thin assets: call core services, yield Dagster events
    jobs/
      crawl.py                      # unchanged
      graph.py                      # unchanged
    sensors/
      crawl.py                      # unchanged
      graph.py                      # unchanged
    resources/
      arango.py                     # ArangoResource — get_instance() → ArangoGraphClient
      crawler.py                    # CrawlerResource — get_instance() → Fetcher
      postgres.py                   # PostgresResource — get_instance() → PostgresClient
      openai.py                     # NEW: OpenAIResource — get_instance() → openai.OpenAI
    partitions.py                   # unchanged
    transformation/                 # unchanged placeholder
```

Files removed after migration:
- `voz_crawler/utils/` (entire directory → split into `core/crawl/`, `core/graph/`)
- `voz_crawler/sources/` (entire directory → `core/crawl/source.py`)

---

## Core Module Design

### `core/crawl/`

**Origin:** `utils/html_parser.py`, `utils/pagination.py`, `sources/voz_thread.py`

**`fetcher.py`** — `Fetcher` class:
- Wraps `requests.Session` for HTTP keep-alive to FlareSolverr
- Constructor accepts `flaresolverr_url: str`, `timeout: int`
- Methods: `fetch(url: str) -> tuple[int, str]`, `is_cf_block(status: int, html: str) -> bool`
- Session created once in `__init__`, reused across calls

**`parser.py`** — pure functions (unchanged logic, new location):
- `extract_posts(html: str) -> list[dict]`
- `extract_quote_edges(html: str) -> list[dict]`

**`pagination.py`** — pure functions (unchanged logic, new location):
- `build_page_url(base_url: str, page_num: int) -> str`
- `discover_total_pages(html: str) -> int`

**`source.py`** — dlt source (dlt is not Dagster; belongs in core):
- `voz_page_posts(page_url, flaresolverr_url, timeout) -> TDataItems` — `@dlt.resource`
- `voz_page_source(page_url, flaresolverr_url, timeout)` — `@dlt.source`
- Uses `Fetcher` internally instead of bare `requests.post`

---

### `core/graph/`

**Origin:** `utils/arango_setup.py`, `utils/graph_retrieval.py`, and business logic from `defs/assets/reply_graph.py`

**`client.py`** — `ArangoGraphClient` class:
- Constructor: `__init__(host, port, username, password, db_name)`
- Creates `ArangoClient` once; holds authenticated `Database` reference
- Lazy `ensure_schema()` call on first `db()` access via `_schema_initialized: bool` flag
- Method: `db() -> arango.db.Database`

**`schema.py`** — unchanged logic, new location:
- `ensure_schema(db: Database) -> None` (idempotent: creates collections, graph, vector index)

**`sync.py`** — `GraphSyncService` class:
- Constructor: `__init__(arango_db: Database, posts_repo: PostsRepository)`
- `sync_posts() -> dict` — incremental sync from PostgreSQL to ArangoDB Posts collection; returns stats
- `extract_explicit_edges() -> dict` — reads HTML from PostgreSQL, calls `extract_quote_edges`, upserts to `quotes` collection; returns stats
- `get_max_synced_post_id() -> int`
- `get_max_extracted_post_id() -> int`

**`retrieval.py`** — unchanged logic, new location:
- `get_thread_context(post_id, arango_db, max_depth, include_implicit) -> list[dict]`

---

### `core/llm/`

**Origin:** business logic from `compute_embeddings` and `detect_implicit_edges` assets

**`embeddings.py`** — `EmbeddingService` class:
- Constructor: `__init__(client: openai.OpenAI, model: str = "text-embedding-3-small", batch_size: int = 20)`
- `compute_missing(arango_db: Database) -> dict` — fetches posts with `embedding == null`, batches calls, updates collection; returns `{"records_processed": N}`

**`implicit_edges.py`** — `ImplicitEdgeDetector` class:
- Constructor: `__init__(client: openai.OpenAI, arango_db: Database)`
- `detect() -> dict` — runs full 3-stage pipeline:
  1. **ANN stage:** AQL with `APPROX_NEAR_COSINE()`, temporal scoring, similarity threshold ≥0.65
  2. **Dedup stage:** excludes pairs already in `quotes` collection
  3. **LLM stage:** GPT-4o-mini structured output, confidence threshold ≥0.75, upsert to `implicit_replies`
- Returns `{"edges_created": N, "candidates_evaluated": M}`

---

### `core/db/`

**Origin:** inline SQL in `defs/assets/reply_graph.py`

**`postgres.py`** — `PostgresClient` class:
- Constructor: `__init__(url: str)`
- Creates `create_engine(url, pool_size=5, max_overflow=10, pool_pre_ping=True)` once
- Property: `engine -> Engine`
- Method: `dispose() -> None` (for graceful shutdown)

**`posts.py`** — `PostsRepository` class:
- Constructor: `__init__(engine: Engine)`
- `fetch_new_posts(after_post_id: int) -> list[dict]` — returns posts newer than cursor
- `fetch_html_for_posts(after_post_id: int) -> list[dict]` — returns `(post_id, raw_content_html)` pairs

---

## Resource `get_instance()` Pattern

All resources use `PrivateAttr` (Pydantic) for lazy initialization — one instance per resource per Dagster run:

```python
# Pattern applied to all resources
from pydantic import PrivateAttr
from typing import Optional

class ArangoResource(ConfigurableResource):
    host: str
    port: int = 8529
    username: str = "root"
    password: str
    db: str = "voz_graph"

    _instance: Optional[ArangoGraphClient] = PrivateAttr(default=None)

    def get_instance(self) -> ArangoGraphClient:
        if self._instance is None:
            self._instance = ArangoGraphClient(
                host=self.host, port=self.port,
                username=self.username, password=self.password,
                db_name=self.db,
            )
        return self._instance
```

**New `OpenAIResource`** (`defs/resources/openai.py`):
- Fields: `api_key: str`
- `get_instance() -> openai.OpenAI` — caches `openai.OpenAI(api_key=self.api_key)`
- Bound in `definitions.py` via `EnvVar("OPENAI_API_KEY")`

**`PostgresResource`** updated:
- `get_instance() -> PostgresClient` — caches `PostgresClient(url=self.url)`
- The dlt pipeline in `ingestion.py` continues to use `postgres.url` directly (dlt manages its own pool)

**`CrawlerResource`** updated:
- `get_instance() -> Fetcher` — caches `Fetcher(flaresolverr_url=..., timeout=...)`

---

## Connection Pooling & Caching

| Component | Before | After |
|---|---|---|
| ArangoDB client | New `ArangoClient` on every `get_client()` call | Single `ArangoGraphClient` per resource, cached via `PrivateAttr` |
| ArangoDB schema | `ensure_schema()` called each asset run | Called once on first `db()` access, guarded by `_schema_initialized` flag |
| PostgreSQL engine | `create_engine()` in asset body, `dispose()` in finally | Shared `PostgresClient` with `pool_pre_ping=True`; engine lives for resource lifetime |
| OpenAI client | `OpenAI()` instantiated inline per asset run | Cached in `OpenAIResource` via `PrivateAttr` |
| FlareSolverr | `requests.post()` — new TCP connection each call | `Fetcher` uses `requests.Session` for HTTP keep-alive |

---

## Asset Shape After Refactor

Assets become thin: acquire service instances, call one method, add metadata.

```python
# defs/assets/reply_graph.py — AFTER (example)

@asset(group_name="reply_graph", deps=[sync_posts_to_arango])
def compute_embeddings(
    context: AssetExecutionContext,
    arango: ArangoResource,
    openai: OpenAIResource,
) -> None:
    svc = EmbeddingService(client=openai.get_instance())
    stats = svc.compute_missing(arango.get_instance().db())
    context.add_output_metadata(stats)


@asset(group_name="reply_graph", deps=[compute_embeddings])
def detect_implicit_edges(
    context: AssetExecutionContext,
    arango: ArangoResource,
    openai: OpenAIResource,
) -> None:
    detector = ImplicitEdgeDetector(
        client=openai.get_instance(),
        arango_db=arango.get_instance().db(),
    )
    stats = detector.detect()
    context.add_output_metadata(stats)
```

---

## Migration Order

1. Create `core/` package skeleton (all `__init__.py` files)
2. Move and rename: `utils/` → `core/crawl/` + `core/graph/` (no logic changes)
3. Create `core/db/postgres.py` + `core/db/posts.py`
4. Create `core/crawl/fetcher.py` (`Fetcher` class from bare `requests.post` calls)
5. Move `sources/voz_thread.py` → `core/crawl/source.py`, use `Fetcher`
6. Create `core/graph/client.py` (`ArangoGraphClient` with lazy schema init)
7. Extract `GraphSyncService` into `core/graph/sync.py`
8. Extract `EmbeddingService` into `core/llm/embeddings.py`
9. Extract `ImplicitEdgeDetector` into `core/llm/implicit_edges.py`
10. Add `get_instance()` + `PrivateAttr` to all resources; add `OpenAIResource`
11. Update `definitions.py` to bind `OpenAIResource`
12. Slim down assets to call core services
13. Delete `voz_crawler/utils/` and `voz_crawler/sources/`
14. Update all imports in tests
15. Run `uv run ruff check . && uv run ruff format .` + `uv run pytest`

---

## Verification

```bash
# Lint + format
uv run ruff check . && uv run ruff format .

# Unit tests (all should pass without changes to test logic)
uv run pytest

# Smoke test: Dagster loads definitions without error
uv run dagster asset list

# Integration check: confirm no circular imports in core
python -c "import voz_crawler.core.crawl; import voz_crawler.core.graph; import voz_crawler.core.llm; import voz_crawler.core.db"
```

Core modules must import with zero Dagster dependencies:
```bash
python -c "
import sys
import voz_crawler.core.crawl.parser
import voz_crawler.core.graph.client
import voz_crawler.core.llm.embeddings
dagster_imported = any('dagster' in m for m in sys.modules)
assert not dagster_imported, 'core must not import Dagster'
print('OK: core has no Dagster imports')
"
```
