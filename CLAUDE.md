# Voz Company Crawler

Data pipeline crawling IT company reviews from the Voz.vn forum.

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Dagster + dagster-dlt |
| Ingestion | dlt (`dlt[postgres]`) |
| Storage (raw) | PostgreSQL |
| Storage (graph) | ArangoDB |
| Embeddings | OpenAI `text-embedding-3-small` |
| HTTP / CF bypass | FlareSolverr (Chrome headless sidecar) |
| Transformation | dbt (scaffold ready) |
| Dependency mgmt | uv |

## Project Layout

```
voz_crawler/
  core/
    entities/
      raw_post.py         RawPost SQLModel (mirrors raw.voz__posts columns)
      arango.py           RawPostDoc, NormalizedPostDoc, ExtractionResultDoc, ArangoEdge
      enrichment.py       NORMALIZATION_VERSION, ENRICHMENT_VERSION constants + enrichment types
    graph/
      post_sync.py        build_upsert_docs — diff rows vs existing hashes
      edge_sync.py        build_edges — parse blockquotes into ArangoEdge list
      quote_parser.py     extract_quote_edges — BeautifulSoup XenForo quote parser
      normalizer.py       normalize_post_html — strip quoted blocks, return own_text
      embedding_sync.py   embed_batch — pure transform, returns list[EmbedPatch] (no DB write)
      enrichment_sync.py  enrich_partition — pure LLM transform, returns list[ExtractionResultDoc]
      company_sync.py     build_company_mention_docs — pure projection from ExtractionResultDocs
      implicit_reply_sync.py  process_partition_implicit_replies — BM25+cosine+LLM implicit edges
    ingestion/
      html_source/
        html_parser.py    BeautifulSoup post extractor (extract_posts)
        pagination.py     XenForo URL builder + page count discovery
    repository/
      raw_repository.py   RawRepository — PostgreSQL read access for post rows
      graph_repository.py GraphRepository — ArangoDB posts/quotes/reply_graph operations
  defs/
    assets/
      ingestion.py        build_ingestion_assets(thread_id, partitions_def) factory
                            → per-thread @dlt_assets with key_prefix=[thread_id]
      reply_graph.py      build_thread_assets(thread_id, partitions_def, posts_asset_key) factory
                            → 7 assets per thread: sync_posts_to_arango (@asset),
                            extract_explicit_edges (@asset),
                            normalize_posts (@asset),
                            compute_embeddings (@asset),
                            classify_posts (@asset),
                            extract_company_mentions (@asset, non-noise only),
                            detect_implicit_replies (@asset, sensor-gated)
    jobs/
      ingestion.py        build_ingestion_jobs(thread_id, ...) factory
                            → discover_pages_job_{thread_id} + crawl_page_job_{thread_id}
      reply_graph.py      build_thread_jobs(thread_id, ...) factory
                            → reply_graph_job_{thread_id} + implicit_reply_job_{thread_id}
    ops/ingestion.py      build_discover_op(thread_id, partitions_def) factory
                            → discover_pages_op_{thread_id} (registers per-thread partitions)
    resources/
      crawler_resource.py CrawlerResource (thread_urls, flaresolverr_url, timeout)
                            url_for_thread(thread_id), url_for_partition(partition_key)
      postgres_resource.py PostgresResource (assembles SQLAlchemy URL)
      arango_resource.py  ArangoDBResource (connects to ArangoDB, ensures schema)
    sensors/ingestion.py  build_ingestion_sensors(thread_id, ...) factory
                            → voz_discover_sensor_{thread_id} + voz_crawl_sensor_{thread_id}
    sensors/reply_graph.py build_implicit_sensor(thread_id, ...) factory
                            → implicit_reply_sensor_{thread_id}
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
docker compose up postgres pgadmin arangodb -d   # Postgres :5432, pgAdmin :5050, ArangoDB :8529
uv run dagster dev                               # Dagit UI :3000
```

`dagster dev` loads `.env` automatically — no dotenv needed.

```bash
uv run ruff check . && uv run ruff format .   # lint + format
uv run pytest                                  # tests
uv add <package>                               # add dependency
uv run python scripts/clean_arango.py         # drop + recreate ArangoDB database (clean run)
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

Each thread URL has its own isolated pipeline with `DynamicPartitionsDefinition(f"voz_pages_{thread_id}")`. Partition key format: `{thread_id}:{page}` (e.g. `"677450:42"`).

Per-thread, two sensors work in tandem:

**`voz_discover_sensor_{thread_id}`** (every 6 hours) triggers `discover_pages_job_{thread_id}`:
1. Fetches page 1 of the thread via FlareSolverr to discover `total_pages`
2. Registers new keys `f"{thread_id}:{p}"` into `DynamicPartitionsDefinition(f"voz_pages_{thread_id}")`

**`voz_crawl_sensor_{thread_id}`** (`run_status_sensor`, fires after `discover_pages_job_{thread_id}` succeeds):
1. Checks Dagster's asset catalog for already-materialized partitions of `{thread_id}/voz__posts`
2. Queues `RunRequest` to `crawl_page_job_{thread_id}` for each unmaterialized page + the last page (always re-crawled daily)

Each run materializes one partition → calls `voz_page_source(page_url)` → parses posts → merges into `raw.voz__posts` by `post_id_on_site`. No dlt cursor state needed.

On Cloudflare block (403/429/503), the asset raises `RuntimeError`. Safe — next sensor evaluation retries automatically.

`definitions.py` generates all per-thread definitions via `_build_thread_pipeline(thread_id)` called in a loop over `VOZ_THREAD_URLS`.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `VOZ_THREAD_URL` | — | Full URL to the Voz thread |
| `FLARESOLVERR_URL` | — | FlareSolverr sidecar URL (e.g. `http://flaresolverr:8191`) |
| `HTTP_DELAY_SECONDS` | `2` | Retained for env compat (unused in asset) |
| `HTTP_TIMEOUT_SECONDS` | `60` | Per-request timeout |
| `PG_RAW_SCHEMA` | `raw` | PostgreSQL schema containing dlt-managed tables |
| `PG_POSTS_TABLE` | `posts` | Table name inside `PG_RAW_SCHEMA` |
| `ARANGO_HOST` | — | ArangoDB hostname (e.g. `localhost` or `arangodb`) |
| `ARANGO_PORT` | `8529` | ArangoDB HTTP port |
| `ARANGO_DB` | — | Target database name (auto-created if absent) |
| `ARANGO_USER` | — | ArangoDB username |
| `ARANGO_PASSWORD` | — | ArangoDB password |
| `OPENAI_API_KEY` | — | OpenAI API key for embedding computation |

## Reply Graph Pipeline

Seven assets per thread (in group `thread_{thread_id}`) run after each successful crawl partition. They form a diamond-shaped DAG: two fast parallel branches (`extract_explicit_edges` and `normalize_posts`) fan out from `sync_posts_to_arango`, then `normalize_posts` fans out again into a parallel embedding + classification pair.

### Asset execution order

```
sync_posts_to_arango
    ├── extract_explicit_edges          (branch A — fast, stateless)
    └── normalize_posts
            ├── compute_embeddings      (parallel with classify_posts)
            └── classify_posts          (parallel with compute_embeddings)
                    └── extract_company_mentions  (non-noise only: review/rating/event)
                                └── detect_implicit_replies  (sensor-gated)
                                        ▲
                                        │ (also deps)
                                extract_explicit_edges
```

### Asset descriptions

**`sync_posts_to_arango`** `[@asset]` — reads `raw.voz__posts` for the page URL, diffs against existing ArangoDB documents by `content_hash`, upserts only changed/new posts. Changed posts have Layer 2 fields reset to null, propagating staleness to downstream enrichment.

**`extract_explicit_edges`** `[@asset]` — re-parses XenForo `<blockquote>` HTML, drops all existing `quotes` edges for the partition, re-inserts fresh edges. Runs concurrently with `normalize_posts`.

**`normalize_posts`** `[@asset]` — fetches posts with stale `normalization_version` from ArangoDB, reads raw HTML from PostgreSQL, strips quoted blocks, writes `normalized_own_text` + `normalization_version`. Staleness: `normalization_version < NORMALIZATION_VERSION`.

**`compute_embeddings`** `[@asset]` — fetches posts with `normalized_own_text` but missing `embedding`, calls OpenAI `text-embedding-3-small`, writes `embedding` + `embedding_model`. Runs in parallel with `classify_posts` after `normalize_posts`.

**`classify_posts`** `[@asset]` — fetches posts with stale `enrichment_version`, runs LLM via PydanticAI to classify `content_class` and extract company mentions, writes `ExtractionResultDoc` + enrichment patches (`content_class`, `has_company_mention`, `enrichment_version`). Depends on `normalize_posts` (text), not `compute_embeddings`. Runs in parallel with `compute_embeddings`.

**`extract_company_mentions`** `[@asset]` — reads `ExtractionResultDoc` for the partition, filters to `content_class in {"review", "rating", "event"}`, projects `CompanyMentionDoc` + `MentionEdge` + `AliasEvidenceDoc`, drops and re-inserts mention data. Returns `MaterializeResult` with `mention_docs: 0` metadata if all posts are noise/question (partition shows as materialized, not failed).

**`detect_implicit_replies`** `[@asset]` — BM25 + cosine re-rank + PydanticAI agent to detect implicit reply edges. No `AutomationCondition.eager()` — triggered by `implicit_reply_sensor` only after all lower-numbered partitions have materialized `extract_company_mentions`. Depends on both `extract_company_mentions` AND `extract_explicit_edges` (explicit edges must exist before implicit detection so the `already_linked` exclusion is correct). Uses noise-aware adaptive window and passes `content_class` labels to the LLM for each candidate.

### AutomationCondition

All assets except `detect_implicit_replies` use `AutomationCondition.eager()` with `IdentityPartitionMapping`. The `default_automation_condition_sensor` evaluates these conditions automatically.

### Per-thread job selection

- `reply_graph_job_{thread_id}`: `AssetSelection.groups(f"thread_{thread_id}") - AssetSelection.assets(posts_asset_key) - AssetSelection.assets(implicit_key)` — excludes the ingestion asset and detect_implicit_replies
- `implicit_reply_job_{thread_id}`: `AssetSelection.assets(implicit_key)` only

### Version constants (in `core/entities/enrichment.py`)

| Constant | Controls |
|---|---|
| `NORMALIZATION_VERSION` | When to re-normalize post HTML |
| `EMBEDDING_MODEL` | When to re-embed (model name change) |
| `ENRICHMENT_VERSION` | When to re-run LLM classification |

### ArangoDB Schema

| Collection | Type | Purpose |
|---|---|---|
| `posts` | vertex | One document per forum post (`_key = post_id_on_site`) |
| `quotes` | edge | XenForo explicit quote relationships |
| `reply_graph` | named graph | ArangoDB graph wrapping posts → quotes → posts |

Indexes: `partition_key` persistent index on both collections for partition-scoped queries.

## Adding dbt Models

1. Copy `dbt/profiles.yml.example` → `~/.dbt/profiles.yml`
2. Add models under `dbt/models/staging/` or `dbt/models/marts/`
3. Sources pre-defined in `dbt/models/sources.yml` pointing at `raw`
4. `cd dbt && uv run dbt run`

## Brainstorm Notes

Brainstorm notes live in `docs/brainstorm/`. When creating a new brainstorm file:

**Naming convention:** `YYYYMMDD-HHMM--<slug>.md`

- `YYYYMMDD` — today's date (e.g. `20260406`)
- `HHMM` — current time in 24-hour format using hyphens (e.g. `1432`)
- `<slug>` — short, lowercase, hyphen-separated description of the topic (e.g. `embedding-strategy.md`, `arango-schema-v2.md`)

**Example:** `20260406-1432--embedding-strategy.md`

**File structure:** Start every brainstorm file with a single `#` title on line 1, followed by content.

```markdown
# Embedding Strategy for Reply Graph

Content starts here...
```

## Library Version Discipline

Before using any library API (especially Dagster, dlt, pydantic-ai, python-arango), always verify the **exact installed version** and fetch docs for that version:

```bash
uv run python -c "import <pkg>; print(<pkg>.__version__)"
```

Then use Context7 MCP with the version-specific library ID to look up the correct API. Never rely on training-data memory for library APIs — constructors, required arguments, and method signatures change between versions. A wrong API call only fails at runtime (when Dagster loads the code location), not at test time, because unit tests don't instantiate `Definitions`.

**Versions to check before touching their APIs:**

| Library | Check command |
|---|---|
| `dagster` | `import dagster; dagster.__version__` |
| `dlt` | `import dlt; dlt.__version__` |
| `pydantic_ai` | `import pydantic_ai; pydantic_ai.__version__` |
| `python-arango` | `import arango; arango.__version__` |
| `openai` | `import openai; openai.__version__` |

---

## Docs-First Workflow

Before writing any code for a non-trivial change, update the relevant documentation first:

1. **`docs/design-decisions.md`** — add or update the decision that motivates the change. Include: what was decided, why, trade-offs considered, and any alternatives rejected. If an existing decision's diagram or rationale becomes stale, fix it in the same commit.

2. **`CLAUDE.md`** — update the project layout, asset descriptions, or running instructions if the change affects them.

**Why docs first:**
- Forces explicit reasoning about the design before implementation details obscure it.
- Surfaces inconsistencies early — a diagram that can't be updated cleanly is a signal the design has a problem.
- Future agents (and humans) read `CLAUDE.md` and `design-decisions.md` as the canonical source of truth. Code without matching docs creates drift that compounds over time.

**What counts as non-trivial:** new assets, new data flows, changes to staleness logic, new external dependencies, changes to partition/sensor behavior. Bug fixes and one-line patches do not require a doc update unless they correct a documented behavior.

## Design Decisions

See [docs/design-decisions.md](docs/design-decisions.md).
