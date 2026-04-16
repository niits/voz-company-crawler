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
      ingestion.py        @dlt_assets: voz_page_posts_assets (partitioned)
      reply_graph.py      5 assets: sync_posts_to_arango (@asset),
                            extract_explicit_edges (@asset),
                            reply_graph_preprocess_assets (@graph_multi_asset: normalize_posts + compute_embeddings),
                            reply_graph_llm_assets (@graph_multi_asset: classify_posts + extract_company_mentions),
                            detect_implicit_replies (@asset, sensor-gated)
    jobs/
      ingestion.py        crawl_page_job + discover_pages_job
      reply_graph.py      reply_graph_job (all reply_graph assets, partitioned)
    ops/ingestion.py      discover_pages_op (registers dynamic partitions)
    resources/
      crawler_resource.py CrawlerResource (thread_url, flaresolverr_url, timeout)
      postgres_resource.py PostgresResource (assembles SQLAlchemy URL)
      arango_resource.py  ArangoDBResource (connects to ArangoDB, ensures schema)
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
| `PG_RAW_SCHEMA` | `raw` | PostgreSQL schema containing dlt-managed tables |
| `PG_POSTS_TABLE` | `posts` | Table name inside `PG_RAW_SCHEMA` |
| `ARANGO_HOST` | — | ArangoDB hostname (e.g. `localhost` or `arangodb`) |
| `ARANGO_PORT` | `8529` | ArangoDB HTTP port |
| `ARANGO_DB` | — | Target database name (auto-created if absent) |
| `ARANGO_USER` | — | ArangoDB username |
| `ARANGO_PASSWORD` | — | ArangoDB password |
| `OPENAI_API_KEY` | — | OpenAI API key for embedding computation |

## Reply Graph Pipeline

Five assets in the `reply_graph` group run after each successful crawl partition. They form two parallel branches that fan out from `sync_posts_to_arango`.

### Asset execution order

```
sync_posts_to_arango
    ├── extract_explicit_edges          (parallel branch A — fast, stateless)
    │       │
    └───────┤   reply_graph_preprocess_assets   (parallel branch B)
            │           └── reply_graph_llm_assets
            │                   │
            └───────────────────┘
                    ▼
            detect_implicit_replies  (sensor-gated; deps: extract_company_mentions + extract_explicit_edges)
```

### Asset descriptions

**`sync_posts_to_arango`** `[@asset]` — reads `raw.voz__posts` for the page URL, diffs against existing ArangoDB documents by `content_hash`, upserts only changed/new posts. Changed posts have Layer 2 fields reset to null, propagating staleness to downstream enrichment.

**`extract_explicit_edges`** `[@asset]` — re-parses XenForo `<blockquote>` HTML, drops all existing `quotes` edges for the partition, re-inserts fresh edges. Runs concurrently with the preprocess branch.

**`reply_graph_preprocess_assets`** `[@graph_multi_asset → normalize_posts, compute_embeddings]` — in-memory chain: one DB read (staleness check + PG fetch) → normalize HTML → embed with OpenAI → one bulk write to ArangoDB. Staleness: `normalization_version < NORMALIZATION_VERSION OR embedding_model != EMBEDDING_MODEL`.

**`reply_graph_llm_assets`** `[@graph_multi_asset → classify_posts, extract_company_mentions]` — in-memory chain: one DB read (staleness check) → LLM classification + mention extraction → one bulk write. Depends on `compute_embeddings` (not `sync_posts_to_arango`). Staleness: no `ExtractionResultDoc` with `enrichment_version == ENRICHMENT_VERSION`.

**`detect_implicit_replies`** `[@asset]` — BM25 + cosine re-rank + PydanticAI agent to detect implicit reply edges. No `AutomationCondition.eager()` — triggered by `implicit_reply_sensor` only after all lower-numbered partitions have materialized `extract_company_mentions`. Depends on both `extract_company_mentions` AND `extract_explicit_edges` (explicit edges must exist in `reply_graph` before implicit detection runs, so the `already_linked` exclusion is correct). Uses noise-aware adaptive window (`content_class == "noise"` posts don't consume window slots) and passes `content_class` labels to the LLM for each candidate.

### AutomationCondition

All assets except `detect_implicit_replies` use `AutomationCondition.eager()` with `IdentityPartitionMapping`. The `.with_attributes()` call is applied post-hoc to `@graph_multi_asset` objects since the decorator does not accept `automation_condition` directly.

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
