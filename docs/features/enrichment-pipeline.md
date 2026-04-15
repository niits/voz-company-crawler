# Enrichment Pipeline

Post-ingestion pipeline that enriches the ArangoDB reply graph with structured metadata.
Data models for all collections are in `docs/features/data-model.md`.
Company identity and alias resolution are in `docs/features/company-identity.md`.

---

## Final goal

Enable graph queries that answer: *"What do people say about company X? What positions and
salaries are mentioned? What events happened and when?"*

---

## Asset topology

```
voz_page_posts_assets                        group: ingestion
  └─► sync_posts_to_arango                   group: reply_graph
        ├─► extract_explicit_edges           group: reply_graph
        └─► normalize_posts                  group: reply_graph  (NEW)
              └─► compute_embeddings         group: reply_graph  (MODIFIED)
                    └─► classify_posts       group: reply_graph  (NEW, OpenAI)
                          ├─► extract_company_mentions           (NEW, ArangoDB)
                          │     └─► resolve_company_mentions     (NEW, ArangoDB)
                          └─► detect_implicit_replies            (NEW, OpenAI)
```

All assets except `detect_implicit_replies`: `AutomationCondition.eager()`, `IdentityPartitionMapping`, `partitions_def=voz_pages_partitions`.
`detect_implicit_replies` is triggered by `implicit_reply_sensor` (not `eager()`) — the sensor gates execution until all lower-numbered partitions have materialized `extract_company_mentions`.
Resources used: `arango`, `openai` (existing). No new Dagster resources.

---

## Asset descriptions

### `sync_posts_to_arango` (existing)

Reads `raw.posts` from PostgreSQL for the partition's page URL. Diffs against existing
ArangoDB post hashes. Upserts changed/new posts with `on_duplicate="replace"`, which resets
all layer 2 fields to null. Skips unchanged posts.

### `extract_explicit_edges` (modified)

Parses `raw_content_html` for XenForo `<blockquote data-source="post: {id}">` elements.
Drops all existing `html_metadata` edges for the partition, then re-inserts.

Unattributed quotes (`data-source=""`) cannot produce edges; their count is reported as
`unresolvable_quotes` in `MaterializeResult` metadata. `extract_quote_edges` returns
`(edges, unresolvable_count)`.

### `normalize_posts` (new)

Reads `raw_content_html` from PostgreSQL for posts where `normalized_own_text IS NULL`.
Runs `normalize_post_html(html)` per post — strips quoted blocks, preserves own text.
Patches `normalized_own_text`, `normalized_quoted_blocks`, `normalization_version` via
`update_many`. No LLM, no HTTP beyond Postgres + ArangoDB.

### `compute_embeddings` (modified)

Embeds `normalized_own_text` instead of `content_text`. Posts where `normalized_own_text`
is null or shorter than 20 characters are skipped (pure-quote posts are noise; no useful
vector). Model: `text-embedding-3-small`, batch size 100.

### `classify_posts` (new)

For each post where `enrichment_version IS NULL OR enrichment_version < ENRICHMENT_VERSION`:

1. Calls PydanticAI agent (model: `gpt-4o-mini`, `temperature=0`) with `normalized_own_text`
   and post metadata.
2. Agent returns `PostEnrichmentResult` (classification + company mentions + alias definitions).
3. Builds `ExtractionResultDoc` and upserts into `extraction_results` collection.
4. PATCHes `content_class`, `content_class_confidence`, `has_company_mention`,
   `enrichment_version` onto the post document for fast AQL filtering.

One LLM call per post, serial within a partition. Partition-level concurrency controlled by Dagster concurrency key `voz_llm` (limit 1) — only one partition runs LLM/ArangoDB sync at a time.

### `extract_company_mentions` (new)

Reads `ExtractionResultDoc` records for the partition at the current `ENRICHMENT_VERSION`.
For each document with non-empty `company_mentions`:

1. Drops all existing `mentions` edges for the partition.
2. Builds `CompanyMentionDoc` + `MentionEdge` from `company_mentions` list.
3. Upserts `CompanyMentionDoc` with `on_duplicate="replace"`.
4. Inserts `MentionEdge` with `on_duplicate="ignore"`.
5. Creates `AliasEvidenceDoc` records from `alias_definitions` (if any).

No LLM.

### `resolve_company_mentions` (new)

Runs after `extract_company_mentions`. Checks for new `AliasEvidenceDoc` records.
Recomputes `CompanyAliasDoc.resolutions` for affected aliases. Backfills `company_key`,
`is_ambiguous`, `resolution_version` on `CompanyMentionDoc` records where
`resolution_version` is stale. Creates stub `CompanyDoc` entries for newly discovered names.

See `docs/features/company-identity.md` for the resolution algorithm.

### `detect_implicit_replies` (new)

Trigger: `implicit_reply_sensor` (not `eager()`). Sensor fires when a partition materializes
`extract_company_mentions` and checks that all lower-numbered partitions have also materialized
`extract_company_mentions` — ensuring the cross-partition lookback window is fully populated.

For each post in the partition that has an embedding:

**Stage 1 — Retrieval** (no LLM, single AQL query via `reply_graph_search` view):

Computes local reply rate over the preceding 2-hour window to derive:
- `adaptive_n` = `clamp(round(BASE_N × rate / BASE_RATE), MIN_WINDOW=2, MAX_WINDOW=20)`
- `max_lookback_hours` = `clamp(BASE_LOOKBACK × BASE_RATE / max(rate, MIN_RATE), 2, 168)`

Candidate query over `reply_graph_search` view combines three signals in one AQL call:
- **Embedding similarity** (weight 0.50): cosine similarity ≥ 0.65
- **BM25 lexical** (weight 0.30): `BM25()` score > 1.0 via ArangoSearch
- **Window boost** (weight 0.20): adaptive_n nearest prior posts always included

Posts already connected to the source in `reply_graph` (any edge direction, any method) are
excluded via a `1..1 ANY` traversal subquery. Returns top 5 candidates.

**Stage 2 — LLM decision**:

Source post + all candidates sent to PydanticAI agent in one call. Agent returns
`ImplicitReplyDecision`. Accepted edges (confidence ≥ 0.6) are stored as `ArangoEdge`
with `method="implicit_llm"`. Decision is also PATCHed into
`ExtractionResultDoc.implicit_replies` for auditability.

---

## Incremental processing

| Field | Null when | Triggers reprocess when |
|---|---|---|
| `normalized_own_text` | `content_hash` changes | `IS NULL` |
| `embedding` | `normalized_own_text` reset | `IS NULL` |
| `enrichment_version` | `embedding` reset | `IS NULL OR < ENRICHMENT_VERSION` |

Bumping `NORMALIZATION_VERSION` in `enrichment.py` reprocesses all normalizations.
Bumping `ENRICHMENT_VERSION` reprocesses all LLM calls.

Implicit reply edges (`implicit_llm`) are not recomputed unless the post itself changes
(embedding reset). They accumulate across runs.

---

## Content classification classes

| Class | Meaning |
|---|---|
| `review` | First-hand experience or opinion about working at a company |
| `rating` | Numerical salary or score mention without full narrative |
| `event` | Company news: layoff, acquisition, hiring freeze, incident |
| `question` | Asking for information about a company |
| `noise` | Greetings, reactions, very short posts, off-topic |

---

## Implicit reply parameters

| Parameter | Default | Meaning |
|---|---|---|
| `rate_window` | 2 hours | Window for measuring local reply rate |
| `BASE_N` | 5 | Window size at reference rate |
| `BASE_RATE` | 10 posts/hour | Reference rate |
| `MIN_WINDOW` | 2 | Hard floor on post-count window |
| `MAX_WINDOW` | 20 | Hard ceiling on post-count window |
| `BASE_LOOKBACK` | 24 hours | Lookback at reference rate |
| `MIN_LOOKBACK_HOURS` | 2 | Hard floor on time lookback |
| `MAX_LOOKBACK_HOURS` | 168 | Hard ceiling (7 days) |
| `MIN_RATE` | 1 post/hour | Floor for rate in lookback formula |
| `MIN_EMB_SIM` | 0.65 | Minimum cosine similarity for embedding candidates |
| `CONFIDENCE_THRESHOLD` | 0.6 | Minimum LLM confidence to store an implicit edge |

---

## LLM implementation

All LLM calls use **PydanticAI** agents:
- Model: `openai:gpt-4o-mini`, `temperature=0`
- Result types are Pydantic `BaseModel` subclasses — no manual `response_format` schema construction
- Dagster's `openai.get_client(context)` client is injected into PydanticAI via `OpenAIModel("gpt-4o-mini", openai_client=client)` — token usage is auto-logged to Dagster asset catalog
- `result.usage()` totals (request_tokens, response_tokens) reported in `MaterializeResult.metadata`
- `result.all_messages()` stored as `ExtractionResultDoc.messages_json` for full input/output audit trail
- ArangoDB repo injected via PydanticAI `RunContext[deps]`

### Partition-level concurrency

`classify_posts` and `detect_implicit_replies` both carry `op_tags={"dagster/concurrency_key": "voz_llm"}`.
Set `voz_llm` limit to `1` in Dagster instance config to ensure only one partition runs LLM/ArangoDB sync at a time.

---

## New files

| File | Purpose |
|---|---|
| `voz_crawler/core/graph/normalizer.py` | `normalize_post_html()` |
| `voz_crawler/core/graph/enrichment_sync.py` | PydanticAI agent + `enrich_and_update()` |
| `voz_crawler/core/graph/company_sync.py` | `build_company_mention_docs()` |
| `voz_crawler/core/graph/implicit_reply_sync.py` | Stage 1 retrieval + Stage 2 PydanticAI agent |
| `voz_crawler/core/graph/alias_resolution.py` | Confidence recomputation + backfill logic |
| `voz_crawler/core/entities/enrichment.py` | LLM response models + version constants |
| `voz_crawler/core/entities/company.py` | `CompanyDoc`, `CompanyAliasDoc`, `AliasEvidenceDoc` |
| `voz_crawler/core/entities/arango.py` | Replace with `RawPostDoc`, `NormalizedPostDoc`, `ExtractionResultDoc`, `CompanyMentionDoc`, `MentionEdge`, `ArangoEdge` |
| `voz_crawler/core/repository/graph_repository.py` | All new AQL methods + `ensure_schema()` additions |
| `voz_crawler/defs/assets/reply_graph.py` | New assets: `normalize_posts`, `classify_posts`, `extract_company_mentions`, `resolve_company_mentions`, `detect_implicit_replies` |
| `voz_crawler/definitions.py` | Register all new assets |
