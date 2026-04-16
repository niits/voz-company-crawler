# Data Model Reorganization

**Date**: 2026-04-06

**Date**: 2026-04-06

**Status**: Spec
**Motivation**: The current `ArangoPost` conflates raw ingestion data with derived/computed fields (`embedding`). As enrichment adds more derived fields, the model becomes unmanageable. A clean 3-layer separation makes each layer's ownership, invalidation rules, and update frequency explicit.

---

## Three-layer model

```
PostgreSQL (dlt-managed)
  └── RawPost                   layer 0 — immutable ingestion record

ArangoDB — posts collection
  ├── RawPostDoc                layer 1 — raw fields only, no computed data
  └── NormalizedPostDoc         layer 2 — all enrichment written here (PATCH, never replace)

Python (transient, never stored)
  └── ExtractionResult          layer 3 — LLM output models, validated then projected into layer 2
```

---

## Layer 1 — `RawPostDoc` (ArangoDB `posts` collection)

Replaces the current `ArangoPost`. Contains only what comes directly from the raw ingestion — no computed fields.

Stored once on `sync_posts_to_arango`. Updated only when `content_hash` changes (same pattern as today).

```python
class RawPostDoc(SQLModel):
    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")          # str(post_id_on_site)
    post_id: int
    author_username: str | None
    author_id: str | None
    posted_at: str | None                   # ISO datetime string
    content_text: str | None                # raw full text (includes quoted blocks)
    content_hash: str                       # SHA-256 of content_text
    partition_key: str
    thread_url: str
    page_number: int
```

**Removed from this layer**: `embedding` — it was computed from `content_text` which mixes the author's own words with quoted text. Embeddings must be computed from normalized content.

---

## Layer 2 — `NormalizedPostDoc` (same `posts` collection, separate fields)

All enrichment is written to these fields via `update_many` (PATCH semantics). Layer 1 fields are never touched after initial upsert.

The `normalized_*` fields are populated by the content normalization step. The `embedding` field is computed from `normalized_own_text` — not from `content_text`.

```python
class NormalizedPostDoc(SQLModel):
    """Patch payload for the enrichment fields of a post document.

    Written by multiple assets; each asset only patches the fields it owns.
    All fields are None until the relevant enrichment asset runs.
    """
    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")

    # ── Content normalization (set by normalize_posts asset) ─────────────────
    normalized_own_text: str | None         # author's actual reply, quotes stripped
    normalized_quoted_blocks: list[dict] | None   # [{author, source_post_id, text}, ...]
    normalization_version: int | None

    # ── Embedding (set by compute_embeddings, based on normalized_own_text) ──
    embedding: list[float] | None           # embedding of normalized_own_text
    embedding_model: str | None

    # ── LLM enrichment (set by classify_posts) ───────────────────────────────
    content_class: str | None               # review | rating | event | question | noise
    content_class_confidence: float | None
    has_company_mention: bool | None
    enrichment_staging: dict | None         # raw PostEnrichmentResult JSON; cleared after extract_company_mentions
    enrichment_version: int | None
```

### Staleness chain

Each layer depends on the one above it. Reset propagates downward:

```
content_hash changes (content_text updated)
  → reset: normalized_own_text = null, normalization_version = null
      → reset: embedding = null
          → reset: enrichment_version = null, content_class = null, ...
```

In practice, `sync_posts_to_arango` uses `on_duplicate="replace"` which already resets all fields on content change. The chain is enforced by asset dependency order.

### Staleness flags summary

| Field | Set to null when... | Checked by |
|---|---|---|
| `normalized_own_text` | `content_hash` changes | `fetch_posts_needing_normalization` |
| `embedding` | `normalized_own_text` changes | `fetch_posts_needing_embedding` |
| `enrichment_version` | `embedding` recomputed | `fetch_posts_needing_enrichment` |

`extraction_results` documents are superseded (replaced) when `enrichment_version` is bumped — old versions are not deleted, allowing comparison across prompt versions.

---

## Layer 3 — Extraction result documents (persisted in ArangoDB)

LLM output is stored as first-class documents in ArangoDB, not projected inline into `NormalizedPostDoc` and discarded. This gives:

- **Audit trail** — exact LLM output is queryable at any time.
- **Re-projection without re-inference** — if downstream logic changes (e.g. company name normalization rules), re-run the projection pass without paying for LLM calls again.
- **Version history** — when `ENRICHMENT_VERSION` is bumped and the LLM re-runs, old extractions are superseded but not deleted (retained with their version number for comparison).
- **Decoupled writes** — `classify_posts` writes to `extraction_results`; `extract_company_mentions` reads from it. No staging field on the post document needed.

### ArangoDB collection: `extraction_results`

One document per (post, enrichment_version) pair. On re-run, the existing document is replaced (`on_duplicate="replace"`).

```python
class ExtractionResultDoc(SQLModel):
    """Persisted LLM extraction output for a single post.

    Stored in the ArangoDB `extraction_results` collection.
    _key = f"{post_key}_{enrichment_version}"
    """
    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")              # f"{post_key}_{enrichment_version}"
    post_key: str                               # references posts/{post_key}
    partition_key: str
    enrichment_version: int

    # Classification
    content_class: str
    content_class_confidence: float

    # Company mentions — full structured output stored as-is
    company_mentions: list[dict]                # list of CompanyMentionResult.model_dump()

    # Implicit reply decisions — stored for auditability
    implicit_replies: list[dict]                # list of ImplicitReply.model_dump(), written by detect_implicit_replies

    # Metadata
    model_used: str                             # e.g. "gpt-4o-mini"
    extracted_at: str                           # ISO datetime of extraction
```

### Python-only LLM response models

These are the typed models returned by PydanticAI agents. They are validated, then written into `ExtractionResultDoc` via `.model_dump()`. Defined in `voz_crawler/core/entities/enrichment.py`.

```python
# ── Constants ─────────────────────────────────────────────────────────────────
NORMALIZATION_VERSION = 1   # bump to reprocess all normalizations
ENRICHMENT_VERSION = 1      # bump to reprocess all LLM enrichments

# ── PydanticAI result models ───────────────────────────────────────────────────

class ContentClassification(BaseModel):
    content_class: str          # review | rating | event | question | noise
    confidence: float

class CompanyMentionResult(BaseModel):
    company_name_raw: str
    identification_clues: list[str]   # aliases/nicknames as-found, not resolved
    company_name: str                 # normalized best-effort
    company_type: str                 # it | outsourcing | product | startup | bank | telco | unknown
    mention_type: str                 # positive_review | negative_review | salary_info | layoff | hiring | event | general
    sentiment: str                    # positive | negative | neutral
    summary: str

class PostEnrichmentResult(BaseModel):
    post_key: str
    classification: ContentClassification
    company_mentions: list[CompanyMentionResult]

class ImplicitReply(BaseModel):
    candidate_key: str
    confidence: float
    reasoning: str

class ImplicitReplyDecision(BaseModel):
    source_key: str
    replies_to: list[ImplicitReply]
```

### Write flow

```
classify_posts asset
  → PydanticAI agent returns PostEnrichmentResult
  → build ExtractionResultDoc (company_mentions written, implicit_replies=[])
  → upsert into extraction_results collection
  → PATCH posts/{post_key}: content_class, content_class_confidence, has_company_mention, enrichment_version

extract_company_mentions asset
  → read extraction_results WHERE post_key IN partition AND enrichment_version == current
  → build CompanyMention vertex docs + MentionEdge docs
  → upsert into company_mentions; insert into mentions

detect_implicit_replies asset
  → PydanticAI agent returns ImplicitReplyDecision
  → PATCH extraction_results/{key}: implicit_replies = [...]
  → insert ArangoEdge(method="implicit_llm") into quotes
```

### Staleness: `NormalizedPostDoc` projection fields

`classify_posts` still PATCHes `content_class`, `content_class_confidence`, `has_company_mention`, and `enrichment_version` onto the post document for fast AQL filtering (e.g. "find all review posts"). These are projections derived from `ExtractionResultDoc` — they can always be re-derived from the persisted extraction if needed.

The `enrichment_staging` field is **removed** — `ExtractionResultDoc` replaces it entirely.

---

## Embedding on normalized content

### Problem with current approach

`compute_embeddings` currently embeds `content_text` which contains the full raw text including quoted blocks. For a post like:

```
"Fire Of Heart said:\nXin review Kyber nào...\nClick to expand...\nCó mấy thằng em làm ở Kyber network..."
```

The embedding vector is a blend of the quoted post's meaning and the author's actual reply. This causes:
- **Cosine similarity pollution** in implicit reply detection — two posts quoting the same popular post will appear highly similar even if their own replies are unrelated.
- **Classification noise** — the embedding does not represent what the author actually said.

### Fix

Embed `normalized_own_text` instead of `content_text`.

If `normalized_own_text` is empty (post was only a quote with no own text), skip embedding — these posts are noise by definition (`content_class="noise"` is almost certain).

New `fetch_posts_needing_embedding` query:
```aql
FOR p IN posts
  FILTER p.partition_key == @pk
  FILTER p.normalized_own_text != null
  FILTER LENGTH(p.normalized_own_text) >= 20
  FILTER p.embedding == null
  RETURN {key: p._key, text: p.normalized_own_text}
```

---

## New asset: `normalize_posts`

A new asset is needed between `sync_posts_to_arango` and `compute_embeddings`:

```
sync_posts_to_arango
  └── normalize_posts         (NEW) — strips quotes, sets normalized_own_text
        └── compute_embeddings  (MODIFIED) — embeds normalized_own_text
              └── classify_posts
```

`normalize_posts`:
- Reads `raw_content_html` from PostgreSQL for the partition (same query as `extract_explicit_edges`)
- Runs `normalize_post_html(html)` for each post
- Patches `normalized_own_text`, `normalized_quoted_blocks`, `normalization_version` via `update_many`
- No LLM, no network calls beyond Postgres + ArangoDB

---

## Implicit Reply Detection — graph view

Implicit reply candidates are retrieved by querying the `reply_graph` named graph via AQL traversal rather than scanning the `posts` collection directly.

**Why a graph view**: The traversal naturally excludes posts already connected to the source post by an existing explicit edge (any direction) — no separate `get_explicit_edge_pairs` query needed. The graph is the source of truth for existing connections.

**ArangoSearch view** (`reply_graph_search`): An ArangoSearch view over the `posts` collection enabling:
- Full-text BM25 scoring via `BM25()` and `PHRASE()` AQL functions (replaces in-Python BM25)
- Combined scoring with `COSINE_SIMILARITY` in a single AQL query
- No Python-side tokenization needed

View definition (added to `ensure_schema()`):
```python
if not self._db.has_view("reply_graph_search"):
    self._db.create_arangosearch_view(
        "reply_graph_search",
        properties={
            "links": {
                "posts": {
                    "fields": {
                        "normalized_own_text": {"analyzers": ["text_vi", "identity"]},
                        "embedding": {},
                        "partition_key": {"analyzers": ["identity"]},
                    }
                }
            }
        },
    )
```

Note: `text_vi` (Vietnamese text analyzer) may not be available in all ArangoDB versions — fall back to `text_en` or `identity` if absent. This can be checked at `ensure_schema()` time via `self._db.analyzers()`.

**New `GraphRepository` method** replacing `fetch_embedding_candidates`:

```python
def fetch_implicit_reply_candidates(
    self,
    partition_key: str,
    source_key: str,
    source_embedding: list[float],
    source_text: str,
    window_keys: list[str],         # adaptive window post keys (always included)
    top_n: int = 5,
    min_emb_sim: float = 0.65,
) -> list[dict]:
    """
    Single AQL query over reply_graph_search view combining:
      - BM25 lexical score (via ArangoSearch)
      - COSINE_SIMILARITY (embedding)
      - window boost (explicit list of keys)

    Excludes posts already connected to source_key in the reply_graph (any edge direction).
    Returns top_n candidates by combined score.
    """
```

The query skeleton:
```aql
LET already_linked = (
  FOR v IN 1..1 ANY CONCAT("posts/", @source_key)
    GRAPH "reply_graph"
    RETURN v._key
)
FOR p IN reply_graph_search
  SEARCH ANALYZER(p.normalized_own_text IN TOKENS(@source_text, "text_en"), "text_en")
    AND p.partition_key == @pk
    AND p._key != @source_key
    AND TO_NUMBER(p._key) < TO_NUMBER(@source_key)
    AND p._key NOT IN already_linked
  LET emb_sim = p.embedding != null ? COSINE_SIMILARITY(p.embedding, @source_emb) : 0
  LET bm25 = BM25(p)
  LET window_boost = p._key IN @window_keys ? 0.2 : 0.0
  LET score = 0.5 * emb_sim + 0.3 * MIN([bm25 / 5.0, 1.0]) + window_boost
  FILTER emb_sim >= @min_emb_sim OR p._key IN @window_keys OR bm25 > 1.0
  SORT score DESC
  LIMIT @top_n
  RETURN {key: p._key, text: p.normalized_own_text, emb_sim, bm25, score}
```

The `already_linked` subquery traverses the `reply_graph` named graph in any direction — this replaces the separate `get_explicit_edge_pairs` call and automatically includes both `html_metadata` and `implicit_llm` edges in the exclusion set.

---

## File changes

| File | Change |
|---|---|
| `voz_crawler/core/entities/arango.py` | Replace `ArangoPost`/`EmbedItem`/`EmbedPatch` with `RawPostDoc`, `NormalizedPostDoc`, `ExtractionResultDoc`, updated patch models |
| `voz_crawler/core/entities/enrichment.py` | PydanticAI result models + version constants (transient; projected into `ExtractionResultDoc`) |
| `voz_crawler/core/graph/normalizer.py` | New — `normalize_post_html()` + `NormalizedBlock` dataclass |
| `voz_crawler/core/graph/embedding_sync.py` | Update to use `normalized_own_text` instead of `content_text` |
| `voz_crawler/core/graph/post_sync.py` | Update to build `RawPostDoc` (drop `embedding` field) |
| `voz_crawler/core/graph/implicit_reply_sync.py` | Use `fetch_implicit_reply_candidates` (graph view query) |
| `voz_crawler/core/repository/graph_repository.py` | Add `ensure_schema()` view creation; replace `fetch_embedding_candidates` + `get_explicit_edge_pairs` with `fetch_implicit_reply_candidates`; add `fetch_posts_needing_normalization`, `update_post_normalizations` |
| `voz_crawler/defs/assets/reply_graph.py` | Add `normalize_posts` asset; update `compute_embeddings` dep |
| `voz_crawler/definitions.py` | Register `normalize_posts` |