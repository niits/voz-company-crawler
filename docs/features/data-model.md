# Data Model

All ArangoDB collections, Python models, and the layer contract that governs them.

---

## Layer contract

```
Layer 0 — PostgreSQL (dlt-managed, read-only from this pipeline)
  RawPost          one row per forum post; source of truth for ingestion

Layer 1 — ArangoDB posts collection, raw fields
  RawPostDoc       written once by sync_posts_to_arango; replaced only when content_hash changes

Layer 2 — ArangoDB posts collection, enrichment fields
  NormalizedPostDoc  PATCH-only; each asset writes only the fields it owns; never replaces layer 1

Layer 3 — ArangoDB extraction_results collection
  ExtractionResultDoc  persisted LLM output; one document per (post, enrichment_version)
```

All layers share the same ArangoDB `posts` document — layers 1 and 2 are logical separation of
write ownership, not separate collections. Layer 3 is a separate collection.

---

## Layer 1 — `RawPostDoc`

```python
class RawPostDoc(SQLModel):
    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")      # str(post_id_on_site)
    post_id: int
    author_username: str | None
    author_id: str | None
    posted_at: str | None               # ISO datetime
    content_text: str | None            # raw full text including quoted blocks
    content_hash: str                   # SHA-256(content_text); change triggers full re-enrichment
    partition_key: str                  # "{thread_id}:{page_number}"
    thread_url: str
    page_number: int
```

Written by `sync_posts_to_arango` with `on_duplicate="replace"`. Replacing resets all layer 2
fields to null — this is the staleness propagation mechanism.

---

## Layer 2 — `NormalizedPostDoc`

All fields are null until the asset that owns them runs.

```python
class NormalizedPostDoc(SQLModel):
    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")

    # Owned by: normalize_posts
    normalized_own_text: str | None         # author's own words; quoted blocks stripped
    normalized_quoted_blocks: list[dict] | None
    # Each block: {author: str|None, source_post_id: int|None, text: str}
    normalization_version: int | None

    # Owned by: compute_embeddings
    embedding: list[float] | None           # embedding of normalized_own_text (NOT content_text)
    embedding_model: str | None

    # Owned by: classify_posts  (fast-filter projections from ExtractionResultDoc)
    content_class: str | None               # review | rating | event | question | noise
    content_class_confidence: float | None
    has_company_mention: bool | None
    enrichment_version: int | None
```

### Staleness chain

```
content_hash changes
  → on_duplicate="replace" resets all layer 2 fields to null
      → normalize_posts re-runs  → normalized_own_text updated
          → compute_embeddings re-runs  → embedding updated
              → classify_posts re-runs  → enrichment_version updated
```

Bumping `NORMALIZATION_VERSION` or `ENRICHMENT_VERSION` constants triggers reprocessing of
all posts regardless of content changes.

---

## Layer 3 — `ExtractionResultDoc`

Persisted LLM output. Decoupled from the post document so extraction can be re-read, audited,
or re-projected without re-calling the LLM.

```python
class ExtractionResultDoc(SQLModel):
    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")          # f"{post_key}_{enrichment_version}"
    post_key: str
    partition_key: str
    enrichment_version: int

    # Classification
    content_class: str
    content_class_confidence: float

    # Company mentions — full LLM output stored verbatim
    company_mentions: list[dict]            # list[CompanyMentionResult.model_dump()]

    # Alias definitions the LLM detected in this post
    alias_definitions: list[dict]           # list[AliasDefinitionResult.model_dump()]

    # Implicit reply decisions — written by detect_implicit_replies after classify_posts
    implicit_replies: list[dict]            # list[ImplicitReply.model_dump()]

    # Provenance
    model_used: str                         # e.g. "gpt-4o-mini"
    extracted_at: str                       # ISO datetime
```

On version bump: the new document (`f"{post_key}_{new_version}"`) is inserted; the old one is
retained for comparison. `on_duplicate="replace"` applies only within the same version.

---

## Graph collections

### `quotes` — explicit + implicit reply edges

```python
class ArangoEdge(SQLModel):
    model_config = ConfigDict(populate_by_name=True)

    from_vertex: str = Field(alias="_from")     # "posts/{post_id}"
    to_vertex: str = Field(alias="_to")         # "posts/{post_id}"
    key: str = Field(alias="_key")
    quote_ordinal: int
    confidence: float                           # 1.0 for html_metadata; LLM score for implicit_llm
    method: str                                 # "html_metadata" | "implicit_llm"
    partition_key: str
```

`extract_explicit_edges` manages `html_metadata` edges (drop-and-reinsert per partition).
`detect_implicit_replies` inserts `implicit_llm` edges (never drops — implicit edges are not
recomputed unless the post changes).

**Unattributed quotes** (`data-source=""`): `extract_explicit_edges` counts them but cannot
create an edge. Surfaced as `unresolvable_quotes` in `MaterializeResult` metadata.

### `company_mentions` — company mention vertices

```python
class CompanyMentionDoc(SQLModel):
    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")              # f"{post_key}_{mention_ordinal}"
    post_key: str
    partition_key: str

    # From LLM extraction (immutable after write)
    company_name_raw: str
    identification_clues: list[str]             # aliases/slang as-found; never resolved here
    company_name: str                           # LLM best-effort normalized name
    company_type: str | None                    # it | outsourcing | product | startup | bank | telco | unknown
    mention_type: str                           # positive_review | negative_review | salary_info |
                                                # layoff | hiring | event | general
    sentiment: str | None                       # positive | negative | neutral
    summary: str | None

    # From resolution pass (mutable; updated by resolve_company_mentions)
    company_key: str | None                     # references companies._key; null = unresolved/ambiguous
    is_ambiguous: bool                          # true when alias resolution is contested
    resolution_version: int | None              # company_aliases.resolution_version at last projection
```

### `mentions` — edges from posts to company_mentions

```python
class MentionEdge(SQLModel):
    model_config = ConfigDict(populate_by_name=True)

    from_vertex: str = Field(alias="_from")     # "posts/{post_key}"
    to_vertex: str = Field(alias="_to")         # "company_mentions/{key}"
    key: str = Field(alias="_key")
    partition_key: str
```

### `companies`, `company_aliases`, `alias_evidence`

Defined in `docs/features/company-identity.md`.

---

## LLM response models (transient Python, `voz_crawler/core/entities/enrichment.py`)

```python
NORMALIZATION_VERSION = 1
ENRICHMENT_VERSION = 1

class ContentClassification(BaseModel):
    content_class: str      # review | rating | event | question | noise
    confidence: float

class CompanyMentionResult(BaseModel):
    company_name_raw: str
    identification_clues: list[str]
    company_name: str
    company_type: str
    mention_type: str
    sentiment: str
    summary: str

class AliasDefinitionResult(BaseModel):
    alias: str
    company_name: str       # what the LLM believes this alias resolves to
    evidence_type: str      # explicit_definition | co_occurrence | disambiguation
    confidence: float

class PostEnrichmentResult(BaseModel):
    post_key: str
    classification: ContentClassification
    company_mentions: list[CompanyMentionResult]
    alias_definitions: list[AliasDefinitionResult]   # empty list if none found

class ImplicitReply(BaseModel):
    candidate_key: str
    confidence: float
    reasoning: str

class ImplicitReplyDecision(BaseModel):
    source_key: str
    replies_to: list[ImplicitReply]
```

---

## Content normalization (`voz_crawler/core/graph/normalizer.py`)

`normalize_post_html(html: str) -> dict` parses raw HTML, separates the author's own text from
quoted blocks, and returns:

```python
{
    "own_text": str,           # author's actual content; empty string if post is a pure quote
    "quoted_blocks": [
        {
            "author": str | None,           # data-quote attribute; None for unattributed quotes
            "source_post_id": int | None,   # data-source post id; None for unattributed quotes
            "text": str,
        }
    ]
}
```

Smilie image tags (`<img class="smilie">`) are stripped; their alt codes (`:sweat:`) are removed
with `re.sub(r':[a-z_]+:', '', text)`.

**Unattributed quotes** (`data-source=""`, `data-quote=""`): content is still extracted into
`quoted_blocks` with `author=None`, `source_post_id=None`. No edge is created.

---

## `ensure_schema()` — full collection list

| Collection | Type | Indexes |
|---|---|---|
| `posts` | vertex | `partition_key`, ArangoSearch view `reply_graph_search` |
| `quotes` | edge | `partition_key` |
| `extraction_results` | vertex | `(post_key, enrichment_version)`, `partition_key` |
| `company_mentions` | vertex | `partition_key`, `company_key`, `resolution_version` |
| `mentions` | edge | `partition_key` |
| `companies` | vertex | `is_stub` |
| `company_aliases` | vertex | `alias_normalized`, `resolution_version` |
| `alias_evidence` | vertex | `alias_slug`, `source_post_key` |

Named graph `reply_graph` edge definitions:

| Edge collection | From | To |
|---|---|---|
| `quotes` | `posts` | `posts` |
| `mentions` | `posts` | `company_mentions` |

ArangoSearch view `reply_graph_search` indexes `posts.normalized_own_text` (analyzer: `text_en`
fallback if `text_vi` unavailable), `posts.embedding`, `posts.partition_key`.
