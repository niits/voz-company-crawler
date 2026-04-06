# Post Enrichment Pipeline

**Date**: 2026-04-06

**Date**: 2026-04-06

**Status**: Experimental (notebook phase)
**Goal**: Enrich the ArangoDB reply graph with structured metadata per post so the graph can answer questions about companies, positions, salaries, events, and sentiment over time.

---

## Overview

Three enrichment tasks run on every post after it is synced to ArangoDB and embedded:

| Task | What it produces |
|---|---|
| Content classification | Is this post informative or noise? |
| Company entity extraction | Which companies are mentioned and how? |
| Implicit reply detection | Conversational replies not captured by HTML quote tags |

All three are **incremental** — only new or changed posts are processed. The `enrichment_version` field on each post document acts as the staleness flag (same pattern as `embedding=None`).

---

## Task 1 — Content Classification

### Output fields (written to `posts` collection)

| Field | Type | Values |
|---|---|---|
| `content_class` | `str \| None` | `review` \| `rating` \| `event` \| `question` \| `noise` |
| `content_class_confidence` | `float \| None` | 0.0–1.0 |
| `enrichment_version` | `int \| None` | Bumped on prompt change to trigger reprocessing |

### Class definitions

| Class | Meaning |
|---|---|
| `review` | First-hand experience or opinion about working at / dealing with a company |
| `rating` | Numerical salary or score mention without full narrative |
| `event` | Company news: layoff, acquisition, IPO, hiring freeze, incident |
| `question` | Asking for information or opinions about a company |
| `noise` | Greetings, reactions (`+1`, `hihi`), very short posts (< 10 words), off-topic |

---

## Task 2 — Company Entity Extraction

### `CompanyMentionResult` — LLM response model

Each post may contain zero or more company mentions. The LLM extracts a list of `CompanyMentionResult` objects.

| Field | Type | Description |
|---|---|---|
| `company_name_raw` | `str` | Exact string as it appeared in the post text |
| `identification_clues` | `list[str]` | All aliases, nicknames, abbreviations, and slang found in the post that could identify the company — **not resolved, just captured** |
| `company_name` | `str` | Best-effort normalized name extracted by LLM (strip "công ty", "cty TNHH", "JSC", etc.) — may still be ambiguous |
| `company_type` | `str` | `it` \| `outsourcing` \| `product` \| `startup` \| `bank` \| `telco` \| `unknown` |
| `mention_type` | `str` | `positive_review` \| `negative_review` \| `salary_info` \| `layoff` \| `hiring` \| `event` \| `general` |
| `sentiment` | `str` | `positive` \| `negative` \| `neutral` |
| `summary` | `str` | One-sentence summary of what is said about this company |

### `identification_clues` — design intent

Vietnamese forum users routinely refer to companies via nicknames, abbreviations, and inside jokes instead of official names. The LLM should capture these raw clues without attempting to resolve them to canonical names.

**Examples of clues the LLM should capture:**

| Raw text in post | `identification_clues` captured | Likely company (not stored here) |
|---|---|---|
| "nhà F", "công ty F" | `["nhà F", "công ty F"]` | FPT Group |
| "ép sọt", "Fsoft" | `["ép sọt", "Fsoft"]` | FPT Software |
| "Nam á đỏ" | `["Nam á đỏ"]` | Nam A Bank, NAB |
| "Vtek", "V-tech" | `["Vtek", "V-tech"]` | VTech (ambiguous) |
| "cty cá" | `["cty cá"]` | Unknown, needs resolution |
| "gã khổng lồ xanh" | `["gã khổng lồ xanh"]` | Possibly Viettel or Samsung |
| "big4 consulting" | `["big4 consulting"]` | Deloitte/PwC/EY/KPMG (ambiguous) |

**Why capture clues instead of resolving immediately:**
- Resolution requires a curated knowledge base of Vietnamese IT company aliases that does not exist yet.
- The same nickname can refer to different companies depending on context or year.
- Capturing clues now enables building that knowledge base from the data itself (clustering clues by co-occurrence, user confirmation, etc.).
- Once a resolution table exists, a separate lightweight pass can update `company_name` without re-running the expensive LLM extraction.

### `CompanyMention` — ArangoDB vertex document

Stored in the `company_mentions` collection. Each mention from a post is one document.

| Field | Type | Description |
|---|---|---|
| `_key` | `str` | `{post_id}_{mention_ordinal}` |
| `post_id` | `str` | ArangoDB `_key` of the source post |
| `partition_key` | `str` | Same partition key as the source post |
| `company_name_raw` | `str` | From `CompanyMentionResult.company_name_raw` |
| `identification_clues` | `list[str]` | From `CompanyMentionResult.identification_clues` |
| `company_name` | `str` | Normalized best-effort name |
| `company_canonical` | `str \| None` | Resolved canonical name — `None` until a resolution pass fills it |
| `company_type` | `str \| None` | |
| `mention_type` | `str` | |
| `sentiment` | `str \| None` | |
| `summary` | `str \| None` | |
| `extraction_version` | `int` | Bumped on prompt change |

### `MentionEdge` — ArangoDB edge

Stored in the `mentions` edge collection: `posts/{post_id}` → `company_mentions/{key}`.

---

## Task 3 — Implicit Reply Detection

Two-stage pipeline per partition:

### Stage 1 — Retrieval (no LLM)

For each post `P`, collect candidate prior posts it might implicitly reply to using three signals combined with an **adaptive time-filtered window**.

#### Time filter

Only consider prior posts within a lookback window `[T_P - max_lookback, T_P)`. Posts older than `max_lookback` are excluded regardless of similarity score.

`max_lookback` is also adaptive — the inverse of the reply rate. In a hot thread, all relevant context is very recent (past few hours). In a slow or resumed thread, someone may reply to a post from days ago, so the time window must be wider.

```
max_lookback_hours = clamp(BASE_LOOKBACK × (BASE_RATE / max(reply_rate, MIN_RATE)),
                           MIN_LOOKBACK_HOURS, MAX_LOOKBACK_HOURS)
```

| Parameter | Default | Meaning |
|---|---|---|
| `BASE_LOOKBACK` | `24 hours` | Lookback at reference reply rate |
| `MIN_RATE` | `1 post/hour` | Floor to prevent very large lookback from near-zero rate |
| `MIN_LOOKBACK_HOURS` | `2` | Always look at least 2 hours back |
| `MAX_LOOKBACK_HOURS` | `168` | Hard upper bound — 7 days |

**Examples**:

| Reply rate | Time lookback |
|---|---|
| 40 posts/hour (hot burst) | `24 × 10/40` = **6 hours** |
| 10 posts/hour (baseline) | `24 × 10/10` = **24 hours** |
| 2 posts/hour (slow) | `24 × 10/2` = **120 hours** (5 days) |
| 0.5 posts/hour (resumed) | `24 × 10/1` = 240 → clamped to **168 hours** |

Together, the adaptive window (post count) and adaptive time filter move in **opposite directions** as rate changes — hot threads get a larger post-count window but a narrower time filter; slow threads get a smaller post-count window but a wider time filter.

#### Adaptive window size

The reply window (the N nearest prior posts always included as candidates) is not fixed — it adapts to the **local reply frequency** measured around post `P`.

**Intuition**: In a hot, fast-moving thread, many people post within a short burst. A reply target can be buried several posts back within the same minute — if the window is too small, it gets missed entirely. A larger window is needed to surface all plausible candidates. In a slow or resumed thread, there are few posts in any time slice, so the window naturally shrinks; a large window would just add noise with no benefit.

**Local measurement**: Rate is measured over the posts immediately preceding `P` within `rate_window` hours. This is important because reply rate changes significantly over the life of a thread — a quiet thread can become a hot thread when a controversial topic surfaces, and the window should reflect the *current* conversation tempo, not the thread average.

**Algorithm**:

```
reply_rate = number of posts in [T_P - rate_window, T_P) / rate_window  (posts/hour)
adaptive_n = round(BASE_N × (reply_rate / BASE_RATE))
adaptive_n = clamp(adaptive_n, MIN_WINDOW, MAX_WINDOW)
```

| Parameter | Default | Meaning |
|---|---|---|
| `rate_window` | `2 hours` | Time span for measuring local reply frequency |
| `BASE_N` | `5` | Window size at reference reply rate |
| `BASE_RATE` | `10 posts/hour` | Reference rate that maps to `BASE_N` |
| `MIN_WINDOW` | `2` | Always include at least 2 prior posts |
| `MAX_WINDOW` | `20` | Hard upper bound — never look back more than 20 posts |

**Examples**:

| Reply rate | Adaptive window | Reasoning |
|---|---|---|
| 40 posts/hour (hot burst) | `round(5 × 40/10)` = 20 → clamped to **20** | Dense conversation; reply target may be many indices back |
| 10 posts/hour (baseline) | `round(5 × 10/10)` = **5** | Moderate pace; standard window |
| 2 posts/hour (slow) | `round(5 × 2/10)` = **1** → clamped to MIN **2** | Sparse; few candidates available anyway |
| 0 posts/hour (resumed) | `round(5 × 0/10)` = 0 → clamped to MIN **2** | Cold restart; just look at the nearest 2 prior posts |

#### Scoring signals (applied within the filtered pool)

| Signal | Method | Weight |
|---|---|---|
| Reply window | Adaptive N nearest prior posts (within time filter) | 0.20 |
| Embedding similarity | Cosine similarity ≥ 0.65 against filtered pool | 0.50 |
| BM25 lexical | Token overlap score > 1.0 against filtered pool | 0.30 |

Candidates are deduplicated, posts already linked via explicit `html_metadata` edges are removed, and the top 5 by combined score are returned.

### Stage 2 — LLM Decision (PydanticAI agent)

The source post + all candidates are sent to `gpt-4o-mini` in a single call. The LLM decides which candidates (if any) represent an implicit reply relationship.

**Accepted edges**: `confidence >= 0.6`, stored in the `quotes` collection with `method="implicit_llm"`.

### `ImplicitReply` — LLM response model

| Field | Type | Description |
|---|---|---|
| `candidate_key` | `str` | `_key` of the post being replied to |
| `confidence` | `float` | 0.0–1.0 |
| `reasoning` | `str` | Why the LLM believes this is an implicit reply |

---

## Incremental Processing

### Staleness flags

| Field | Set by | Checked by | Triggers reprocess when |
|---|---|---|---|
| `embedding` | `compute_embeddings` | `fetch_posts_needing_embedding` | `== null` |
| `enrichment_version` | `classify_posts` | `fetch_posts_needing_enrichment` | `null` or `< ENRICHMENT_VERSION` |

### Staging field

`classify_posts` writes the raw `PostEnrichmentResult` JSON to `enrichment_staging` on each post document. `extract_company_mentions` reads this field, upserts `CompanyMention` docs, then clears `enrichment_staging`. This separation means:

- Company normalization logic can be re-run without re-paying for LLM classification.
- `enrichment_staging` doubles as an audit trail until cleared.

### Future: alias resolution pass

A separate low-cost pass (no LLM required) will:
1. Collect all `identification_clues` across `company_mentions`.
2. Cluster them by similarity + co-occurrence to discover alias groups.
3. Match against a hand-curated (or crowd-sourced) resolution table.
4. Write `company_canonical` back to matching `company_mentions` documents.

---

## Dagster Asset Topology

```
voz_page_posts_assets
  └─► sync_posts_to_arango
        └─► compute_embeddings
              └─► classify_posts              group=reply_graph, compute_kind=OpenAI
                    ├─► extract_company_mentions  group=reply_graph, compute_kind=ArangoDB
                    └─► detect_implicit_replies   group=reply_graph, compute_kind=OpenAI
```

All new assets: `AutomationCondition.eager()`, `IdentityPartitionMapping`.
No new Dagster resources — reuses `arango` and `openai`.

---

## LLM Implementation

All LLM-related functions use **PydanticAI** agents:
- Agents are typed with Pydantic result models — no manual `response_format` JSON schema construction.
- Dependencies (ArangoDB repo, config) injected via `RunContext[deps]`.
- Model: `openai:gpt-4o-mini`, `temperature=0`.

---

## New Files

| File | Purpose |
|---|---|
| `voz_crawler/core/entities/enrichment.py` | PydanticAI result models + `ENRICHMENT_VERSION` |
| `voz_crawler/core/graph/enrichment_sync.py` | PydanticAI agents: classify + extract |
| `voz_crawler/core/graph/implicit_reply_sync.py` | Stage 1 retrieval + PydanticAI agent for Stage 2 |
| `voz_crawler/core/graph/company_sync.py` | `build_company_mention_docs` |
| `voz_crawler/core/entities/arango.py` | Add `EnrichItem`, `EnrichmentPatch`, `CompanyMention`, `MentionEdge` |
| `voz_crawler/core/repository/graph_repository.py` | New AQL methods + `ensure_schema()` additions |
| `voz_crawler/defs/assets/reply_graph.py` | 3 new `@asset` functions |
| `voz_crawler/definitions.py` | Register 3 new assets |
| `notebooks/enrichment-experiments.ipynb` | Concept validation notebook (PydanticAI) |

---

## Open Questions

- **Confidence threshold for implicit replies**: 0.6 is a starting point — tune after running experiments on a few partitions.
- **Alias resolution table format**: JSON file in `data/`? ArangoDB collection? TBD after enough `identification_clues` are collected.
- **Cross-partition implicit replies**: Currently scoped to same partition only. If needed later, can extend by fetching embeddings from adjacent partitions (pages N-1, N+1).