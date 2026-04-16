# Company Identity Model

Resolves `identification_clues` captured from posts to canonical company entities.
Resolution is evidence-driven, versioned, and never requires LLM re-runs when it changes.

---

## Design principle

```
identification_clues  →  immutable signals captured at extraction time
company_aliases       →  mutable mapping: alias → candidate companies with confidence
alias_evidence        →  append-only log of posts that define or clarify an alias
company_mentions.company_key  →  derived projection; re-computable from signals × aliases
```

When a new post changes the meaning of an alias, only `company_aliases` and the projection
change. The original `identification_clues` and `ExtractionResultDoc` are untouched.

---

## Collections

### `companies` — canonical company entities

```python
class CompanyDoc(SQLModel):
    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")      # slugified canonical name, e.g. "fpt-software"
    canonical_name: str                 # "FPT Software"
    company_type: str | None            # it | outsourcing | product | startup | bank | telco
    parent_key: str | None              # e.g. "fpt-group"; models corporate hierarchy
    is_stub: bool                       # True = auto-created, not yet curated
    created_at: str
    updated_at: str
```

Manually seeded for well-known companies. Auto-created as stubs when `resolve_company_mentions`
encounters a new canonical name from a high-confidence alias resolution. Stubs can be merged,
renamed, or confirmed by a human.

### `company_aliases` — alias-to-company resolution

One document per unique alias. Ambiguity is first-class: the resolution is a ranked list of
candidates, not a single answer.

```python
class CompanyAliasDoc(SQLModel):
    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")          # slugify(alias_normalized)
    alias: str                              # original form, e.g. "nhà F"
    alias_normalized: str                   # lowercase, diacritics stripped, e.g. "nha f"

    resolutions: list[dict]
    # Each entry: {
    #   company_key: str,
    #   confidence: float,       # sum of weighted evidence (see algorithm below)
    #   evidence_count: int,
    #   last_evidence_post_key: str,
    #   last_updated: str
    # }
    # Sorted by confidence DESC.

    best_company_key: str | None    # top resolution if confidence gap > AMBIGUITY_THRESHOLD (0.15)
    is_ambiguous: bool              # True when top-2 confidences differ by < 0.15
    is_unresolved: bool             # True when no resolution has confidence > 0.4

    resolution_version: int         # bumped each time resolutions change
```

### `alias_evidence` — append-only evidence log

```python
class AliasEvidenceDoc(SQLModel):
    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")          # f"{source_post_key}_{alias_slug}"
    alias: str
    alias_slug: str                         # matches company_aliases._key
    company_key: str                        # which company this evidence supports
    source_post_key: str
    partition_key: str
    evidence_type: str
    confidence: float                       # base weight before evidence_type multiplier
    enrichment_version: int
    extracted_at: str
```

Evidence types and their weight multipliers:

| Type | Multiplier | Example |
|---|---|---|
| `explicit_definition` | 1.0 | "nhà F tức là Fsoft" |
| `disambiguation` | 0.7 | "nhà F chứ không phải NAB" |
| `co_occurrence` | 0.3 | alias and canonical name in same post, no contradiction |
| `contradiction` | −0.5 | "nhà F ở đây là FPT Corp, không phải Fsoft" (reduces existing resolution) |

Evidence is never deleted or updated.

---

## Resolution algorithm

Runs in `alias_resolution.py` whenever new `AliasEvidenceDoc` records arrive:

```
for each affected alias:
    load all AliasEvidenceDoc for this alias
    aggregate confidence per company_key:
        score[company_key] += evidence.confidence × type_multiplier(evidence.evidence_type)
        (contradiction applies −0.5 × confidence to the contradicted company)
    normalize scores to [0, 1]
    sort by score DESC → new resolutions list
    best = resolutions[0] if (resolutions[0].score - resolutions[1].score) > 0.15
    is_ambiguous = not best
    is_unresolved = resolutions[0].score < 0.4
    increment resolution_version
    save CompanyAliasDoc
```

---

## Backfill

After recomputing aliases, all `CompanyMentionDoc` records that used an updated alias and
have a stale `resolution_version` are updated:

```aql
FOR m IN company_mentions
  FILTER m.identification_clues ANY == @alias
  FILTER m.resolution_version < @new_version OR m.resolution_version == null
  UPDATE m WITH {
    company_key: @best_company_key,
    is_ambiguous: @is_ambiguous,
    resolution_version: @new_version
  } IN company_mentions
```

This is a pure database operation — no LLM, no HTTP calls.

---

## Evolving resolution scenario

**Post A** (early in thread): "bên nhà F (tức Fsoft) lương khá"
→ `alias_evidence`: `{alias: "nhà F", company_key: "fpt-software", type: "explicit_definition", confidence: 0.9}`
→ `company_aliases["nha-f"]`: `best_company_key="fpt-software"`, `resolution_version=1`
→ backfill: all `company_mentions` with "nhà F" in `identification_clues` → `company_key="fpt-software"`

**Post B** (later in thread): "nhà F trong ngữ cảnh này là FPT Corp nhé mọi người"
→ `alias_evidence`: `{alias: "nhà F", company_key: "fpt-group", type: "contradiction", confidence: 0.7}`
→ recompute:
  - `fpt-software`: `0.9 × 1.0 + 0.7 × (−0.5)` = 0.55, normalized ~0.44
  - `fpt-group`: `0.7 × 0.7` = 0.49, normalized ~0.39 (using disambiguation weight)
  - gap = 0.05 < 0.15 → `is_ambiguous=True`, `best_company_key=None`
→ `resolution_version=2`
→ backfill: all `company_mentions` with `resolution_version=1` → `company_key=None`, `is_ambiguous=True`

Original `identification_clues` and `ExtractionResultDoc` are unchanged throughout.

---

## Evidence sources

Evidence comes from three sources; no LLM re-run is needed for sources 2 and 3.

**Source 1 — LLM** (`classify_posts`): `PostEnrichmentResult.alias_definitions` contains
`AliasDefinitionResult` records when the LLM detects an explicit alias definition or
disambiguation in the post text. Projected into `AliasEvidenceDoc` by `extract_company_mentions`.

**Source 2 — Co-occurrence rule** (no LLM): if a post's `identification_clues` contains an
alias and the same post's `company_mentions` contains an explicit canonical `company_name`,
a `co_occurrence` evidence record is created. Applied in `resolve_company_mentions`.

**Source 3 — Manual curation**: a human writes `AliasEvidenceDoc` records directly.
Manual evidence uses `confidence=1.0` and overrides automated ambiguity.

---

## Query patterns

```aql
// All posts about FPT Software (resolved, unambiguous)
FOR m IN company_mentions
  FILTER m.company_key == "fpt-software" AND m.is_ambiguous == false
  LET post = DOCUMENT("posts", m.post_key)
  RETURN {type: m.mention_type, sentiment: m.sentiment, text: post.normalized_own_text}

// FPT Group and all subsidiaries
FOR c IN companies
  FILTER c._key == "fpt-group" OR c.parent_key == "fpt-group"
  FOR m IN company_mentions
    FILTER m.company_key == c._key
    RETURN m

// All aliases for a company
FOR a IN company_aliases
  FILTER a.best_company_key == "fpt-software"
  RETURN a.alias

// Unresolved aliases sorted by mention frequency (curation priority queue)
FOR a IN company_aliases
  FILTER a.is_unresolved == true OR a.is_ambiguous == true
  LET mention_count = LENGTH(
    FOR m IN company_mentions FILTER m.identification_clues ANY == a.alias RETURN 1
  )
  SORT mention_count DESC
  LIMIT 20
  RETURN {alias: a.alias, mention_count, candidates: a.resolutions}
```
