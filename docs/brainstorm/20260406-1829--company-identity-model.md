# Company Identity Model

**Date**: 2026-04-06

**Date**: 2026-04-06

**Status**: Spec
**Goal**: Resolve `identification_clues` captured from posts to canonical company entities, in a way that handles ambiguity and evolves as the thread provides more evidence — without requiring LLM re-runs when resolution changes.

---

## The core problem

Forum posts use nicknames, slang, and abbreviations instead of official company names. The same alias can:

- Be unambiguous ("Fsoft" almost always means FPT Software)
- Be ambiguous ("nhà F" could mean FPT Group or FPT Software)
- Change meaning over time as the community context shifts
- Be disambiguated by a later post ("bên nhà F tức là Fsoft")

Resolution of `identification_clues → canonical company` is a **separate concern** from extraction. It is mutable, versioned, and evidence-driven. It must not require re-running the LLM when a new post changes what an old alias means.

---

## Separation of concerns

```
LLM extraction (immutable once stored)
  → identification_clues: ["nhà F", "ép sọt"]    captured signals, never changed

Alias resolution (mutable, versioned)
  → company_aliases: "nhà F" → {fpt-software: 0.8, fpt-group: 0.4}

Canonical entities (curated)
  → companies: "fpt-software", "fpt-group", ...

Derived fact (re-derivable without LLM)
  → company_mentions.company_key = "fpt-software"   projected from signals + resolution
```

When a new post changes the resolution of "nhà F", only `company_aliases` and the derived `company_key` projection change. The original `identification_clues` and `ExtractionResultDoc` are untouched.

---

## ArangoDB collections

### `companies` — canonical company entities (vertex)

Manually seeded for well-known companies; auto-created as stubs for newly discovered ones.

```python
class CompanyDoc(SQLModel):
    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")          # slugified canonical name, e.g. "fpt-software"
    canonical_name: str                     # "FPT Software"
    company_type: str | None                # it | outsourcing | product | startup | bank | telco
    parent_key: str | None                  # e.g. "fpt-group" for FPT Software
    is_stub: bool                           # True = auto-created, not yet curated
    created_at: str
    updated_at: str
```

`parent_key` models corporate hierarchy: FPT Software → FPT Group. Queries can traverse upward to aggregate across subsidiaries.

`is_stub=True` means the company was created automatically from a high-confidence alias resolution but has not been curated. Stubs can be merged, renamed, or confirmed by a human or a future curation pass.

---

### `company_aliases` — alias-to-company resolution (vertex)

One document per unique alias string (lowercased, stripped). The resolution is a ranked list of candidate companies with confidence scores, not a single answer. This models ambiguity as a first-class concept.

```python
class CompanyAliasDoc(SQLModel):
    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")          # slugify(alias), e.g. "nha-f"
    alias: str                              # original form, e.g. "nhà F"
    alias_normalized: str                   # lowercase, diacritic-stripped

    # Ranked resolution candidates — sorted by confidence DESC
    resolutions: list[dict]
    # Each entry: {company_key, confidence, evidence_count, last_evidence_post_key, last_updated}

    # Derived convenience fields (recomputed after each evidence update)
    best_company_key: str | None            # top resolution if confidence > ambiguity_threshold
    is_ambiguous: bool                      # True when top-2 confidences are within 0.15 of each other
    is_unresolved: bool                     # True when no resolution has confidence > 0.4

    resolution_version: int                 # bumped each time resolutions change; used to find stale company_mentions
```

**Ambiguity rule**: if the top-2 candidates are within 0.15 confidence of each other, `is_ambiguous=True` and `best_company_key=None`. Downstream mentions using this alias get `company_key=None` and `is_ambiguous=True`.

---

### `alias_evidence` — append-only resolution evidence log (vertex)

Every post that provides information about what an alias means creates a document here. This is the source of truth from which `company_aliases.resolutions` is recomputed.

```python
class AliasEvidenceDoc(SQLModel):
    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")          # f"{post_key}_{alias_slug}"
    alias: str
    alias_slug: str                         # matches company_aliases._key
    company_key: str                        # which company this evidence points to
    source_post_key: str
    partition_key: str

    evidence_type: str
    # explicit_definition: post directly states what alias means ("nhà F tức là Fsoft")
    # co_occurrence: alias appears in same post as canonical name with no contradiction
    # disambiguation: post uses alias to contrast two companies ("nhà F chứ không phải NAB")
    # contradiction: post challenges existing resolution ("nhà F ở đây là FPT Corp, không phải Fsoft")

    confidence: float                       # assigned by LLM or rule; contradiction = negative weight
    enrichment_version: int
    extracted_at: str
```

Evidence is **never deleted or updated** — new evidence is appended. `company_aliases.resolutions` is recomputed by summing evidence weights per company, applying recency decay if desired.

---

### `company_mentions` — updated with resolution fields

The `CompanyMentionResult` from LLM extraction is stored in `ExtractionResultDoc`. The `CompanyMention` vertex in ArangoDB adds two resolution fields that are populated by the resolution pass:

```python
# Additional fields on CompanyMention (from data-model-reorganization.md):
company_key: str | None                    # resolved canonical company _key, null if unresolved/ambiguous
is_ambiguous: bool                         # True when alias resolution is ambiguous
resolution_version: int | None             # company_aliases.resolution_version at time of resolution
```

When `company_aliases.resolution_version` changes for an alias, a backfill finds all `company_mentions` where `identification_clues` contains that alias and `resolution_version` is stale, then re-projects `company_key`.

---

## Evidence collection — how aliases get resolved

Resolution evidence comes from three sources, no LLM re-run required for sources 2 and 3:

### Source 1 — LLM detection (during `classify_posts`)

The `PostEnrichmentResult` model gains an optional field:

```python
class AliasDefinitionResult(BaseModel):
    alias: str                              # the alias being defined
    company_name: str                       # what it resolves to (LLM's best guess)
    evidence_type: str                      # explicit_definition | co_occurrence | disambiguation
    confidence: float

class PostEnrichmentResult(BaseModel):
    ...
    alias_definitions: list[AliasDefinitionResult]  # new field; empty list if no definitions found
```

When the LLM sees "nhà F tức là Fsoft" it populates `alias_definitions`. This is projected into `AliasEvidenceDoc` by the `extract_company_mentions` asset.

### Source 2 — Co-occurrence rule (no LLM)

If a post's `identification_clues` contains alias "nhà F" and the same post's `company_mentions` also contains an explicit `company_name="FPT Software"`, that is weak co-occurrence evidence. Applied in a rule-based pass after LLM extraction.

### Source 3 — Manual curation

A human (or a future curation UI) can directly write `AliasEvidenceDoc` records to resolve disputed aliases. These get the highest base confidence (`1.0`) and override ambiguity.

---

## Resolution recomputation

When new `AliasEvidenceDoc` documents arrive for an alias:

```
1. Load all AliasEvidenceDoc for this alias
2. Sum confidence per candidate company:
   - explicit_definition: weight 1.0
   - co_occurrence: weight 0.3
   - disambiguation: weight 0.7
   - contradiction: weight -0.5 (reduces confidence of the contradicted company)
3. Normalize scores to [0, 1]
4. Update company_aliases.resolutions, best_company_key, is_ambiguous, is_unresolved
5. Increment company_aliases.resolution_version
6. Trigger backfill: UPDATE company_mentions WHERE alias IN identification_clues AND resolution_version < new_version
```

This is a pure database operation — no LLM, no HTTP calls. Runs in the `resolve_company_mentions` asset.

---

## "Changing resolution" scenario

**Setup**: early in the thread, "nhà F" has no resolution evidence. 500 posts later, post A says "bên nhà F (tức Fsoft)" — this is an explicit definition. 300 posts after that, post B says "nhà F trong ngữ cảnh này là FPT Corp nhé mọi người" — this is a contradiction.

**State after post A**:
```
company_aliases["nha-f"].resolutions = [{company_key: "fpt-software", confidence: 0.85}]
company_aliases["nha-f"].best_company_key = "fpt-software"
company_aliases["nha-f"].is_ambiguous = false
company_aliases["nha-f"].resolution_version = 1

→ backfill: all company_mentions with "nhà F" in identification_clues
  get company_key="fpt-software", resolution_version=1
```

**State after post B**:
```
alias_evidence: new record {alias: "nhà F", company_key: "fpt-group", evidence_type: "contradiction", confidence: 0.7}

recompute:
  fpt-software score: 1.0 (explicit) - 0.5 (contradiction applied to fpt-software) = 0.5
  fpt-group score: 0.7 (disambiguation)
  normalized: fpt-software=0.42, fpt-group=0.58
  delta < 0.15 → is_ambiguous = True

company_aliases["nha-f"].best_company_key = None
company_aliases["nha-f"].is_ambiguous = True
company_aliases["nha-f"].resolution_version = 2

→ backfill: company_mentions with resolution_version=1 and "nhà F" in clues
  get company_key=None, is_ambiguous=True, resolution_version=2
```

The old posts are now correctly marked as ambiguous. The original `identification_clues` and LLM extraction are untouched. If more evidence arrives later and resolves the ambiguity, another backfill restores `company_key`.

---

## Dagster assets

```
classify_posts
  └── extract_company_mentions         (writes CompanyMention + AliasEvidenceDoc)
        └── resolve_company_mentions   (NEW, no LLM) — recomputes alias resolutions,
                                        backfills company_key on stale company_mentions
```

`resolve_company_mentions`:
- Runs after `extract_company_mentions`
- Checks for new `AliasEvidenceDoc` records since last resolution run
- Recomputes affected `CompanyAliasDoc` entries
- Backfills `company_mentions` where `resolution_version` is stale
- Creates stub `CompanyDoc` entries for newly-discovered canonical names
- No LLM, no OpenAI resource needed — `arango` resource only

---

## Query patterns enabled

```aql
// All posts about FPT Software (including via aliases)
FOR m IN company_mentions
  FILTER m.company_key == "fpt-software"
  FILTER m.is_ambiguous == false
  LET post = DOCUMENT("posts", m.post_id)
  RETURN {mention_type: m.mention_type, sentiment: m.sentiment, post: post.normalized_own_text}

// Include parent company (FPT Group = all FPT entities)
FOR c IN companies
  FILTER c.key == "fpt-group" OR c.parent_key == "fpt-group"
  FOR m IN company_mentions
    FILTER m.company_key == c._key
    RETURN m

// What aliases exist for a company?
FOR a IN company_aliases
  FILTER a.best_company_key == "fpt-software"
  RETURN a.alias

// All unresolved aliases needing curation
FOR a IN company_aliases
  FILTER a.is_unresolved == true OR a.is_ambiguous == true
  SORT LENGTH(FOR m IN company_mentions FILTER m.identification_clues ANY == a.alias RETURN 1) DESC
  LIMIT 20
  RETURN {alias: a.alias, mention_count: ..., top_candidates: a.resolutions}
```

---

## New files

| File | Purpose |
|---|---|
| `voz_crawler/core/entities/company.py` | `CompanyDoc`, `CompanyAliasDoc`, `AliasEvidenceDoc` — ArangoDB models |
| `voz_crawler/core/graph/alias_resolution.py` | Confidence recomputation logic + backfill query builder |
| `voz_crawler/core/repository/graph_repository.py` | New methods: `upsert_companies`, `upsert_alias_evidence`, `recompute_alias_resolutions`, `backfill_company_mentions_resolution` |
| `voz_crawler/defs/assets/reply_graph.py` | New `resolve_company_mentions` asset |
| `voz_crawler/definitions.py` | Register new asset |

`ensure_schema()` additions:
- `companies` vertex collection + index on `is_stub`
- `company_aliases` vertex collection + index on `alias_normalized`, `resolution_version`
- `alias_evidence` vertex collection + index on `alias_slug`, `source_post_key`