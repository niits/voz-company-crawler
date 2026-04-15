from pydantic import ConfigDict, Field
from sqlmodel import SQLModel


class CompanyMentionDoc(SQLModel):
    """Vertex document in the company_mentions collection.

    One document per (post, mention_ordinal) pair.
    Immutable LLM extraction fields are set on first write.
    Resolution fields (company_key, is_ambiguous, resolution_version) are updated
    by resolve_company_mentions as alias confidence changes.
    """

    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")              # f"{post_key}_{mention_ordinal}"
    post_key: str
    partition_key: str

    # From LLM extraction (immutable after write)
    company_name_raw: str
    identification_clues: list[str]             # aliases/slang as-found; never resolved here
    company_name: str                           # LLM best-effort normalized name
    company_type: str | None                    # it | outsourcing | product | startup | bank | telco | unknown
    mention_type: str                           # positive_review | negative_review | salary_info | layoff | hiring | event | general
    sentiment: str | None                       # positive | negative | neutral
    summary: str | None

    # From resolution pass (mutable; updated by resolve_company_mentions)
    company_key: str | None = None              # references companies._key; null = unresolved
    is_ambiguous: bool = False
    resolution_version: int | None = None


class MentionEdge(SQLModel):
    """Edge from posts → company_mentions in the reply_graph."""

    model_config = ConfigDict(populate_by_name=True)

    from_vertex: str = Field(alias="_from")     # "posts/{post_key}"
    to_vertex: str = Field(alias="_to")         # "company_mentions/{key}"
    key: str = Field(alias="_key")
    partition_key: str


class CompanyDoc(SQLModel):
    """Canonical company entity in the companies collection.

    Manually seeded for well-known companies. Auto-created as stubs when
    resolve_company_mentions encounters a new name from high-confidence alias resolution.
    """

    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")      # slugified canonical name, e.g. "fpt-software"
    canonical_name: str                 # "FPT Software"
    company_type: str | None            # it | outsourcing | product | startup | bank | telco
    parent_key: str | None = None       # e.g. "fpt-group"; models corporate hierarchy
    is_stub: bool = True                # True = auto-created, not yet curated
    created_at: str
    updated_at: str


class CompanyAliasDoc(SQLModel):
    """Alias-to-company resolution document in the company_aliases collection.

    One document per unique alias. Ambiguity is first-class: resolutions is a ranked list,
    not a single answer. Updated by resolve_company_mentions when new evidence arrives.
    """

    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")          # slugify(alias_normalized)
    alias: str                              # original form, e.g. "nhà F"
    alias_normalized: str                   # lowercase, diacritics stripped, e.g. "nha f"

    resolutions: list[dict]
    # Each entry: {company_key, confidence, evidence_count,
    #              last_evidence_post_key, last_updated}
    # Sorted by confidence DESC.

    best_company_key: str | None = None     # top resolution if confidence gap > 0.15
    is_ambiguous: bool = False              # True when top-2 confidences differ by < 0.15
    is_unresolved: bool = True              # True when no resolution has confidence > 0.4
    resolution_version: int = 0             # bumped each time resolutions change


class AliasEvidenceDoc(SQLModel):
    """Append-only evidence log for alias resolution in the alias_evidence collection."""

    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")          # f"{source_post_key}_{alias_slug}"
    alias: str
    alias_slug: str                         # matches company_aliases._key
    company_key: str                        # which company this evidence supports
    source_post_key: str
    partition_key: str
    evidence_type: str                      # explicit_definition | disambiguation | co_occurrence | contradiction
    confidence: float                       # base weight before evidence_type multiplier
    enrichment_version: int
    extracted_at: str
