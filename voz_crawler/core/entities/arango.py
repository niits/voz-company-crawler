from pydantic import ConfigDict, Field
from sqlmodel import SQLModel


class RawPostDoc(SQLModel):
    """Layer 1 — raw post document written to ArangoDB posts collection.

    Written once by sync_posts_to_arango. Replaced only when content_hash changes;
    replacement resets all Layer 2 (NormalizedPostDoc) fields to null.
    Use model_dump(by_alias=True) for python-arango bulk operations.
    """

    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")  # str(post_id_on_site)
    post_id: int
    author_username: str | None
    author_id: str | None
    posted_at: str | None  # ISO datetime
    content_text: str | None  # raw full text including quoted blocks
    content_hash: str  # SHA-256(content_text); change triggers re-enrichment
    partition_key: str  # "{thread_id}:{page_number}"
    thread_url: str
    page_number: int


class NormalizedPostDoc(SQLModel):
    """Layer 2 — enrichment patch payload for an existing post document.

    Written by multiple assets via update_many (PATCH semantics).
    Each asset only patches the fields it owns; Layer 1 fields are never touched.
    All fields are None until the relevant enrichment asset runs.
    Use model_dump(by_alias=True) for python-arango update_many.
    """

    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")

    # Owned by: normalize_posts
    normalized_own_text: str | None = None
    normalized_quoted_blocks: list[dict] | None = None
    normalization_version: int | None = None

    # Owned by: compute_embeddings
    embedding: list[float] | None = None
    embedding_model: str | None = None

    # Owned by: extract_company_mentions (fast-filter projections from ExtractionResultDoc)
    content_class: str | None = None  # review | rating | event | question | noise
    content_class_confidence: float | None = None
    has_company_mention: bool | None = None
    enrichment_version: int | None = None


class ExtractionResultDoc(SQLModel):
    """Layer 3 — persisted LLM extraction output for a single post.

    Stored in the ArangoDB extraction_results collection.
    _key = f"{post_key}_{enrichment_version}"
    Retained across version bumps for comparison; replaced within the same version.
    Use model_dump(by_alias=True) for python-arango bulk operations.
    """

    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")  # f"{post_key}_{enrichment_version}"
    post_key: str  # references posts/{post_key}
    partition_key: str
    enrichment_version: int

    # Classification
    content_class: str
    content_class_confidence: float

    # Company mentions — full LLM output stored verbatim
    company_mentions: list[dict]  # list[CompanyMentionResult.model_dump()]

    # Alias definitions the LLM detected in this post
    alias_definitions: list[dict]  # list[AliasDefinitionResult.model_dump()]

    # Implicit reply decisions — written by detect_implicit_replies after extract_company_mentions
    implicit_replies: list[dict]  # list[ImplicitReply.model_dump()]

    # Full PydanticAI message history for audit (system prompt + user input + assistant output)
    messages_json: list[dict] | None  # allows re-projection without re-calling LLM

    # Provenance
    model_used: str  # e.g. "gpt-4o-mini"
    extracted_at: str  # ISO datetime


class ArangoEdge(SQLModel):
    """Edge document in the ArangoDB quotes collection.

    Represents both explicit (html_metadata) and implicit (implicit_llm) reply relationships.
    _from / _to are ArangoDB vertex references (e.g. "posts/12345").
    Use model_dump(by_alias=True) for python-arango bulk operations.
    """

    model_config = ConfigDict(populate_by_name=True)

    from_vertex: str = Field(alias="_from")  # "posts/{post_id}"
    to_vertex: str = Field(alias="_to")  # "posts/{post_id}"
    key: str = Field(alias="_key")
    quote_ordinal: int
    confidence: float  # 1.0 for html_metadata; LLM score for implicit_llm
    method: str  # "html_metadata" | "implicit_llm"
    partition_key: str


class EmbedItem(SQLModel):
    """Post stub for embedding — just enough to call the embed API.

    text field is populated from normalized_own_text (not content_text).
    """

    key: str
    text: str


class EmbedPatch(SQLModel):
    """Patch payload written back to ArangoDB after embedding is computed."""

    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")
    embedding: list[float]
    embedding_model: str
