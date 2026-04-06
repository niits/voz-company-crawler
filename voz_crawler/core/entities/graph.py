"""Backend-agnostic graph entities.

These dataclasses are the public contract shared between:
- GraphRepository implementations (ArangoGraphRepository, Neo4jGraphRepository)
- Graph pipeline modules (post_sync, quote_parser, embedding_sync, edge_sync)
- Dagster assets

Neither ArangoDB-specific aliases (_key, _from, _to) nor Neo4j-specific
identifiers appear here.  Each backend adapter converts internally.
"""

from dataclasses import dataclass, field


@dataclass
class GraphPost:
    """A forum post node in the reply graph.

    post_id is the canonical cross-backend identity (unique constraint in both DBs).
    embedding is set to None on insert/update so the embedding asset re-processes it.
    """

    post_id: int
    author_username: str | None
    author_id: str | None
    posted_at: str | None
    content_text: str | None
    content_hash: str
    partition_key: str
    thread_url: str
    page_number: int
    embedding: list[float] | None = field(default=None)


@dataclass
class GraphEdge:
    """A directed quote relationship between two posts.

    from_post_id → to_post_id means "from_post_id quoted to_post_id".
    edge_key is a stable logical key used for deduplication: "{from}_{to}_{ordinal}".
    """

    from_post_id: int
    to_post_id: int
    edge_key: str
    quote_ordinal: int
    confidence: float
    method: str
    partition_key: str


@dataclass
class EmbedItem:
    """Minimal post stub returned by fetch_posts_needing_embedding."""

    post_id: int
    text: str


@dataclass
class EmbedPatch:
    """Embedding update payload written back after the OpenAI call."""

    post_id: int
    embedding: list[float]
    embedding_model: str
