from pydantic import ConfigDict, Field
from sqlmodel import SQLModel


class ArangoPost(SQLModel):
    """Validated entity for a post document in the ArangoDB posts collection.

    Field names follow ArangoDB conventions via aliases (_key etc.).
    Use model_dump(by_alias=True) when passing to python-arango bulk operations.
    """

    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")
    post_id: int
    author_username: str | None
    author_id: str | None
    posted_at: str | None
    content_text: str | None
    content_hash: str
    partition_key: str
    thread_url: str
    page_number: int
    embedding: list[float] | None


class ArangoEdge(SQLModel):
    """Validated entity for a quote edge document in the ArangoDB quotes collection.

    _from / _to are ArangoDB vertex references (e.g. "posts/12345").
    """

    model_config = ConfigDict(populate_by_name=True)

    from_vertex: str = Field(alias="_from")
    to_vertex: str = Field(alias="_to")
    key: str = Field(alias="_key")
    quote_ordinal: int
    confidence: float
    method: str
    partition_key: str


class EmbedItem(SQLModel):
    """A post stub returned by fetch_posts_needing_embedding — just enough to call the embed API."""

    key: str
    text: str


class EmbedPatch(SQLModel):
    """Patch payload written back to ArangoDB after embedding is computed."""

    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(alias="_key")
    embedding: list[float]
    embedding_model: str
