"""GraphRepository Protocol — the public contract for all graph store backends.

Both ArangoGraphRepository and Neo4jGraphRepository implement this protocol.
Callers (Dagster assets, embedding_sync, etc.) depend only on this interface.

Using @runtime_checkable allows isinstance() checks in tests to verify that
both backend implementations satisfy the protocol contract.
"""

from typing import Protocol, runtime_checkable

from voz_crawler.core.entities.graph import EmbedItem, EmbedPatch, GraphEdge, GraphPost


@runtime_checkable
class GraphRepository(Protocol):
    """Read/write operations on the graph store, backend-agnostic."""

    def ensure_schema(self) -> None:
        """Idempotently create collections/labels, indexes, and constraints.

        Safe to call on every asset run.
        """
        ...

    def get_existing_hashes(self, partition_key: str) -> dict[str, str]:
        """Return {str(post_id): content_hash} for all posts in this partition."""
        ...

    def upsert_posts(self, docs: list[GraphPost]) -> None:
        """Bulk-upsert post nodes/documents with replace semantics."""
        ...

    def fetch_posts_needing_embedding(self, partition_key: str) -> list[EmbedItem]:
        """Return posts in this partition that have no embedding yet."""
        ...

    def update_post_embeddings(self, patches: list[EmbedPatch]) -> None:
        """Patch embedding and embedding_model on existing post nodes/documents."""
        ...

    def drop_partition_edges(self, partition_key: str) -> int:
        """Delete all edges/relationships for a partition. Returns count deleted."""
        ...

    def insert_edges(self, edges: list[GraphEdge]) -> None:
        """Bulk-insert quote edges/relationships. Duplicate edges are ignored."""
        ...
