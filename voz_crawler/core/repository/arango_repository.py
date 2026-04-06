"""ArangoDB implementation of GraphRepository.

Accepts GraphPost / GraphEdge (backend-agnostic) and converts them
internally to ArangoDB document dicts using ArangoPost / ArangoEdge aliases.
Callers never see ArangoDB-specific types.
"""

from voz_crawler.core.entities.arango import ArangoEdge, ArangoPost
from voz_crawler.core.entities.arango import EmbedPatch as _ArangoPatch
from voz_crawler.core.entities.graph import EmbedItem, EmbedPatch, GraphEdge, GraphPost


class ArangoGraphRepository:
    """Graph store backed by ArangoDB (python-arango driver).

    Constructed by GraphStoreResource — not instantiated directly.
    ``db`` is a python-arango Database object.
    """

    def __init__(self, db) -> None:
        self._db = db

    # ── Schema setup ────────────────────────────────────────────────────────

    def ensure_schema(self) -> None:
        """Idempotently create posts vertex, quotes edge collection, reply_graph, and indexes."""
        if not self._db.has_collection("posts"):
            self._db.create_collection("posts")
        if not self._db.has_collection("quotes"):
            self._db.create_collection("quotes", edge=True)
        if not self._db.has_graph("reply_graph"):
            self._db.create_graph(
                "reply_graph",
                edge_definitions=[
                    {
                        "edge_collection": "quotes",
                        "from_vertex_collections": ["posts"],
                        "to_vertex_collections": ["posts"],
                    }
                ],
            )
        posts_col = self._db.collection("posts")
        posts_col.add_persistent_index(fields=["partition_key"], unique=False)
        quotes_col = self._db.collection("quotes")
        quotes_col.add_persistent_index(fields=["partition_key"], unique=False)

    # ── Posts ────────────────────────────────────────────────────────────────

    def get_existing_hashes(self, partition_key: str) -> dict[str, str]:
        """Return {str(post_id): content_hash} for all posts in this partition."""
        cursor = self._db.aql.execute(
            "FOR p IN posts FILTER p.partition_key == @pk RETURN {k: p._key, h: p.content_hash}",
            bind_vars={"pk": partition_key},
        )
        return {doc["k"]: doc["h"] for doc in cursor}

    def upsert_posts(self, docs: list[GraphPost]) -> None:
        """Bulk-upsert post documents. Uses replace semantics to reset embedding on changes."""
        if not docs:
            return
        arango_docs = [
            ArangoPost(
                key=str(d.post_id),
                post_id=d.post_id,
                author_username=d.author_username,
                author_id=d.author_id,
                posted_at=d.posted_at,
                content_text=d.content_text,
                content_hash=d.content_hash,
                partition_key=d.partition_key,
                thread_url=d.thread_url,
                page_number=d.page_number,
                embedding=d.embedding,
            ).model_dump(by_alias=True)
            for d in docs
        ]
        self._db.collection("posts").import_bulk(arango_docs, on_duplicate="replace")

    def fetch_posts_needing_embedding(self, partition_key: str) -> list[EmbedItem]:
        """Return EmbedItems for posts in this partition that are missing embeddings."""
        cursor = self._db.aql.execute(
            "FOR p IN posts"
            " FILTER p.partition_key == @pk"
            "   AND p.embedding == null"
            "   AND p.content_text != null"
            "   AND LENGTH(p.content_text) >= 20"
            " RETURN {post_id: TO_NUMBER(p._key), text: p.content_text}",
            bind_vars={"pk": partition_key},
        )
        return [EmbedItem(post_id=doc["post_id"], text=doc["text"]) for doc in cursor]

    def update_post_embeddings(self, patches: list[EmbedPatch]) -> None:
        """Patch embedding fields on existing post documents."""
        if not patches:
            return
        arango_patches = [
            _ArangoPatch(
                key=str(p.post_id),
                embedding=p.embedding,
                embedding_model=p.embedding_model,
            ).model_dump(by_alias=True)
            for p in patches
        ]
        self._db.collection("posts").update_many(arango_patches)

    # ── Edges ────────────────────────────────────────────────────────────────

    def drop_partition_edges(self, partition_key: str) -> int:
        """Delete all quote edges for a partition. Returns the count dropped."""
        result = self._db.aql.execute(
            "FOR e IN quotes FILTER e.partition_key == @pk REMOVE e IN quotes RETURN 1",
            bind_vars={"pk": partition_key},
        )
        return sum(1 for _ in result)

    def insert_edges(self, edges: list[GraphEdge]) -> None:
        """Bulk-insert quote edges. Ignores duplicates (edge relationships are immutable)."""
        if not edges:
            return
        arango_edges = [
            ArangoEdge(
                from_vertex=f"posts/{e.from_post_id}",
                to_vertex=f"posts/{e.to_post_id}",
                key=e.edge_key,
                quote_ordinal=e.quote_ordinal,
                confidence=e.confidence,
                method=e.method,
                partition_key=e.partition_key,
            ).model_dump(by_alias=True)
            for e in edges
        ]
        self._db.collection("quotes").import_bulk(arango_edges, on_duplicate="ignore")
