from voz_crawler.core.entities.arango import ArangoEdge, ArangoPost, EmbedItem, EmbedPatch


class GraphRepository:
    """Graph store interactions for the reply-graph pipeline.

    Abstracts the underlying graph database — swap the implementation by
    replacing the body of each method without touching callers.

    Constructed by ArangoDBResource.get_repository() — not instantiated directly.
    """

    def __init__(self, db) -> None:
        self._db = db

    # ── Schema setup ────────────────────────────────────────────────────────

    def ensure_schema(self) -> None:
        """Idempotently create posts vertex, quotes edge collection, reply_graph, and indexes.

        Safe to call on every asset run — all operations are no-ops if already present.
        """
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
        """Return {_key: content_hash} for all posts in this partition."""
        cursor = self._db.aql.execute(
            "FOR p IN posts FILTER p.partition_key == @pk RETURN {k: p._key, h: p.content_hash}",
            bind_vars={"pk": partition_key},
        )
        return {doc["k"]: doc["h"] for doc in cursor}

    def upsert_posts(self, docs: list[ArangoPost]) -> None:
        """Bulk-upsert post documents. Uses replace semantics to reset embedding on changes."""
        if docs:
            self._db.collection("posts").import_bulk(
                [d.model_dump(by_alias=True) for d in docs],
                on_duplicate="replace",
            )

    def fetch_posts_needing_embedding(self, partition_key: str) -> list[EmbedItem]:
        """Return EmbedItems for posts in this partition that are missing embeddings."""
        cursor = self._db.aql.execute(
            "FOR p IN posts"
            " FILTER p.partition_key == @pk"
            "   AND p.embedding == null"
            "   AND p.content_text != null"
            "   AND LENGTH(p.content_text) >= 20"
            " RETURN {key: p._key, text: p.content_text}",
            bind_vars={"pk": partition_key},
        )
        return [EmbedItem.model_validate(doc) for doc in cursor]

    def update_post_embeddings(self, patches: list[EmbedPatch]) -> None:
        """Patch embedding fields on existing post documents."""
        if patches:
            self._db.collection("posts").update_many([p.model_dump(by_alias=True) for p in patches])

    # ── Edges ────────────────────────────────────────────────────────────────

    def drop_partition_edges(self, partition_key: str) -> int:
        """Delete all quote edges for a partition. Returns the count dropped."""
        result = self._db.aql.execute(
            "FOR e IN quotes FILTER e.partition_key == @pk REMOVE e IN quotes RETURN 1",
            bind_vars={"pk": partition_key},
        )
        return sum(1 for _ in result)

    def insert_edges(self, edges: list[ArangoEdge]) -> None:
        """Bulk-insert quote edges. Ignores duplicates (edge relationships are immutable)."""
        if edges:
            self._db.collection("quotes").import_bulk(
                [e.model_dump(by_alias=True) for e in edges],
                on_duplicate="ignore",
            )
