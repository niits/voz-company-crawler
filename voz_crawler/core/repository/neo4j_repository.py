"""Neo4j implementation of GraphRepository.

Uses Cypher queries via the official neo4j Python driver.
All nodes are labeled :Post; all edges are typed :QUOTES.

The GenAI plugin (NEO4J_PLUGINS=["genai"] in docker-compose) enables the
VECTOR INDEX created in ensure_schema() for cosine similarity search on embeddings.
"""

import logging

from voz_crawler.core.entities.graph import EmbedItem, EmbedPatch, GraphEdge, GraphPost

log = logging.getLogger(__name__)


class Neo4jGraphRepository:
    """Graph store backed by Neo4j (official neo4j Python driver).

    Constructed by GraphStoreResource — not instantiated directly.
    ``driver`` is a neo4j.Driver instance; ``database`` is the target DB name.
    """

    def __init__(self, driver, database: str) -> None:
        self._driver = driver
        self._database = database

    def _session(self):
        return self._driver.session(database=self._database)

    # ── Schema setup ────────────────────────────────────────────────────────

    def ensure_schema(self) -> None:
        """Idempotently create constraints and indexes.

        Three statements:
        1. Uniqueness constraint on Post.post_id (also creates backing index)
        2. Index on Post.partition_key for partition-scoped queries
        3. Vector index on Post.embedding (requires Neo4j GenAI plugin + 5.11+)
           Wrapped in try/except — community edition may not support it.
        """
        with self._session() as session:
            session.run(
                "CREATE CONSTRAINT post_id_unique IF NOT EXISTS "
                "FOR (p:Post) REQUIRE p.post_id IS UNIQUE"
            )
            session.run(
                "CREATE INDEX post_partition_key IF NOT EXISTS FOR (p:Post) ON (p.partition_key)"
            )
            try:
                session.run(
                    "CREATE VECTOR INDEX post_embeddings IF NOT EXISTS "
                    "FOR (p:Post) ON (p.embedding) "
                    "OPTIONS {indexConfig: {"
                    "  `vector.dimensions`: 1536,"
                    "  `vector.similarity_function`: 'cosine'"
                    "}}"
                )
            except Exception as exc:
                log.warning(
                    "Vector index creation skipped (community edition or plugin missing): %s", exc
                )

    # ── Posts ────────────────────────────────────────────────────────────────

    def get_existing_hashes(self, partition_key: str) -> dict[str, str]:
        """Return {str(post_id): content_hash} for all posts in this partition."""
        with self._session() as session:
            result = session.run(
                "MATCH (p:Post {partition_key: $pk}) "
                "RETURN p.post_id AS post_id, p.content_hash AS content_hash",
                pk=partition_key,
            )
            return {str(record["post_id"]): record["content_hash"] for record in result}

    def upsert_posts(self, docs: list[GraphPost]) -> None:
        """Bulk-upsert Post nodes. MERGE on post_id, SET replaces all properties."""
        if not docs:
            return
        rows = [
            {
                "post_id": d.post_id,
                "author_username": d.author_username,
                "author_id": d.author_id,
                "posted_at": d.posted_at,
                "content_text": d.content_text,
                "content_hash": d.content_hash,
                "partition_key": d.partition_key,
                "thread_url": d.thread_url,
                "page_number": d.page_number,
                "embedding": d.embedding,
            }
            for d in docs
        ]
        with self._session() as session:
            session.run(
                "UNWIND $rows AS row "
                "MERGE (p:Post {post_id: row.post_id}) "
                "SET p.author_username = row.author_username,"
                "    p.author_id = row.author_id,"
                "    p.posted_at = row.posted_at,"
                "    p.content_text = row.content_text,"
                "    p.content_hash = row.content_hash,"
                "    p.partition_key = row.partition_key,"
                "    p.thread_url = row.thread_url,"
                "    p.page_number = row.page_number,"
                "    p.embedding = row.embedding",
                rows=rows,
            )

    def fetch_posts_needing_embedding(self, partition_key: str) -> list[EmbedItem]:
        """Return posts in this partition with no embedding and content length >= 20."""
        with self._session() as session:
            result = session.run(
                "MATCH (p:Post {partition_key: $pk}) "
                "WHERE p.embedding IS NULL "
                "  AND p.content_text IS NOT NULL "
                "  AND size(p.content_text) >= 20 "
                "RETURN p.post_id AS post_id, p.content_text AS text",
                pk=partition_key,
            )
            return [EmbedItem(post_id=record["post_id"], text=record["text"]) for record in result]

    def update_post_embeddings(self, patches: list[EmbedPatch]) -> None:
        """Patch embedding and embedding_model on existing Post nodes."""
        if not patches:
            return
        rows = [
            {
                "post_id": p.post_id,
                "embedding": p.embedding,
                "embedding_model": p.embedding_model,
            }
            for p in patches
        ]
        with self._session() as session:
            session.run(
                "UNWIND $patches AS patch "
                "MATCH (p:Post {post_id: patch.post_id}) "
                "SET p.embedding = patch.embedding, "
                "    p.embedding_model = patch.embedding_model",
                patches=rows,
            )

    # ── Edges ────────────────────────────────────────────────────────────────

    def drop_partition_edges(self, partition_key: str) -> int:
        """Delete all QUOTES relationships for a partition. Returns count deleted."""
        with self._session() as session:
            result = session.run(
                "MATCH ()-[r:QUOTES {partition_key: $pk}]->() "
                "WITH count(r) AS total "
                "MATCH ()-[r:QUOTES {partition_key: $pk}]->() "
                "DELETE r "
                "RETURN total",
                pk=partition_key,
            )
            record = result.single()
            return record["total"] if record else 0

    def insert_edges(self, edges: list[GraphEdge]) -> None:
        """Bulk-insert QUOTES relationships. MERGE on edge_key deduplicates."""
        if not edges:
            return
        rows = [
            {
                "from_post_id": e.from_post_id,
                "to_post_id": e.to_post_id,
                "edge_key": e.edge_key,
                "quote_ordinal": e.quote_ordinal,
                "confidence": e.confidence,
                "method": e.method,
                "partition_key": e.partition_key,
            }
            for e in edges
        ]
        with self._session() as session:
            session.run(
                "UNWIND $rows AS e "
                "MATCH (src:Post {post_id: e.from_post_id}) "
                "MATCH (tgt:Post {post_id: e.to_post_id}) "
                "MERGE (src)-[r:QUOTES {edge_key: e.edge_key}]->(tgt) "
                "ON CREATE SET "
                "  r.quote_ordinal = e.quote_ordinal,"
                "  r.confidence = e.confidence,"
                "  r.method = e.method,"
                "  r.partition_key = e.partition_key",
                rows=rows,
            )
