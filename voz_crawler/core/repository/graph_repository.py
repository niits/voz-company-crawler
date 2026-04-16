from voz_crawler.core.entities.arango import (
    ArangoEdge,
    EmbedItem,
    EmbedPatch,
    ExtractionResultDoc,
    NormalizedPostDoc,
    RawPostDoc,
)
from voz_crawler.core.entities.company import (
    AliasEvidenceDoc,
    CompanyMentionDoc,
    MentionEdge,
)


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
        """Idempotently create all collections, indexes, named graph, and ArangoSearch view.

        Safe to call on every asset run — all operations are no-ops if already present.
        """
        # ── Vertex collections ───────────────────────────────────────────────
        for name in (
            "posts",
            "extraction_results",
            "company_mentions",
            "companies",
            "company_aliases",
            "alias_evidence",
        ):
            if not self._db.has_collection(name):
                self._db.create_collection(name)

        # ── Edge collections ─────────────────────────────────────────────────
        for name in ("quotes", "mentions"):
            if not self._db.has_collection(name):
                self._db.create_collection(name, edge=True)

        # ── Named graph ──────────────────────────────────────────────────────
        if not self._db.has_graph("reply_graph"):
            self._db.create_graph(
                "reply_graph",
                edge_definitions=[
                    {
                        "edge_collection": "quotes",
                        "from_vertex_collections": ["posts"],
                        "to_vertex_collections": ["posts"],
                    },
                    {
                        "edge_collection": "mentions",
                        "from_vertex_collections": ["posts"],
                        "to_vertex_collections": ["company_mentions"],
                    },
                ],
            )
        else:
            # Ensure mentions edge definition exists in the graph
            graph = self._db.graph("reply_graph")
            existing_edges = {e["edge_collection"] for e in graph.edge_definitions()}
            if "mentions" not in existing_edges:
                graph.create_edge_definition(
                    edge_collection="mentions",
                    from_vertex_collections=["posts"],
                    to_vertex_collections=["company_mentions"],
                )

        # ── Indexes ──────────────────────────────────────────────────────────
        self._db.collection("posts").add_persistent_index(fields=["partition_key"], unique=False)
        self._db.collection("quotes").add_persistent_index(fields=["partition_key"], unique=False)
        self._db.collection("extraction_results").add_persistent_index(
            fields=["post_key", "enrichment_version"], unique=False
        )
        self._db.collection("extraction_results").add_persistent_index(
            fields=["partition_key"], unique=False
        )
        self._db.collection("company_mentions").add_persistent_index(
            fields=["partition_key"], unique=False
        )
        self._db.collection("company_mentions").add_persistent_index(
            fields=["company_key"], unique=False
        )
        self._db.collection("company_mentions").add_persistent_index(
            fields=["resolution_version"], unique=False
        )
        self._db.collection("mentions").add_persistent_index(fields=["partition_key"], unique=False)
        self._db.collection("companies").add_persistent_index(fields=["is_stub"], unique=False)
        self._db.collection("company_aliases").add_persistent_index(
            fields=["alias_normalized"], unique=False
        )
        self._db.collection("company_aliases").add_persistent_index(
            fields=["resolution_version"], unique=False
        )
        self._db.collection("alias_evidence").add_persistent_index(
            fields=["alias_slug"], unique=False
        )
        self._db.collection("alias_evidence").add_persistent_index(
            fields=["source_post_key"], unique=False
        )

        # ── ArangoSearch view ────────────────────────────────────────────────
        # Determine best available text analyzer
        available = {a["name"].split("::")[-1] for a in self._db.analyzers()}
        text_analyzer = "text_vi" if "text_vi" in available else "text_en"

        view_props = {
            "links": {
                "posts": {
                    "analyzers": [text_analyzer, "identity"],
                    "fields": {
                        "normalized_own_text": {"analyzers": [text_analyzer]},
                        "partition_key": {"analyzers": ["identity"]},
                    },
                    "includeAllFields": False,
                }
            }
        }
        existing_views = {v["name"] for v in self._db.views()}
        if "reply_graph_search" not in existing_views:
            self._db.create_arangosearch_view("reply_graph_search", properties=view_props)
        else:
            self._db.update_arangosearch_view("reply_graph_search", properties=view_props)

    # ── Posts — Layer 1 ─────────────────────────────────────────────────────

    def get_existing_hashes(self, partition_key: str) -> dict[str, str]:
        """Return {_key: content_hash} for all posts in this partition."""
        cursor = self._db.aql.execute(
            "FOR p IN posts FILTER p.partition_key == @pk RETURN {k: p._key, h: p.content_hash}",
            bind_vars={"pk": partition_key},
        )
        return {doc["k"]: doc["h"] for doc in cursor}

    def upsert_posts(self, docs: list[RawPostDoc]) -> None:
        """Bulk-upsert Layer 1 post documents. Replace semantics reset all Layer 2 fields."""
        if docs:
            self._db.collection("posts").import_bulk(
                [d.model_dump(by_alias=True) for d in docs],
                on_duplicate="replace",
            )

    # ── Posts — Layer 2 normalization ────────────────────────────────────────

    def fetch_posts_needing_preprocess(
        self, partition_key: str, normalization_version: int, embedding_model: str
    ) -> list[str]:
        """Return _keys of posts needing normalization or re-embedding.

        A post needs preprocess if:
        - normalization_version is null or stale, OR
        - normalization is current but embedding is missing or from a different model.
        """
        cursor = self._db.aql.execute(
            "FOR p IN posts"
            " FILTER p.partition_key == @pk"
            "   AND ("
            "     p.normalization_version == null"
            "     OR p.normalization_version < @norm_ver"
            "     OR ("
            "       p.normalized_own_text != null"
            "       AND LENGTH(p.normalized_own_text) >= 20"
            "       AND (p.embedding == null OR p.embedding_model != @embed_model)"
            "     )"
            "   )"
            " RETURN p._key",
            bind_vars={
                "pk": partition_key,
                "norm_ver": normalization_version,
                "embed_model": embedding_model,
            },
        )
        return list(cursor)

    def upsert_normalized_embedded(self, patches: list[NormalizedPostDoc]) -> None:
        """Single bulk write for normalization + embedding fields together."""
        if patches:
            self._db.collection("posts").update_many(
                [p.model_dump(by_alias=True, exclude_none=True) for p in patches]
            )

    def write_llm_results(
        self,
        partition_key: str,
        extraction_docs: list[ExtractionResultDoc],
        enrichment_patches: list[NormalizedPostDoc],
        mention_docs,
        mention_edges,
        alias_evidence,
    ) -> int:
        """Single write boundary for all LLM-derived data.

        Writes in dependency order: extraction_results → post patches →
        drop old mention edges → company_mentions → mention_edges → alias_evidence.
        Returns count of extraction docs written.
        """
        if extraction_docs:
            self._db.collection("extraction_results").import_bulk(
                [d.model_dump(by_alias=True) for d in extraction_docs],
                on_duplicate="replace",
            )
        if enrichment_patches:
            self._db.collection("posts").update_many(
                [p.model_dump(by_alias=True, exclude_none=True) for p in enrichment_patches]
            )
        self._db.aql.execute(
            "FOR e IN mentions FILTER e.partition_key == @pk REMOVE e IN mentions",
            bind_vars={"pk": partition_key},
        )
        if mention_docs:
            self._db.collection("company_mentions").import_bulk(
                [d.model_dump(by_alias=True) for d in mention_docs],
                on_duplicate="replace",
            )
        if mention_edges:
            self._db.collection("mentions").import_bulk(
                [e.model_dump(by_alias=True) for e in mention_edges],
                on_duplicate="ignore",
            )
        if alias_evidence:
            self._db.collection("alias_evidence").import_bulk(
                [d.model_dump(by_alias=True) for d in alias_evidence],
                on_duplicate="replace",
            )
        return len(extraction_docs)

    def fetch_posts_needing_normalization(self, partition_key: str) -> list[dict]:
        """Return {key, html} for posts missing normalized_own_text in this partition."""
        cursor = self._db.aql.execute(
            "FOR p IN posts"
            " FILTER p.partition_key == @pk"
            "   AND p.normalized_own_text == null"
            " RETURN {key: p._key}",
            bind_vars={"pk": partition_key},
        )
        return list(cursor)

    def update_post_normalizations(self, patches: list[NormalizedPostDoc]) -> None:
        """Patch normalization fields on existing post documents."""
        if patches:
            self._db.collection("posts").update_many(
                [p.model_dump(by_alias=True, exclude_none=True) for p in patches]
            )

    # ── Posts — Layer 2 embeddings ───────────────────────────────────────────

    def fetch_posts_needing_embedding(self, partition_key: str) -> list[EmbedItem]:
        """Return EmbedItems for posts with normalized_own_text but missing embeddings."""
        cursor = self._db.aql.execute(
            "FOR p IN posts"
            " FILTER p.partition_key == @pk"
            "   AND p.embedding == null"
            "   AND p.normalized_own_text != null"
            "   AND LENGTH(p.normalized_own_text) >= 20"
            " RETURN {key: p._key, text: p.normalized_own_text}",
            bind_vars={"pk": partition_key},
        )
        return [EmbedItem.model_validate(doc) for doc in cursor]

    def update_post_embeddings(self, patches: list[EmbedPatch]) -> None:
        """Patch embedding fields on existing post documents."""
        if patches:
            self._db.collection("posts").update_many([p.model_dump(by_alias=True) for p in patches])

    # ── Posts — Layer 2 enrichment projection ────────────────────────────────

    def fetch_posts_needing_enrichment(
        self, partition_key: str, enrichment_version: int
    ) -> list[EmbedItem]:
        """Return {key, text} for posts needing LLM enrichment (null or stale version)."""
        cursor = self._db.aql.execute(
            "FOR p IN posts"
            " FILTER p.partition_key == @pk"
            "   AND p.normalized_own_text != null"
            "   AND LENGTH(p.normalized_own_text) >= 20"
            "   AND (p.enrichment_version == null OR p.enrichment_version < @ver)"
            " RETURN {key: p._key, text: p.normalized_own_text}",
            bind_vars={"pk": partition_key, "ver": enrichment_version},
        )
        return [EmbedItem.model_validate(doc) for doc in cursor]

    def patch_post_enrichment_fields(self, patches: list[NormalizedPostDoc]) -> None:
        """Patch content_class / has_company_mention / enrichment_version on post documents."""
        if patches:
            self._db.collection("posts").update_many(
                [p.model_dump(by_alias=True, exclude_none=True) for p in patches]
            )

    # ── Extraction results ────────────────────────────────────────────────────

    def upsert_extraction_results(self, docs: list[ExtractionResultDoc]) -> None:
        """Upsert LLM extraction results. Replace within same (post, version) pair."""
        if docs:
            self._db.collection("extraction_results").import_bulk(
                [d.model_dump(by_alias=True) for d in docs],
                on_duplicate="replace",
            )

    def fetch_extraction_results(self, partition_key: str, enrichment_version: int) -> list[dict]:
        """Return raw extraction result docs for a partition at the current version."""
        cursor = self._db.aql.execute(
            "FOR e IN extraction_results"
            " FILTER e.partition_key == @pk AND e.enrichment_version == @ver"
            " RETURN e",
            bind_vars={"pk": partition_key, "ver": enrichment_version},
        )
        return list(cursor)

    def patch_extraction_implicit_replies(
        self, post_key: str, enrichment_version: int, implicit_replies: list[dict]
    ) -> None:
        """Patch implicit_replies field on an ExtractionResultDoc after detection."""
        doc_key = f"{post_key}_{enrichment_version}"
        self._db.collection("extraction_results").update(
            {"_key": doc_key, "implicit_replies": implicit_replies}
        )

    # ── Company mentions ──────────────────────────────────────────────────────

    def upsert_company_mentions(self, docs: list[CompanyMentionDoc]) -> None:
        """Upsert CompanyMentionDoc vertices. Replace on re-enrichment."""
        if docs:
            self._db.collection("company_mentions").import_bulk(
                [d.model_dump(by_alias=True) for d in docs],
                on_duplicate="replace",
            )

    def drop_mention_edges(self, partition_key: str) -> int:
        """Delete all mention edges for a partition. Returns count dropped."""
        result = self._db.aql.execute(
            "FOR e IN mentions FILTER e.partition_key == @pk REMOVE e IN mentions RETURN 1",
            bind_vars={"pk": partition_key},
        )
        return sum(1 for _ in result)

    def insert_mention_edges(self, edges: list[MentionEdge]) -> None:
        """Bulk-insert mention edges (posts → company_mentions)."""
        if edges:
            self._db.collection("mentions").import_bulk(
                [e.model_dump(by_alias=True) for e in edges],
                on_duplicate="ignore",
            )

    # ── Alias evidence ────────────────────────────────────────────────────────

    def upsert_alias_evidence(self, docs: list[AliasEvidenceDoc]) -> None:
        """Upsert alias evidence records. Replace on re-enrichment of same post."""
        if docs:
            self._db.collection("alias_evidence").import_bulk(
                [d.model_dump(by_alias=True) for d in docs],
                on_duplicate="replace",
            )

    # ── Quotes / explicit edges ───────────────────────────────────────────────

    def drop_partition_edges(self, partition_key: str) -> int:
        """Delete all html_metadata quote edges for a partition. Returns count dropped."""
        result = self._db.aql.execute(
            "FOR e IN quotes"
            " FILTER e.partition_key == @pk AND e.method == 'html_metadata'"
            " REMOVE e IN quotes RETURN 1",
            bind_vars={"pk": partition_key},
        )
        return sum(1 for _ in result)

    def insert_edges(self, edges: list[ArangoEdge]) -> None:
        """Bulk-insert quote edges. Ignores duplicates."""
        if edges:
            self._db.collection("quotes").import_bulk(
                [e.model_dump(by_alias=True) for e in edges],
                on_duplicate="ignore",
            )

    def insert_implicit_edges(self, edges: list[ArangoEdge]) -> None:
        """Bulk-insert implicit_llm reply edges. Ignores duplicates (edges accumulate)."""
        if edges:
            self._db.collection("quotes").import_bulk(
                [e.model_dump(by_alias=True) for e in edges],
                on_duplicate="ignore",
            )

    # ── Implicit reply candidate retrieval ───────────────────────────────────

    def fetch_implicit_reply_candidates(
        self,
        partition_key: str,
        source_key: str,
        source_text: str,
        window_keys: list[str],
        top_n: int = 5,
        min_emb_sim: float = 0.65,
    ) -> list[dict]:
        """Combined AQL query over reply_graph_search: BM25 + window boost.

        Excludes posts already connected to source_key in reply_graph (any direction).
        Returns top_n candidates sorted by combined score.
        Note: cosine similarity scoring is done in Python after fetching candidates
        (ArangoDB COSINE_SIMILARITY requires Enterprise edition).
        """
        cursor = self._db.aql.execute(
            """
            LET already_linked = (
              FOR v IN 1..1 ANY CONCAT("posts/", @source_key)
                GRAPH "reply_graph"
                RETURN v._key
            )
            FOR p IN reply_graph_search
              SEARCH ANALYZER(
                p.normalized_own_text IN TOKENS(@source_text, @analyzer),
                @analyzer
              )
                AND p.partition_key == @pk
                AND p._key != @source_key
                AND p._key NOT IN already_linked
              LET bm25 = BM25(p)
              LET window_boost = p._key IN @window_keys ? 0.2 : 0.0
              LET score = 0.3 * MIN([bm25 / 5.0, 1.0]) + window_boost
              FILTER bm25 > 1.0 OR p._key IN @window_keys
              SORT score DESC
              LIMIT @top_n
              RETURN {
                key: p._key,
                text: p.normalized_own_text,
                embedding: p.embedding,
                posted_at: p.posted_at,
                bm25: bm25,
                score: score
              }
            """,
            bind_vars={
                "pk": partition_key,
                "source_key": source_key,
                "source_text": source_text,
                "window_keys": window_keys,
                "top_n": top_n,
                "analyzer": self._text_analyzer(),
            },
        )
        return list(cursor)

    def fetch_partition_posts_for_implicit_reply(self, partition_key: str) -> list[dict]:
        """Return all posts for a partition sorted by posted_at, for window computation."""
        cursor = self._db.aql.execute(
            "FOR p IN posts"
            " FILTER p.partition_key == @pk"
            "   AND p.normalized_own_text != null"
            "   AND p.embedding != null"
            " SORT p.posted_at ASC"
            " RETURN {key: p._key, text: p.normalized_own_text,"
            "         posted_at: p.posted_at, embedding: p.embedding,"
            "         partition_key: p.partition_key}",
            bind_vars={"pk": partition_key},
        )
        return list(cursor)

    def _text_analyzer(self) -> str:
        available = {a["name"].split("::")[-1] for a in self._db.analyzers()}
        return "text_vi" if "text_vi" in available else "text_en"
