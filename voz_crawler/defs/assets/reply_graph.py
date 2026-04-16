from dataclasses import dataclass

from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetOut,
    AutomationCondition,
    IdentityPartitionMapping,
    In,
    MaterializeResult,
    MetadataValue,
    Nothing,
    OpExecutionContext,
    Out,
    Output,
    asset,
    graph_multi_asset,
    op,
)
from dagster_openai import OpenAIResource

from voz_crawler.core.entities.arango import EmbedItem, NormalizedPostDoc
from voz_crawler.core.entities.enrichment import (
    ENRICHMENT_VERSION,
    NORMALIZATION_VERSION,
)
from voz_crawler.core.graph.company_sync import build_company_mention_docs
from voz_crawler.core.graph.edge_sync import build_edges
from voz_crawler.core.graph.embedding_sync import EMBEDDING_MODEL, embed_batch
from voz_crawler.core.graph.enrichment_sync import EnrichmentStats, run_llm_enrichment
from voz_crawler.core.graph.implicit_reply_sync import process_partition_implicit_replies
from voz_crawler.core.graph.normalizer import normalize_post_html
from voz_crawler.core.graph.post_sync import build_upsert_docs
from voz_crawler.core.ingestion.html_source.pagination import build_page_url
from voz_crawler.defs.assets.ingestion import voz_pages_partitions
from voz_crawler.defs.resources.arango_resource import ArangoDBResource
from voz_crawler.defs.resources.crawler_resource import CrawlerResource
from voz_crawler.defs.resources.postgres_resource import PostgresResource


def _page_url(partition_key: str, thread_url: str) -> str:
    _, page_num_str = partition_key.rsplit(":", 1)
    return build_page_url(thread_url, int(page_num_str))


# ── Internal bundle types (in-memory only, never serialized) ─────────────────


@dataclass
class _ClassifyBundle:
    """Data flowing between _classify_op and _build_mentions_op."""

    partition_key: str
    extraction_docs: list
    enrichment_patches: list
    stats: EnrichmentStats


@dataclass
class _LLMBundle:
    """Data flowing into _write_llm_op — everything the LLM group produced."""

    partition_key: str
    extraction_docs: list
    enrichment_patches: list
    mention_docs: list
    mention_edges: list
    alias_evidence: list
    stats: EnrichmentStats


# ═══════════════════════════════════════════════════════════════════════════════
# Branch root — sync_posts_to_arango
# ═══════════════════════════════════════════════════════════════════════════════


@asset(
    partitions_def=voz_pages_partitions,
    group_name="reply_graph",
    compute_kind="ArangoDB",
    ins={
        "dlt_voz_posts": AssetIn(
            key=AssetKey(["dlt_voz_posts"]),
            partition_mapping=IdentityPartitionMapping(),
        )
    },
    automation_condition=AutomationCondition.eager(),
    description="Layer 1 posts in ArangoDB, content-hash diffed from PostgreSQL.",
)
def sync_posts_to_arango(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    arango: ArangoDBResource,
    crawler: CrawlerResource,
    dlt_voz_posts: Nothing,
) -> MaterializeResult:
    """Sync raw.voz__posts → ArangoDB posts collection.

    on_duplicate=replace resets all Layer 2 enrichment fields to null
    whenever content changes, propagating staleness downstream.
    """
    partition_key = context.partition_key
    _, page_num_str = partition_key.rsplit(":", 1)
    page_number = int(page_num_str)
    page_url = _page_url(partition_key, crawler.thread_url)

    context.log.info(f"[sync_posts] partition={partition_key!r} page_url={page_url}")

    rows = postgres.get_repository().fetch_posts(page_url)
    if not rows:
        context.log.warning(f"[sync_posts] no posts for page_url={page_url!r}")
        return MaterializeResult(
            metadata={
                "posts_in_postgres": MetadataValue.int(0),
                "posts_upserted": MetadataValue.int(0),
                "posts_skipped_unchanged": MetadataValue.int(0),
            }
        )

    arango_repo = arango.get_repository()
    existing_hashes = arango_repo.get_existing_hashes(partition_key)
    to_upsert, skipped = build_upsert_docs(
        rows, existing_hashes, partition_key, crawler.thread_url, page_number
    )
    arango_repo.upsert_posts(to_upsert)

    context.log.info(f"[sync_posts] upserted={len(to_upsert)} skipped={skipped}")
    return MaterializeResult(
        metadata={
            "posts_in_postgres": MetadataValue.int(len(rows)),
            "posts_upserted": MetadataValue.int(len(to_upsert)),
            "posts_skipped_unchanged": MetadataValue.int(skipped),
        }
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Branch A — extract_explicit_edges (fast, stateless, no LLM)
# ═══════════════════════════════════════════════════════════════════════════════


@asset(
    partitions_def=voz_pages_partitions,
    group_name="reply_graph",
    compute_kind="ArangoDB",
    ins={
        "sync_posts_to_arango": AssetIn(
            key=AssetKey(["sync_posts_to_arango"]),
            partition_mapping=IdentityPartitionMapping(),
        )
    },
    automation_condition=AutomationCondition.eager(),
    description="XenForo quote edges extracted from raw HTML blockquotes.",
)
def extract_explicit_edges(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    arango: ArangoDBResource,
    crawler: CrawlerResource,
    sync_posts_to_arango: Nothing,
) -> MaterializeResult:
    """Extract XenForo blockquote edges into the quotes edge collection.

    Drops all existing html_metadata edges for the partition and re-inserts fresh ones.
    """
    partition_key = context.partition_key
    page_url = _page_url(partition_key, crawler.thread_url)

    context.log.info(f"[extract_edges] partition={partition_key!r}")

    rows = postgres.get_repository().fetch_posts_with_blockquotes(page_url)
    arango_repo = arango.get_repository()
    dropped = arango_repo.drop_partition_edges(partition_key)
    edges = build_edges(rows, partition_key)
    arango_repo.insert_edges(edges)

    context.log.info(
        f"[extract_edges] dropped={dropped} inserted={len(edges)} from {len(rows)} posts"
    )
    return MaterializeResult(
        metadata={
            "posts_with_quotes": MetadataValue.int(len(rows)),
            "edges_dropped": MetadataValue.int(dropped),
            "edges_inserted": MetadataValue.int(len(edges)),
        }
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Branch B — reply_graph_preprocess_assets (normalize → embed → write once)
# ═══════════════════════════════════════════════════════════════════════════════


@op(ins={"sync_posts_to_arango": In(Nothing)})
def _fetch_preprocess_op(
    context: OpExecutionContext,
    postgres: PostgresResource,
    arango: ArangoDBResource,
    crawler: CrawlerResource,
    sync_posts_to_arango,
) -> Output[list]:
    """Single DB read: staleness check (ArangoDB) + HTML fetch (PostgreSQL).

    Returns filtered RawPost rows for posts that need normalization or re-embedding.
    """
    partition_key = context.partition_key
    page_url = _page_url(partition_key, crawler.thread_url)

    arango_repo = arango.get_repository()
    needing_keys = set(
        arango_repo.fetch_posts_needing_preprocess(
            partition_key, NORMALIZATION_VERSION, EMBEDDING_MODEL
        )
    )
    context.log.info(f"[fetch_preprocess] {len(needing_keys)} posts need preprocess")

    if not needing_keys:
        return Output(value=[], metadata={"posts_to_process": MetadataValue.int(0)})

    rows = postgres.get_repository().fetch_posts_with_html(page_url)
    filtered = [r for r in rows if str(r.post_id_on_site) in needing_keys]
    context.log.info(f"[fetch_preprocess] fetched {len(filtered)} rows from PG")
    return Output(
        value=filtered,
        metadata={"posts_to_process": MetadataValue.int(len(filtered))},
    )


@op(out={"data": Out(), "stats": Out(dict)})
def _normalize_op(
    context: OpExecutionContext,
    rows: list,
) -> dict:
    """Pure: strip quoted blocks, populate normalized_own_text. No DB access."""
    partition_key = context.partition_key

    if not rows:
        return {
            "data": [],
            "stats": {"partition_key": partition_key, "normalized": 0},
        }

    patches: list[NormalizedPostDoc] = []
    for row in rows:
        normalized = normalize_post_html(row.raw_content_html or "")
        patches.append(
            NormalizedPostDoc(
                key=str(row.post_id_on_site),
                normalized_own_text=normalized["own_text"] or None,
                normalized_quoted_blocks=normalized["quoted_blocks"] or None,
                normalization_version=NORMALIZATION_VERSION,
            )
        )

    context.log.info(f"[normalize] produced {len(patches)} patches")
    return {
        "data": patches,
        "stats": {
            "partition_key": partition_key,
            "normalized": len(patches),
            "normalization_version": NORMALIZATION_VERSION,
        },
    }


@op(out={"data": Out(), "stats": Out(dict)})
def _embed_op(
    context: OpExecutionContext,
    patches: list,
    openai: OpenAIResource,
) -> dict:
    """Pure: compute OpenAI embeddings for normalized_own_text. No DB access."""
    partition_key = context.partition_key

    if not patches:
        return {
            "data": [],
            "stats": {"partition_key": partition_key, "embedded": 0},
        }

    to_embed = [
        EmbedItem(key=p.key, text=p.normalized_own_text)
        for p in patches
        if p.normalized_own_text and len(p.normalized_own_text) >= 20
    ]
    context.log.info(f"[embed] {len(to_embed)} of {len(patches)} posts eligible for embedding")

    embed_lookup: dict[str, list[float]] = {}
    if to_embed:
        with openai.get_client(context) as client:
            embed_patches = embed_batch(client, to_embed)
        embed_lookup = {ep.key: ep.embedding for ep in embed_patches}

    result: list[NormalizedPostDoc] = []
    for p in patches:
        if p.key in embed_lookup:
            result.append(
                NormalizedPostDoc(
                    key=p.key,
                    normalized_own_text=p.normalized_own_text,
                    normalized_quoted_blocks=p.normalized_quoted_blocks,
                    normalization_version=p.normalization_version,
                    embedding=embed_lookup[p.key],
                    embedding_model=EMBEDDING_MODEL,
                )
            )
        else:
            result.append(p)

    context.log.info(f"[embed] embedded={len(embed_lookup)}")
    return {
        "data": result,
        "stats": {
            "partition_key": partition_key,
            "embedded": len(embed_lookup),
            "model": EMBEDDING_MODEL,
        },
    }


@op
def _write_preprocess_op(
    context: OpExecutionContext,
    patches: list,
    arango: ArangoDBResource,
) -> Output[dict]:
    """Single bulk write: normalization + embedding fields in one update_many."""
    partition_key = context.partition_key

    if not patches:
        return Output(
            value={"partition_key": partition_key, "written": 0},
            metadata={"posts_written": MetadataValue.int(0)},
        )

    arango.get_repository().upsert_normalized_embedded(patches)
    context.log.info(f"[write_preprocess] wrote {len(patches)} posts")
    return Output(
        value={"partition_key": partition_key, "written": len(patches)},
        metadata={
            "posts_written": MetadataValue.int(len(patches)),
            "normalization_version": MetadataValue.int(NORMALIZATION_VERSION),
            "embedding_model": MetadataValue.text(EMBEDDING_MODEL),
        },
    )


@graph_multi_asset(
    outs={
        "normalize_posts": AssetOut(
            group_name="reply_graph",
            description="Posts with quoted blocks stripped; normalized_own_text populated.",
        ),
        "compute_embeddings": AssetOut(
            group_name="reply_graph",
            description="OpenAI text-embedding-3-small embeddings for normalized_own_text.",
        ),
    },
    ins={
        "sync_posts_to_arango": AssetIn(
            key=AssetKey(["sync_posts_to_arango"]),
            partition_mapping=IdentityPartitionMapping(),
        )
    },
    partitions_def=voz_pages_partitions,
)
def reply_graph_preprocess_assets(sync_posts_to_arango: Nothing):
    """In-memory chain: fetch → normalize → embed → write once.

    Single DB read at _fetch_preprocess_op (staleness check + PG fetch).
    Single DB write at _write_preprocess_op (one update_many for all fields).
    All intermediate ops are pure transforms with no DB access.
    """
    rows = _fetch_preprocess_op(sync_posts_to_arango=sync_posts_to_arango)
    norm = _normalize_op(rows)
    emb = _embed_op(norm.data)
    written = _write_preprocess_op(emb.data)
    return {
        "normalize_posts": norm.stats,
        "compute_embeddings": written,
    }


reply_graph_preprocess_assets = reply_graph_preprocess_assets.with_attributes(
    automation_condition=AutomationCondition.eager()
)


# ═══════════════════════════════════════════════════════════════════════════════
# Branch B continued — reply_graph_llm_assets (fetch → classify → mentions → write once)
# ═══════════════════════════════════════════════════════════════════════════════


@op(ins={"compute_embeddings": In(Nothing)})
def _fetch_llm_op(
    context: OpExecutionContext,
    arango: ArangoDBResource,
    compute_embeddings,
) -> Output[list]:
    """Single DB read: fetch posts needing LLM enrichment (staleness check)."""
    partition_key = context.partition_key

    items = arango.get_repository().fetch_posts_needing_enrichment(
        partition_key, ENRICHMENT_VERSION
    )
    context.log.info(f"[fetch_llm] {len(items)} posts need enrichment")
    return Output(
        value=items,
        metadata={"posts_to_enrich": MetadataValue.int(len(items))},
    )


@op(
    out={"data": Out(), "stats": Out(dict)},
    tags={"dagster/concurrency_key": "voz_llm"},
)
def _classify_op(
    context: OpExecutionContext,
    items: list,
    openai: OpenAIResource,
) -> dict:
    """Pure LLM: classify posts + extract company mentions via PydanticAI. No DB access."""
    partition_key = context.partition_key

    if not items:
        return {
            "data": _ClassifyBundle(
                partition_key=partition_key,
                extraction_docs=[],
                enrichment_patches=[],
                stats=EnrichmentStats(),
            ),
            "stats": {"partition_key": partition_key, "enriched": 0, "errors": 0},
        }

    extraction_docs, enrichment_patches, stats = run_llm_enrichment(
        items, openai.api_key, partition_key
    )

    context.log.info(f"[classify] enriched={stats.enriched} errors={stats.errors}")
    if stats.error_keys:
        context.log.warning(f"[classify] failed keys: {stats.error_keys[:10]}")

    return {
        "data": _ClassifyBundle(
            partition_key=partition_key,
            extraction_docs=extraction_docs,
            enrichment_patches=enrichment_patches,
            stats=stats,
        ),
        "stats": {
            "partition_key": partition_key,
            "enriched": stats.enriched,
            "errors": stats.errors,
            "llm_request_tokens": stats.request_tokens,
            "llm_response_tokens": stats.response_tokens,
            "enrichment_version": ENRICHMENT_VERSION,
        },
    }


@op(out={"data": Out(), "stats": Out(dict)})
def _build_mentions_op(
    context: OpExecutionContext,
    bundle: _ClassifyBundle,
) -> dict:
    """Pure: project company mentions from extraction results. No DB access."""
    partition_key = context.partition_key

    if not bundle.extraction_docs:
        return {
            "data": _LLMBundle(
                partition_key=partition_key,
                extraction_docs=[],
                enrichment_patches=[],
                mention_docs=[],
                mention_edges=[],
                alias_evidence=[],
                stats=bundle.stats,
            ),
            "stats": {
                "partition_key": partition_key,
                "mention_docs": 0,
                "alias_evidence": 0,
            },
        }

    mention_docs, mention_edges, alias_evidence = build_company_mention_docs(
        bundle.extraction_docs, partition_key
    )

    context.log.info(
        f"[build_mentions] docs={len(mention_docs)} alias_evidence={len(alias_evidence)}"
    )
    return {
        "data": _LLMBundle(
            partition_key=partition_key,
            extraction_docs=bundle.extraction_docs,
            enrichment_patches=bundle.enrichment_patches,
            mention_docs=mention_docs,
            mention_edges=mention_edges,
            alias_evidence=alias_evidence,
            stats=bundle.stats,
        ),
        "stats": {
            "partition_key": partition_key,
            "mention_docs": len(mention_docs),
            "alias_evidence": len(alias_evidence),
        },
    }


@op
def _write_llm_op(
    context: OpExecutionContext,
    bundle: _LLMBundle,
    arango: ArangoDBResource,
) -> Output[dict]:
    """Single write boundary: all LLM-derived data in one repo call."""
    partition_key = context.partition_key

    if not bundle.extraction_docs:
        return Output(
            value={"partition_key": partition_key, "written": 0},
            metadata={"extraction_docs_written": MetadataValue.int(0)},
        )

    written = arango.get_repository().write_llm_results(
        partition_key=partition_key,
        extraction_docs=bundle.extraction_docs,
        enrichment_patches=bundle.enrichment_patches,
        mention_docs=bundle.mention_docs,
        mention_edges=bundle.mention_edges,
        alias_evidence=bundle.alias_evidence,
    )

    context.log.info(
        f"[write_llm] extraction_docs={written}"
        f" mentions={len(bundle.mention_docs)}"
        f" alias_evidence={len(bundle.alias_evidence)}"
    )
    return Output(
        value={"partition_key": partition_key, "written": written},
        metadata={
            "extraction_docs_written": MetadataValue.int(written),
            "mention_docs_upserted": MetadataValue.int(len(bundle.mention_docs)),
            "mention_edges_inserted": MetadataValue.int(len(bundle.mention_edges)),
            "alias_evidence_docs": MetadataValue.int(len(bundle.alias_evidence)),
        },
    )


@graph_multi_asset(
    outs={
        "classify_posts": AssetOut(
            group_name="reply_graph",
            description="LLM-classified posts with content_class and company_mentions.",
        ),
        "extract_company_mentions": AssetOut(
            group_name="reply_graph",
            description="Company mention vertices + edges projected from ExtractionResultDocs.",
        ),
    },
    ins={
        "compute_embeddings": AssetIn(
            key=AssetKey(["compute_embeddings"]),
            partition_mapping=IdentityPartitionMapping(),
        )
    },
    partitions_def=voz_pages_partitions,
)
def reply_graph_llm_assets(compute_embeddings: Nothing):
    """In-memory chain: fetch → classify → build_mentions → write once.

    Depends on compute_embeddings (not sync_posts_to_arango) — the LLM group
    only needs posts with embeddings ready; it has no direct dependency on the sync step.
    Single DB read at _fetch_llm_op (staleness check).
    Single DB write at _write_llm_op (extraction_results + post patches + mentions).
    """
    items = _fetch_llm_op(compute_embeddings=compute_embeddings)
    classified = _classify_op(items)
    mentions = _build_mentions_op(classified.data)
    written = _write_llm_op(mentions.data)
    return {
        "classify_posts": classified.stats,
        "extract_company_mentions": written,
    }


reply_graph_llm_assets = reply_graph_llm_assets.with_attributes(
    automation_condition=AutomationCondition.eager()
)


# ═══════════════════════════════════════════════════════════════════════════════
# detect_implicit_replies — sensor-gated, cross-partition lookback
# ═══════════════════════════════════════════════════════════════════════════════


@asset(
    partitions_def=voz_pages_partitions,
    group_name="reply_graph",
    compute_kind="OpenAI",
    deps=[AssetDep("extract_company_mentions", partition_mapping=IdentityPartitionMapping())],
    # No AutomationCondition.eager() — triggered by implicit_reply_sensor only.
    # implicit_reply_sensor gates execution until all lower-numbered partitions
    # have materialized extract_company_mentions (cross-partition lookback window).
    op_tags={"dagster/concurrency_key": "voz_llm"},
)
def detect_implicit_replies(
    context: AssetExecutionContext,
    arango: ArangoDBResource,
    openai: OpenAIResource,
) -> MaterializeResult:
    """Detect implicit reply edges for posts in this partition.

    Stage 1: AQL query (BM25 + window boost) + cosine re-rank.
    Stage 2: PydanticAI agent decides genuine implicit replies (confidence >= 0.60).
    Inserts ArangoEdge(method='implicit_llm'), patches ExtractionResultDoc.implicit_replies.
    """
    partition_key = context.partition_key
    context.log.info(f"[detect_implicit_replies] partition={partition_key!r}")

    arango_repo = arango.get_repository()

    with openai.get_client(context):
        stats = process_partition_implicit_replies(arango_repo, openai.api_key, partition_key)

    context.log.info(
        f"[detect_implicit_replies] processed={stats.posts_processed}"
        f" edges={stats.edges_inserted} errors={stats.errors}"
    )
    if stats.error_keys:
        context.log.warning(f"[detect_implicit_replies] failed keys: {stats.error_keys[:10]}")

    return MaterializeResult(
        metadata={
            "partition_key": MetadataValue.text(partition_key),
            "posts_processed": MetadataValue.int(stats.posts_processed),
            "implicit_edges_inserted": MetadataValue.int(stats.edges_inserted),
            "errors": MetadataValue.int(stats.errors),
        }
    )
