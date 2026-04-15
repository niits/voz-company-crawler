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
    Output,
    asset,
    graph_multi_asset,
    op,
)
from dagster_openai import OpenAIResource

from voz_crawler.core.entities.arango import NormalizedPostDoc
from voz_crawler.core.entities.enrichment import ENRICHMENT_VERSION, NORMALIZATION_VERSION
from voz_crawler.core.graph.company_sync import build_company_mention_docs
from voz_crawler.core.graph.edge_sync import build_edges
from voz_crawler.core.graph.embedding_sync import (
    EMBED_BATCH_SIZE,
    EMBEDDING_MODEL,
    embed_and_update,
)
from voz_crawler.core.graph.enrichment_sync import enrich_partition
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


# ── Ops (internal computation units) ────────────────────────────────────────


@op(ins={"dlt_voz_posts": In(Nothing)})
def _sync_posts_op(
    context: OpExecutionContext,
    postgres: PostgresResource,
    arango: ArangoDBResource,
    crawler: CrawlerResource,
) -> Output[dict]:
    """Sync raw.voz__posts → ArangoDB posts collection (Layer 1).

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
        return Output(
            value={
                "partition_key": partition_key,
                "page_url": page_url,
                "upserted": 0,
                "skipped": 0,
            },
            metadata={
                "posts_in_postgres": MetadataValue.int(0),
                "posts_upserted": MetadataValue.int(0),
                "posts_skipped_unchanged": MetadataValue.int(0),
            },
        )

    arango_repo = arango.get_repository()
    existing_hashes = arango_repo.get_existing_hashes(partition_key)
    to_upsert, skipped = build_upsert_docs(
        rows, existing_hashes, partition_key, crawler.thread_url, page_number
    )
    arango_repo.upsert_posts(to_upsert)

    context.log.info(f"[sync_posts] upserted={len(to_upsert)} skipped={skipped}")
    return Output(
        value={
            "partition_key": partition_key,
            "page_url": page_url,
            "upserted": len(to_upsert),
            "skipped": skipped,
        },
        metadata={
            "posts_in_postgres": MetadataValue.int(len(rows)),
            "posts_upserted": MetadataValue.int(len(to_upsert)),
            "posts_skipped_unchanged": MetadataValue.int(skipped),
        },
    )


@op
def _extract_edges_op(
    context: OpExecutionContext,
    sync_result: dict,
    postgres: PostgresResource,
    arango: ArangoDBResource,
    crawler: CrawlerResource,
) -> Output[dict]:
    """Extract XenForo quote edges into the quotes edge collection.

    Depends on sync_result to guarantee posts are written before edges are built.
    Always drops + re-inserts all html_metadata edges for this partition.
    """
    partition_key = sync_result["partition_key"]
    page_url = sync_result["page_url"]

    context.log.info(f"[extract_edges] partition={partition_key!r}")

    rows = postgres.get_repository().fetch_posts_with_blockquotes(page_url)
    arango_repo = arango.get_repository()
    dropped = arango_repo.drop_partition_edges(partition_key)
    edges = build_edges(rows, partition_key)
    arango_repo.insert_edges(edges)

    context.log.info(
        f"[extract_edges] dropped={dropped} inserted={len(edges)} from {len(rows)} posts"
    )
    return Output(
        value={"partition_key": partition_key, "edges_inserted": len(edges)},
        metadata={
            "posts_with_quotes": MetadataValue.int(len(rows)),
            "edges_dropped": MetadataValue.int(dropped),
            "edges_inserted": MetadataValue.int(len(edges)),
        },
    )


@op
def _normalize_op(
    context: OpExecutionContext,
    sync_result: dict,
    postgres: PostgresResource,
    arango: ArangoDBResource,
    crawler: CrawlerResource,
) -> Output[dict]:
    """Strip quoted blocks from post HTML, persist normalized_own_text."""
    partition_key = sync_result["partition_key"]
    page_url = sync_result["page_url"]

    context.log.info(f"[normalize] partition={partition_key!r}")

    arango_repo = arango.get_repository()
    needing = arango_repo.fetch_posts_needing_normalization(partition_key)
    context.log.info(f"[normalize] {len(needing)} posts need normalization")

    if not needing:
        return Output(
            value={"partition_key": partition_key, "normalized": 0},
            metadata={"posts_normalized": MetadataValue.int(0)},
        )

    needing_keys = {p["key"] for p in needing}
    rows = postgres.get_repository().fetch_posts_with_html(page_url)

    patches: list[NormalizedPostDoc] = []
    for row in rows:
        key = str(row.post_id_on_site)
        if key not in needing_keys:
            continue
        normalized = normalize_post_html(row.raw_content_html or "")
        patches.append(
            NormalizedPostDoc(
                key=key,
                normalized_own_text=normalized["own_text"] or None,
                normalized_quoted_blocks=normalized["quoted_blocks"] or None,
                normalization_version=NORMALIZATION_VERSION,
            )
        )

    arango_repo.update_post_normalizations(patches)
    context.log.info(f"[normalize] patched={len(patches)}")
    return Output(
        value={"partition_key": partition_key, "normalized": len(patches)},
        metadata={
            "posts_normalized": MetadataValue.int(len(patches)),
            "normalization_version": MetadataValue.int(NORMALIZATION_VERSION),
        },
    )


@op
def _embed_op(
    context: OpExecutionContext,
    normalize_result: dict,
    arango: ArangoDBResource,
    openai: OpenAIResource,
) -> Output[dict]:
    """Embed normalized_own_text for posts missing embeddings."""
    partition_key = normalize_result["partition_key"]
    context.log.info(f"[embed] partition={partition_key!r}")

    arango_repo = arango.get_repository()
    to_embed = arango_repo.fetch_posts_needing_embedding(partition_key)
    context.log.info(f"[embed] {len(to_embed)} posts need embedding")

    if not to_embed:
        return Output(
            value={"partition_key": partition_key, "embedded": 0},
            metadata={"posts_embedded": MetadataValue.int(0)},
        )

    with openai.get_client(context) as client:
        total = embed_and_update(arango_repo, client, to_embed)

    context.log.info(f"[embed] embedded={total}")
    return Output(
        value={"partition_key": partition_key, "embedded": total},
        metadata={
            "posts_embedded": MetadataValue.int(total),
            "model": MetadataValue.text(EMBEDDING_MODEL),
            "batch_size": MetadataValue.int(EMBED_BATCH_SIZE),
        },
    )


@op(tags={"dagster/concurrency_key": "voz_llm"})
def _classify_op(
    context: OpExecutionContext,
    embed_result: dict,
    arango: ArangoDBResource,
    openai: OpenAIResource,
) -> Output[dict]:
    """LLM enrichment: classify posts + extract company mentions via PydanticAI."""
    partition_key = embed_result["partition_key"]
    context.log.info(f"[classify] partition={partition_key!r}")

    arango_repo = arango.get_repository()
    with openai.get_client(context):
        stats = enrich_partition(arango_repo, openai.api_key, partition_key)

    context.log.info(f"[classify] enriched={stats.enriched} errors={stats.errors}")
    if stats.error_keys:
        context.log.warning(f"[classify] failed keys: {stats.error_keys[:10]}")

    return Output(
        value={"partition_key": partition_key, "enriched": stats.enriched},
        metadata={
            "posts_classified": MetadataValue.int(stats.enriched),
            "errors": MetadataValue.int(stats.errors),
            "llm_request_tokens": MetadataValue.int(stats.request_tokens),
            "llm_response_tokens": MetadataValue.int(stats.response_tokens),
            "enrichment_version": MetadataValue.int(ENRICHMENT_VERSION),
        },
    )


@op
def _extract_mentions_op(
    context: OpExecutionContext,
    classify_result: dict,
    arango: ArangoDBResource,
) -> Output[dict]:
    """Project company mentions from ExtractionResultDoc into graph collections."""
    partition_key = classify_result["partition_key"]
    context.log.info(f"[extract_mentions] partition={partition_key!r}")

    arango_repo = arango.get_repository()
    extraction_results = arango_repo.fetch_extraction_results(partition_key, ENRICHMENT_VERSION)

    mention_docs, mention_edges, alias_evidence = build_company_mention_docs(
        extraction_results, partition_key
    )

    dropped = arango_repo.drop_mention_edges(partition_key)
    arango_repo.upsert_company_mentions(mention_docs)
    arango_repo.insert_mention_edges(mention_edges)
    arango_repo.upsert_alias_evidence(alias_evidence)

    context.log.info(
        f"[extract_mentions] docs={len(mention_docs)} dropped={dropped}"
        f" alias_evidence={len(alias_evidence)}"
    )
    return Output(
        value={"partition_key": partition_key},
        metadata={
            "mention_docs_upserted": MetadataValue.int(len(mention_docs)),
            "mention_edges_dropped": MetadataValue.int(dropped),
            "mention_edges_inserted": MetadataValue.int(len(mention_edges)),
            "alias_evidence_docs": MetadataValue.int(len(alias_evidence)),
        },
    )


# ── Graph-backed multi-asset ─────────────────────────────────────────────────


@graph_multi_asset(
    outs={
        "sync_posts_to_arango": AssetOut(
            group_name="reply_graph",
            description="Layer 1 posts in ArangoDB, content-hash diffed from PostgreSQL.",
        ),
        "extract_explicit_edges": AssetOut(
            group_name="reply_graph",
            description="XenForo quote edges extracted from raw HTML.",
        ),
        "normalize_posts": AssetOut(
            group_name="reply_graph",
            description="Posts with quoted blocks stripped; normalized_own_text populated.",
        ),
        "compute_embeddings": AssetOut(
            group_name="reply_graph",
            description="OpenAI text-embedding-3-small embeddings for normalized_own_text.",
        ),
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
        "dlt_voz_posts": AssetIn(
            key=AssetKey(["dlt_voz_posts"]),
            partition_mapping=IdentityPartitionMapping(),
        )
    },
    partitions_def=voz_pages_partitions,
)
def reply_graph_assets(dlt_voz_posts: Nothing):
    """Full per-partition enrichment pipeline.

    sync → (edges + normalize) → embed → classify → mentions.


    dlt_voz_posts is a Nothing input — ordering-only dependency on the upstream dlt crawl asset.
    extract_explicit_edges and normalize_posts fan out in parallel after sync_posts_to_arango.
    The classify op is tagged voz_llm to bound concurrent LLM partition execution to 1.
    detect_implicit_replies is a separate sensor-gated asset (cross-partition lookback).
    """
    sync = _sync_posts_op(dlt_voz_posts=dlt_voz_posts)
    edges = _extract_edges_op(sync)
    normalized = _normalize_op(sync)
    embedded = _embed_op(normalized)
    classified = _classify_op(embedded)
    mentions = _extract_mentions_op(classified)
    return {
        "sync_posts_to_arango": sync,
        "extract_explicit_edges": edges,
        "normalize_posts": normalized,
        "compute_embeddings": embedded,
        "classify_posts": classified,
        "extract_company_mentions": mentions,
    }


# Apply AutomationCondition.eager() to all 6 asset outputs in the graph.
# graph_multi_asset does not accept automation_condition directly, so we
# use with_attributes post-hoc to set the same condition on every key.
reply_graph_assets = reply_graph_assets.with_attributes(
    automation_condition=AutomationCondition.eager()
)


# ── detect_implicit_replies — sensor-gated, NOT part of the graph ────────────


@asset(
    partitions_def=voz_pages_partitions,
    group_name="reply_graph",
    compute_kind="OpenAI",
    deps=[AssetDep("classify_posts", partition_mapping=IdentityPartitionMapping())],
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
