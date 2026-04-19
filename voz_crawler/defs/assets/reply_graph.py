from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetKey,
    AutomationCondition,
    DynamicPartitionsDefinition,
    IdentityPartitionMapping,
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_openai import OpenAIResource

from voz_crawler.core.entities.arango import ExtractionResultDoc, NormalizedPostDoc
from voz_crawler.core.entities.enrichment import (
    ENRICHMENT_VERSION,
    NORMALIZATION_VERSION,
)
from voz_crawler.core.graph.company_sync import build_company_mention_docs
from voz_crawler.core.graph.edge_sync import build_edges
from voz_crawler.core.graph.embedding_sync import EMBEDDING_MODEL, embed_batch
from voz_crawler.core.graph.enrichment_sync import run_llm_enrichment
from voz_crawler.core.graph.implicit_reply_sync import process_partition_implicit_replies
from voz_crawler.core.graph.normalizer import normalize_post_html
from voz_crawler.core.graph.post_sync import build_upsert_docs
from voz_crawler.core.ingestion.html_source.pagination import build_page_url
from voz_crawler.defs.resources.arango_resource import ArangoDBResource
from voz_crawler.defs.resources.crawler_resource import CrawlerResource
from voz_crawler.defs.resources.postgres_resource import PostgresResource


def _page_url(partition_key: str, crawler: CrawlerResource) -> str:
    _, page_num_str = partition_key.rsplit(":", 1)
    return build_page_url(crawler.url_for_partition(partition_key), int(page_num_str))


# ═══════════════════════════════════════════════════════════════════════════════
# Per-thread asset factory
# ═══════════════════════════════════════════════════════════════════════════════


def build_thread_assets(
    thread_id: str,
    partitions_def: DynamicPartitionsDefinition,
    posts_asset_key: AssetKey,
) -> list:
    """Returns all 7 reply-graph asset definitions scoped to a single thread.

    Each call creates uniquely-named asset definitions (key_prefix=[thread_id])
    so multiple threads can coexist in the same Definitions without key collision.
    """
    group = f"thread_{thread_id}"

    # ── 1. sync_posts_to_arango ──────────────────────────────────────────────

    def _sync_fn(
        context: AssetExecutionContext,
        postgres: PostgresResource,
        arango: ArangoDBResource,
        crawler: CrawlerResource,
    ) -> MaterializeResult:
        partition_key = context.partition_key
        _, page_num_str = partition_key.rsplit(":", 1)
        page_number = int(page_num_str)
        page_url = _page_url(partition_key, crawler)

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
        thread_url = crawler.url_for_partition(partition_key)
        to_upsert, skipped = build_upsert_docs(
            rows, existing_hashes, partition_key, thread_url, page_number
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

    sync_posts_to_arango = asset(
        name="sync_posts_to_arango",
        key_prefix=[thread_id],
        partitions_def=partitions_def,
        group_name=group,
        compute_kind="ArangoDB",
        deps=[AssetDep(posts_asset_key, partition_mapping=IdentityPartitionMapping())],
        automation_condition=AutomationCondition.eager(),
        description="Layer 1 posts in ArangoDB, content-hash diffed from PostgreSQL.",
    )(_sync_fn)

    # ── 2. extract_explicit_edges ────────────────────────────────────────────

    def _edges_fn(
        context: AssetExecutionContext,
        postgres: PostgresResource,
        arango: ArangoDBResource,
        crawler: CrawlerResource,
    ) -> MaterializeResult:
        partition_key = context.partition_key
        page_url = _page_url(partition_key, crawler)

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

    extract_explicit_edges = asset(
        name="extract_explicit_edges",
        key_prefix=[thread_id],
        partitions_def=partitions_def,
        group_name=group,
        compute_kind="ArangoDB",
        deps=[
            AssetDep(
                AssetKey([thread_id, "sync_posts_to_arango"]),
                partition_mapping=IdentityPartitionMapping(),
            )
        ],
        automation_condition=AutomationCondition.eager(),
        description="XenForo quote edges extracted from raw HTML blockquotes.",
    )(_edges_fn)

    # ── 3. normalize_posts ───────────────────────────────────────────────────

    def _normalize_fn(
        context: AssetExecutionContext,
        postgres: PostgresResource,
        arango: ArangoDBResource,
        crawler: CrawlerResource,
    ) -> MaterializeResult:
        partition_key = context.partition_key
        page_url = _page_url(partition_key, crawler)

        arango_repo = arango.get_repository()
        needing_keys = set(
            arango_repo.fetch_posts_needing_normalization(partition_key, NORMALIZATION_VERSION)
        )
        context.log.info(f"[normalize] {len(needing_keys)} posts need normalization")

        if not needing_keys:
            return MaterializeResult(
                metadata={
                    "normalized": MetadataValue.int(0),
                    "normalization_version": MetadataValue.int(NORMALIZATION_VERSION),
                }
            )

        rows = postgres.get_repository().fetch_posts_with_html(page_url)
        filtered = [r for r in rows if str(r.post_id_on_site) in needing_keys]

        patches: list[NormalizedPostDoc] = []
        for row in filtered:
            normalized = normalize_post_html(row.raw_content_html or "")
            patches.append(
                NormalizedPostDoc(
                    key=str(row.post_id_on_site),
                    normalized_own_text=normalized["own_text"] or None,
                    normalized_quoted_blocks=normalized["quoted_blocks"] or None,
                    normalization_version=NORMALIZATION_VERSION,
                )
            )

        arango_repo.update_post_normalizations(patches)
        context.log.info(f"[normalize] wrote {len(patches)} patches")
        return MaterializeResult(
            metadata={
                "normalized": MetadataValue.int(len(patches)),
                "normalization_version": MetadataValue.int(NORMALIZATION_VERSION),
            }
        )

    normalize_posts = asset(
        name="normalize_posts",
        key_prefix=[thread_id],
        partitions_def=partitions_def,
        group_name=group,
        compute_kind="ArangoDB",
        deps=[
            AssetDep(
                AssetKey([thread_id, "sync_posts_to_arango"]),
                partition_mapping=IdentityPartitionMapping(),
            )
        ],
        automation_condition=AutomationCondition.eager(),
        description="HTML stripped; normalized_own_text populated. Staleness: normalization_version.",  # noqa: E501
    )(_normalize_fn)

    # ── 4. compute_embeddings ────────────────────────────────────────────────

    def _embed_fn(
        context: AssetExecutionContext,
        arango: ArangoDBResource,
        openai: OpenAIResource,
    ) -> MaterializeResult:
        partition_key = context.partition_key
        arango_repo = arango.get_repository()

        items = arango_repo.fetch_posts_needing_embedding(partition_key)
        context.log.info(f"[embed] {len(items)} posts need embedding")

        if not items:
            return MaterializeResult(
                metadata={
                    "embedded": MetadataValue.int(0),
                    "model": MetadataValue.text(EMBEDDING_MODEL),
                }
            )

        with openai.get_client(context) as client:
            embed_patches = embed_batch(client, items)

        arango_repo.update_post_embeddings(embed_patches)
        context.log.info(f"[embed] wrote {len(embed_patches)} patches")
        return MaterializeResult(
            metadata={
                "embedded": MetadataValue.int(len(embed_patches)),
                "model": MetadataValue.text(EMBEDDING_MODEL),
            }
        )

    compute_embeddings = asset(
        name="compute_embeddings",
        key_prefix=[thread_id],
        partitions_def=partitions_def,
        group_name=group,
        compute_kind="OpenAI",
        deps=[
            AssetDep(
                AssetKey([thread_id, "normalize_posts"]),
                partition_mapping=IdentityPartitionMapping(),
            )
        ],
        op_tags={"dagster/concurrency_key": "voz_llm"},
        automation_condition=AutomationCondition.eager(),
        description="OpenAI text-embedding-3-small embeddings for normalized_own_text.",
    )(_embed_fn)

    # ── 5. extract_company_mentions (classify + extract merged) ─────────────

    def _mentions_fn(
        context: AssetExecutionContext,
        arango: ArangoDBResource,
        openai: OpenAIResource,
    ) -> MaterializeResult:
        partition_key = context.partition_key
        arango_repo = arango.get_repository()

        # Phase 1 — LLM classification + extraction (skipped if already current)
        items = arango_repo.fetch_posts_needing_enrichment(partition_key, ENRICHMENT_VERSION)
        context.log.info(f"[extract_mentions] {len(items)} posts need enrichment")

        llm_stats = None
        if items:
            extraction_docs, enrichment_patches, llm_stats = run_llm_enrichment(
                items, openai.api_key, partition_key
            )
            arango_repo.upsert_extraction_results(extraction_docs)
            arango_repo.patch_post_enrichment_fields(enrichment_patches)
            context.log.info(
                f"[extract_mentions] enriched={llm_stats.enriched} errors={llm_stats.errors}"
            )
            if llm_stats.error_keys:
                context.log.warning(
                    f"[extract_mentions] failed keys: {llm_stats.error_keys[:10]}"
                )

        # Phase 2 — project mentions (always runs so re-runs recover failed writes)
        raw_docs = arango_repo.fetch_extraction_results(partition_key, ENRICHMENT_VERSION)
        result_docs = [ExtractionResultDoc.model_validate(d) for d in raw_docs]
        non_noise = [d for d in result_docs if d.content_class in {"review", "rating", "event"}]

        context.log.info(
            f"[extract_mentions] total={len(result_docs)} eligible={len(non_noise)}"
        )

        enriched_count = llm_stats.enriched if llm_stats else 0
        error_count = llm_stats.errors if llm_stats else 0
        req_tokens = llm_stats.request_tokens if llm_stats else 0
        res_tokens = llm_stats.response_tokens if llm_stats else 0

        if not non_noise:
            return MaterializeResult(
                metadata={
                    "enriched": MetadataValue.int(enriched_count),
                    "errors": MetadataValue.int(error_count),
                    "enrichment_version": MetadataValue.int(ENRICHMENT_VERSION),
                    "mention_docs": MetadataValue.int(0),
                    "skipped_reason": MetadataValue.text("all posts are noise/question class"),
                }
            )

        mention_docs, mention_edges, alias_evidence = build_company_mention_docs(
            non_noise, partition_key
        )

        arango_repo.drop_mention_edges(partition_key)
        arango_repo.upsert_company_mentions(mention_docs)
        arango_repo.insert_mention_edges(mention_edges)
        arango_repo.upsert_alias_evidence(alias_evidence)

        context.log.info(
            f"[extract_mentions] docs={len(mention_docs)} edges={len(mention_edges)}"
            f" alias_evidence={len(alias_evidence)}"
        )
        return MaterializeResult(
            metadata={
                "enriched": MetadataValue.int(enriched_count),
                "errors": MetadataValue.int(error_count),
                "llm_request_tokens": MetadataValue.int(req_tokens),
                "llm_response_tokens": MetadataValue.int(res_tokens),
                "enrichment_version": MetadataValue.int(ENRICHMENT_VERSION),
                "mention_docs": MetadataValue.int(len(mention_docs)),
                "mention_edges": MetadataValue.int(len(mention_edges)),
                "alias_evidence_docs": MetadataValue.int(len(alias_evidence)),
            }
        )

    extract_company_mentions = asset(
        name="extract_company_mentions",
        key_prefix=[thread_id],
        partitions_def=partitions_def,
        group_name=group,
        compute_kind="OpenAI",
        deps=[
            AssetDep(
                AssetKey([thread_id, "normalize_posts"]),
                partition_mapping=IdentityPartitionMapping(),
            )
        ],
        op_tags={"dagster/concurrency_key": "voz_llm"},
        automation_condition=AutomationCondition.eager(),
        description=(
            "LLM classify (content_class, enrichment_version) then project"
            " company mention vertices + edges for posts in {review, rating, event}."
        ),
    )(_mentions_fn)

    # ── 7. detect_implicit_replies ───────────────────────────────────────────

    def _implicit_fn(
        context: AssetExecutionContext,
        arango: ArangoDBResource,
        openai: OpenAIResource,
    ) -> MaterializeResult:
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

    detect_implicit_replies = asset(
        name="detect_implicit_replies",
        key_prefix=[thread_id],
        partitions_def=partitions_def,
        group_name=group,
        compute_kind="OpenAI",
        deps=[
            AssetDep(
                AssetKey([thread_id, "extract_company_mentions"]),
                partition_mapping=IdentityPartitionMapping(),
            ),
            AssetDep(
                AssetKey([thread_id, "extract_explicit_edges"]),
                partition_mapping=IdentityPartitionMapping(),
            ),
        ],
        op_tags={"dagster/concurrency_key": "voz_llm"},
    )(_implicit_fn)

    return [
        sync_posts_to_arango,
        extract_explicit_edges,
        normalize_posts,
        compute_embeddings,
        extract_company_mentions,
        detect_implicit_replies,
    ]
