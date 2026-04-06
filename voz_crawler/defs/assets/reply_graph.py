from dagster import (
    AssetDep,
    AssetExecutionContext,
    AutomationCondition,
    IdentityPartitionMapping,
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_openai import OpenAIResource

from voz_crawler.core.graph.edge_sync import build_edges
from voz_crawler.core.graph.embedding_sync import (
    EMBED_BATCH_SIZE,
    EMBEDDING_MODEL,
    embed_and_update,
)
from voz_crawler.core.graph.post_sync import build_upsert_docs
from voz_crawler.core.ingestion.html_source.pagination import build_page_url
from voz_crawler.defs.assets.ingestion import voz_page_posts_assets, voz_pages_partitions
from voz_crawler.defs.resources.arango_resource import ArangoDBResource
from voz_crawler.defs.resources.crawler_resource import CrawlerResource
from voz_crawler.defs.resources.postgres_resource import PostgresResource

_upstream_dep = AssetDep(voz_page_posts_assets, partition_mapping=IdentityPartitionMapping())
_sync_dep = AssetDep("sync_posts_to_arango", partition_mapping=IdentityPartitionMapping())


def _page_url(partition_key: str, thread_url: str) -> str:
    _, page_num_str = partition_key.rsplit(":", 1)
    return build_page_url(thread_url, int(page_num_str))


@asset(
    partitions_def=voz_pages_partitions,
    group_name="reply_graph",
    deps=[_upstream_dep],
    automation_condition=AutomationCondition.eager(),
)
def sync_posts_to_arango(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    arango: ArangoDBResource,
    crawler: CrawlerResource,
) -> MaterializeResult:
    """Partition-scoped sync of raw.voz__posts → ArangoDB posts collection.

    Posts whose content hash is unchanged are skipped.
    Changed or new posts are upserted with embedding=None to trigger re-embedding.
    """
    partition_key = context.partition_key
    _, page_num_str = partition_key.rsplit(":", 1)
    page_number = int(page_num_str)
    page_url = _page_url(partition_key, crawler.thread_url)

    context.log.info(
        f"[sync_posts_to_arango] partition={partition_key!r}"
        f" page_number={page_number} page_url={page_url}"
    )

    rows = postgres.get_repository().fetch_posts(page_url)
    context.log.info(f"[sync_posts_to_arango] fetched {len(rows)} posts from PostgreSQL")

    if not rows:
        context.log.warning(f"[sync_posts_to_arango] no posts found for page_url={page_url!r}")
        return MaterializeResult(
            metadata={
                "partition_key": MetadataValue.text(partition_key),
                "page_url": MetadataValue.url(page_url),
                "posts_in_postgres": MetadataValue.int(0),
                "posts_upserted": MetadataValue.int(0),
                "posts_skipped_unchanged": MetadataValue.int(0),
            }
        )

    arango_repo = arango.get_repository()
    existing_hashes = arango_repo.get_existing_hashes(partition_key)
    context.log.info(
        f"[sync_posts_to_arango] {len(existing_hashes)} existing docs"
        f" for partition {partition_key!r}"
    )

    to_upsert, skipped = build_upsert_docs(
        rows, existing_hashes, partition_key, crawler.thread_url, page_number
    )
    arango_repo.upsert_posts(to_upsert)

    context.log.info(
        f"[sync_posts_to_arango] upserted={len(to_upsert)} skipped_unchanged={skipped}"
    )
    return MaterializeResult(
        metadata={
            "partition_key": MetadataValue.text(partition_key),
            "page_url": MetadataValue.url(page_url),
            "posts_in_postgres": MetadataValue.int(len(rows)),
            "posts_upserted": MetadataValue.int(len(to_upsert)),
            "posts_skipped_unchanged": MetadataValue.int(skipped),
        }
    )


@asset(
    partitions_def=voz_pages_partitions,
    group_name="reply_graph",
    deps=[_sync_dep],
    automation_condition=AutomationCondition.eager(),
)
def extract_explicit_edges(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    arango: ArangoDBResource,
    crawler: CrawlerResource,
) -> MaterializeResult:
    """Partition-scoped extraction of XenForo quote edges → ArangoDB quotes collection.

    Always drops all edges for this partition then re-inserts from fresh HTML.
    """
    partition_key = context.partition_key
    page_url = _page_url(partition_key, crawler.thread_url)

    context.log.info(f"[extract_explicit_edges] partition={partition_key!r} page_url={page_url}")

    rows = postgres.get_repository().fetch_posts_with_blockquotes(page_url)
    context.log.info(f"[extract_explicit_edges] {len(rows)} posts with potential quote blocks")

    arango_repo = arango.get_repository()
    dropped = arango_repo.drop_partition_edges(partition_key)
    context.log.info(f"[extract_explicit_edges] dropped {dropped} existing edges")

    edges = build_edges(rows, partition_key)
    arango_repo.insert_edges(edges)
    context.log.info(f"[extract_explicit_edges] inserted {len(edges)} edges from {len(rows)} posts")

    return MaterializeResult(
        metadata={
            "partition_key": MetadataValue.text(partition_key),
            "page_url": MetadataValue.url(page_url),
            "posts_with_quotes": MetadataValue.int(len(rows)),
            "edges_dropped": MetadataValue.int(dropped),
            "edges_inserted": MetadataValue.int(len(edges)),
        }
    )


@asset(
    partitions_def=voz_pages_partitions,
    group_name="reply_graph",
    compute_kind="OpenAI",
    deps=[_sync_dep],
    automation_condition=AutomationCondition.eager(),
)
def compute_embeddings(
    context: AssetExecutionContext,
    arango: ArangoDBResource,
    openai: OpenAIResource,
) -> MaterializeResult:
    """Partition-scoped embedding of posts with missing or reset embeddings.

    Only processes posts where embedding IS NULL — set on insert or reset when content changes.
    Token usage is auto-logged to the Dagster asset catalog via openai.get_client(context).
    """
    partition_key = context.partition_key
    context.log.info(f"[compute_embeddings] partition={partition_key!r}")

    arango_repo = arango.get_repository()
    to_embed = arango_repo.fetch_posts_needing_embedding(partition_key)
    context.log.info(f"[compute_embeddings] {len(to_embed)} posts need embedding")

    if not to_embed:
        return MaterializeResult(
            metadata={
                "partition_key": MetadataValue.text(partition_key),
                "posts_embedded": MetadataValue.int(0),
                "model": MetadataValue.text(EMBEDDING_MODEL),
            }
        )

    with openai.get_client(context) as client:
        total = embed_and_update(arango_repo, client, to_embed)

    context.log.info(f"[compute_embeddings] embedded {total} posts in partition {partition_key!r}")
    return MaterializeResult(
        metadata={
            "partition_key": MetadataValue.text(partition_key),
            "posts_embedded": MetadataValue.int(total),
            "model": MetadataValue.text(EMBEDDING_MODEL),
            "batch_size": MetadataValue.int(EMBED_BATCH_SIZE),
        }
    )
