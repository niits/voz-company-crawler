import dlt
from dagster import (
    AssetExecutionContext,
    DynamicPartitionsDefinition,
)
from dagster_dlt import DagsterDltResource, dlt_assets

from voz_crawler.core.ingestion.html_source.pagination import build_page_url
from voz_crawler.defs.resources.crawler_resource import CrawlerResource
from voz_crawler.defs.resources.postgres_resource import PostgresResource
from voz_crawler.dlt.sources.voz_thread import (
    voz_page_source,
)

# Dynamic partition set — one partition key per thread page number (as string).
# The sensor adds new keys when total_pages grows.
voz_pages_partitions = DynamicPartitionsDefinition(name="voz_pages")

# Evaluated once at module-load time. dlt source construction is lazy:
# no HTTP requests are made. Used only by @dlt_assets to discover asset keys.
# Overridden at runtime with real config inside the asset function body.
_placeholder_source = voz_page_source(
    page_url="https://placeholder", flaresolverr_url="http://placeholder:8191"
)

_placeholder_pipeline = dlt.pipeline(
    pipeline_name="voz_crawler",
    destination=dlt.destinations.postgres(),
    dataset_name="raw",
)


@dlt_assets(
    dlt_source=_placeholder_source,
    dlt_pipeline=_placeholder_pipeline,
    name="voz_page_posts",
    group_name="voz",
    partitions_def=voz_pages_partitions,
)
def voz_page_posts_assets(
    context: AssetExecutionContext,
    dagster_dlt: DagsterDltResource,
    crawler: CrawlerResource,
    postgres: PostgresResource,
):
    """Crawl one Voz thread page (identified by partition key) and load posts.

    The partition key is the page number as a string (e.g. "1", "42").
    Idempotent via write_disposition='merge' + primary_key='post_id_on_site'.
    """
    # Partition key format: "{thread_id}:{page_number}" e.g. "677450:42"
    _, page_num_str = context.partition_key.rsplit(":", 1)
    page_num = int(page_num_str)
    page_url = build_page_url(crawler.thread_url, page_num)
    context.log.info(f"Crawling page {page_num}: {page_url}")

    runtime_source = voz_page_source(
        page_url=page_url,
        flaresolverr_url=crawler.flaresolverr_url,
        http_timeout_seconds=crawler.http_timeout_seconds,
    )
    runtime_pipeline = dlt.pipeline(
        pipeline_name="voz_crawler",
        destination=dlt.destinations.postgres(credentials=postgres.url),
        dataset_name="raw",
    )
    try:
        yield from dagster_dlt.run(
            context=context,
            dlt_source=runtime_source,
            dlt_pipeline=runtime_pipeline,
        )
    finally:
        # Dispose SQLAlchemy engine to release pooled connections back to the OS.
        # Without this, each run's QueuePool (default: 5+10 connections) lingers
        # until GC, causing "too many clients" across sequential partition runs.
        try:
            with runtime_pipeline.destination_client() as client:
                client.sql_client._engine.dispose()
        except Exception as e:
            context.log.warning(f"Failed to dispose SQLAlchemy engine: {e}")
