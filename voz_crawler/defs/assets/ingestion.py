import dlt
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets

from voz_crawler.sources.voz_thread import voz_page_source
from voz_crawler.utils.pagination import build_page_url

from ..partitions import voz_pages_partitions
from ..resources.crawler import CrawlerResource
from ..resources.postgres import PostgresResource

_placeholder_source = voz_page_source(
    page_url="https://placeholder",
    flaresolverr_url="http://placeholder:8191",
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
    """Crawl one Voz thread page (identified by partition key) and load posts."""
    page_num = int(context.partition_key)
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
        try:
            with runtime_pipeline.destination_client() as client:
                client.sql_client._engine.dispose()
        except Exception:
            pass


(_POSTS_ASSET_KEY,) = voz_page_posts_assets.keys
