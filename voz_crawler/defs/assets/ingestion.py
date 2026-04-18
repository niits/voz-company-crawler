import dlt
from dagster import AssetExecutionContext, AssetKey, DynamicPartitionsDefinition
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData  # noqa: F401 (used in type annotation)

from voz_crawler.core.ingestion.html_source.pagination import build_page_url
from voz_crawler.defs.resources.crawler_resource import CrawlerResource
from voz_crawler.defs.resources.postgres_resource import PostgresResource
from voz_crawler.dlt.sources.voz_thread import voz_page_source


class _ThreadDltTranslator(DagsterDltTranslator):
    """Prefixes all asset keys with thread_id so per-thread dlt assets are unique."""

    def __init__(self, thread_id: str, group_name: str):
        self._thread_id = thread_id
        self._group_name = group_name

    def get_asset_spec(self, data: DltResourceTranslatorData):
        spec = super().get_asset_spec(data)
        return spec.replace_attributes(
            key=AssetKey([self._thread_id, *spec.key.path]),
            group_name=self._group_name,
        )


def build_ingestion_assets(thread_id: str, partitions_def: DynamicPartitionsDefinition):
    """Returns a dlt_assets definition scoped to a single thread.

    Uses _ThreadDltTranslator to prefix asset keys with thread_id, giving each thread
    unique asset keys (e.g. AssetKey([thread_id, "dlt_voz_page_voz__posts"])).

    The placeholder source/pipeline are constructed at decoration time (module load)
    with dummy URLs — dlt source construction is lazy, no HTTP requests are made.
    The real source and pipeline are created inside the asset function body.
    """
    _placeholder_source = voz_page_source(
        page_url="https://placeholder", flaresolverr_url="http://placeholder:8191"
    )
    _placeholder_pipeline = dlt.pipeline(
        pipeline_name=f"voz_crawler_{thread_id}",
        destination=dlt.destinations.postgres(),
        dataset_name="raw",
    )

    @dlt_assets(
        dlt_source=_placeholder_source,
        dlt_pipeline=_placeholder_pipeline,
        name=f"voz_page_posts_{thread_id}",
        dagster_dlt_translator=_ThreadDltTranslator(
            thread_id=thread_id,
            group_name=f"thread_{thread_id}",
        ),
        partitions_def=partitions_def,
    )
    def ingestion_assets(
        context: AssetExecutionContext,
        dagster_dlt: DagsterDltResource,
        crawler: CrawlerResource,
        postgres: PostgresResource,
    ):
        _, page_num_str = context.partition_key.rsplit(":", 1)
        page_num = int(page_num_str)
        page_url = build_page_url(crawler.url_for_partition(context.partition_key), page_num)
        context.log.info(f"Crawling page {page_num}: {page_url}")

        runtime_source = voz_page_source(
            page_url=page_url,
            flaresolverr_url=crawler.flaresolverr_url,
            http_timeout_seconds=crawler.http_timeout_seconds,
        )
        runtime_pipeline = dlt.pipeline(
            pipeline_name=f"voz_crawler_{thread_id}",
            destination=dlt.destinations.postgres(credentials=postgres.url),
            dataset_name=postgres.raw_schema,
        )
        yield from dagster_dlt.run(
            context=context,
            dlt_source=runtime_source,
            dlt_pipeline=runtime_pipeline,
        )

    return ingestion_assets
