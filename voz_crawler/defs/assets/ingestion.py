import dlt
from dagster import AssetExecutionContext, AssetKey, AssetSpec, DynamicPartitionsDefinition
from dagster_dlt import DagsterDltTranslator, DagsterDltResource, dlt_assets

from voz_crawler.core.ingestion.html_source.pagination import build_page_url
from voz_crawler.defs.resources.crawler_resource import CrawlerResource
from voz_crawler.defs.resources.postgres_resource import PostgresResource
from voz_crawler.dlt.sources.voz_thread import voz_page_source


class _ThreadDltTranslator(DagsterDltTranslator):
    """Prepends thread_id to every dlt asset key so multi-thread pipelines don't collide."""

    def __init__(self, thread_id: str, group_name: str) -> None:
        self._thread_id = thread_id
        self._group_name = group_name

    def get_asset_spec(self, data) -> AssetSpec:
        spec = super().get_asset_spec(data)
        prefixed_key = AssetKey([self._thread_id] + list(spec.key.path))
        return spec.replace_attributes(key=prefixed_key, group_name=self._group_name)


def build_ingestion_assets(thread_id: str, partitions_def: DynamicPartitionsDefinition):
    """Returns a @dlt_assets definition scoped to a single thread.

    Uses DagsterDltTranslator to inject thread_id as key prefix so asset keys are
    unique across threads (e.g. AssetKey([thread_id, "voz__posts"])).
    pipeline_name is also scoped per thread to avoid dlt state collisions.
    """
    placeholder_source = voz_page_source(
        page_url="https://placeholder",
        flaresolverr_url="http://placeholder:8191",
    )
    placeholder_pipeline = dlt.pipeline(
        pipeline_name=f"voz_crawler_{thread_id}",
        destination=dlt.destinations.postgres(),
        dataset_name="raw",
    )

    @dlt_assets(
        dlt_source=placeholder_source,
        dlt_pipeline=placeholder_pipeline,
        name=f"voz_page_posts_{thread_id}",
        partitions_def=partitions_def,
        dagster_dlt_translator=_ThreadDltTranslator(
            thread_id=thread_id,
            group_name=f"thread_{thread_id}",
        ),
    )
    def voz_page_posts_asset(
        context: AssetExecutionContext,
        dagster_dlt: DagsterDltResource,
        crawler: CrawlerResource,
        postgres: PostgresResource,
    ):
        thread_url = crawler.url_for_thread(thread_id)
        _, page_num_str = context.partition_key.rsplit(":", 1)
        page_url = build_page_url(thread_url, int(page_num_str))
        context.log.info(f"[{thread_id}] Crawling page {page_num_str}: {page_url}")

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

    return voz_page_posts_asset
