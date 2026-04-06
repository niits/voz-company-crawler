from dagster import AssetSelection, define_asset_job, job

from voz_crawler.defs.assets.ingestion import (
    voz_pages_partitions,
)
from voz_crawler.defs.ops.ingestion import discover_pages_op

crawl_page_job = define_asset_job(
    name="crawl_page_job",
    selection=AssetSelection.groups("voz"),
    partitions_def=voz_pages_partitions,
    description="Crawl one Voz.vn thread page partition and load posts to PostgreSQL.",
    tags={"dagster/concurrency_key": "voz_crawl"},
)


@job(
    description=(
        "Discover new Voz thread pages and register them as partitions. "
        "Run manually or before the first crawl."
    )
)
def discover_pages_job():
    discover_pages_op()
