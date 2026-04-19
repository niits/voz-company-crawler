from dagster import AssetKey, AssetSelection, DynamicPartitionsDefinition, define_asset_job, job


def build_ingestion_jobs(
    thread_id: str,
    partitions_def: DynamicPartitionsDefinition,
    posts_asset_key: AssetKey,
    discover_op,
) -> tuple:
    crawl_job = define_asset_job(
        name=f"crawl_page_job_{thread_id}",
        selection=AssetSelection.assets(posts_asset_key),
        partitions_def=partitions_def,
        description=f"Crawl one page of thread {thread_id} and load posts to PostgreSQL.",
        tags={"dagster/concurrency_key": "voz_crawl"},
    )

    @job(
        name=f"discover_pages_job_{thread_id}",
        description=f"Discover new pages for thread {thread_id} and register as partitions.",
    )
    def discover_job():
        discover_op()

    return crawl_job, discover_job
