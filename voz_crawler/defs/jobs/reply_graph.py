from dagster import AssetSelection, define_asset_job

from voz_crawler.defs.assets.ingestion import voz_pages_partitions

reply_graph_job = define_asset_job(
    name="reply_graph_job",
    selection=AssetSelection.groups("reply_graph"),
    partitions_def=voz_pages_partitions,
    description=(
        "Sync posts and extract quote edges to ArangoDB for one thread page partition. "
        "Triggered by reply_graph_sensor after each successful crawl_page_job."
    ),
)
