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

implicit_reply_job = define_asset_job(
    name="implicit_reply_job",
    selection=AssetSelection.assets("detect_implicit_replies"),
    partitions_def=voz_pages_partitions,
    description=(
        "Detect implicit reply edges for a single partition. "
        "Triggered by implicit_reply_sensor once all preceding partitions "
        "have completed extract_company_mentions."
    ),
    tags={"dagster/concurrency_key": "voz_llm"},
)
