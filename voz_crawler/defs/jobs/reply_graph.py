from dagster import AssetKey, AssetSelection, DynamicPartitionsDefinition, define_asset_job


def build_thread_jobs(
    thread_id: str,
    partitions_def: DynamicPartitionsDefinition,
    posts_asset_key: AssetKey,
) -> tuple:
    group = f"thread_{thread_id}"
    implicit_key = AssetKey([thread_id, "detect_implicit_replies"])

    reply_graph_job = define_asset_job(
        name=f"reply_graph_job_{thread_id}",
        selection=(
            AssetSelection.groups(group)
            - AssetSelection.assets(posts_asset_key)
            - AssetSelection.assets(implicit_key)
        ),
        partitions_def=partitions_def,
        description=(
            f"Sync posts and extract quote edges to ArangoDB for thread {thread_id}. "
            "Triggered by AutomationCondition.eager() after each crawl partition."
        ),
    )

    implicit_job = define_asset_job(
        name=f"implicit_reply_job_{thread_id}",
        selection=AssetSelection.assets(implicit_key),
        partitions_def=partitions_def,
        description=(
            f"Detect implicit reply edges for thread {thread_id}. "
            "Triggered by implicit_reply_sensor once preceding partitions are enriched."
        ),
        tags={"dagster/concurrency_key": "voz_llm"},
    )

    return reply_graph_job, implicit_job
