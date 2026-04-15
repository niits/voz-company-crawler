from dagster import (
    AssetKey,
    DefaultSensorStatus,
    MultiAssetSensorEvaluationContext,
    RunRequest,
    SensorResult,
    multi_asset_sensor,
)

from voz_crawler.defs.jobs.reply_graph import implicit_reply_job

_EXTRACT_MENTIONS_KEY = AssetKey("extract_company_mentions")


@multi_asset_sensor(
    monitored_assets=[_EXTRACT_MENTIONS_KEY],
    job=implicit_reply_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
    description=(
        "Triggers detect_implicit_replies for a partition once all lower-numbered "
        "partitions have materialized extract_company_mentions. "
        "Ensures the cross-partition lookback window is fully populated before running."
    ),
)
def implicit_reply_sensor(context: MultiAssetSensorEvaluationContext) -> SensorResult:
    # Collect all newly materialized partitions since last cursor
    new_events = context.latest_materialization_records_by_partition(_EXTRACT_MENTIONS_KEY)
    if not new_events:
        return SensorResult(skip_reason="No new extract_company_mentions materializations.")

    # All partitions ever materialized for extract_company_mentions
    all_materialized: set[str] = set(
        context.instance.get_materialized_partitions(_EXTRACT_MENTIONS_KEY)
    )

    # Sort all known partitions by page number
    all_known: list[str] = sorted(
        context.instance.get_dynamic_partitions("voz_pages"),
        key=lambda k: int(k.rsplit(":", 1)[1]),
    )

    run_requests: list[RunRequest] = []
    triggered: list[str] = []

    for partition_key in new_events:
        page_num = int(partition_key.rsplit(":", 1)[1])

        # Preceding partitions: all known pages with page_num < this one
        preceding = [k for k in all_known if int(k.rsplit(":", 1)[1]) < page_num]

        if all(p in all_materialized for p in preceding):
            run_requests.append(
                RunRequest(
                    run_key=f"implicit-{partition_key}",
                    partition_key=partition_key,
                )
            )
            triggered.append(partition_key)
        else:
            missing = [p for p in preceding if p not in all_materialized]
            context.log.info(
                f"[implicit_reply_sensor] skipping {partition_key!r} — "
                f"{len(missing)} preceding partition(s) not yet enriched: {missing[:5]}"
            )

    context.advance_all_cursors()

    if not run_requests:
        return SensorResult(
            skip_reason="Preceding partitions not yet fully enriched for any new partition."
        )

    context.log.info(
        f"[implicit_reply_sensor] triggering {len(run_requests)} run(s): {triggered}"
    )
    return SensorResult(run_requests=run_requests)
