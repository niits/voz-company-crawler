from dagster import (
    AssetKey,
    DefaultSensorStatus,
    MultiAssetSensorEvaluationContext,
    RunRequest,
    SensorResult,
    multi_asset_sensor,
)

# How many immediately preceding partitions must have materialized
# extract_company_mentions before a partition's detect_implicit_replies can run.
# Bounded (not "all history") so one permanently-stuck early page can't block
# implicit-reply detection on every later page forever. Matches
# fetch_thread_window_posts's cross-page candidate lookback in implicit_reply_sync.py —
# most implicit replies land within 1-2 pages, so this is a deliberate liveness/recall
# trade-off, not a hard correctness guarantee for replies further back.
PRECEDING_PARTITIONS_REQUIRED = 2


def build_implicit_sensor(
    thread_id: str,
    implicit_job,
):
    extract_mentions_key = AssetKey([thread_id, "extract_company_mentions"])

    @multi_asset_sensor(
        name=f"implicit_reply_sensor_{thread_id}",
        monitored_assets=[extract_mentions_key],
        job=implicit_job,
        default_status=DefaultSensorStatus.RUNNING,
        minimum_interval_seconds=30,
        description=(
            f"Triggers detect_implicit_replies for thread {thread_id} once the "
            f"{PRECEDING_PARTITIONS_REQUIRED} immediately preceding partition(s) have "
            "materialized extract_company_mentions."
        ),
    )
    def implicit_sensor(context: MultiAssetSensorEvaluationContext) -> SensorResult:
        new_events = context.latest_materialization_records_by_partition(extract_mentions_key)
        if not new_events:
            return SensorResult(skip_reason="No new extract_company_mentions materializations.")

        all_materialized: set[str] = set(
            context.instance.get_materialized_partitions(extract_mentions_key)
        )

        run_requests: list[RunRequest] = []
        triggered: list[str] = []

        for partition_key in new_events:
            page_num = int(partition_key.rsplit(":", 1)[1])
            # Exact page-number arithmetic, not "last N registered partitions" —
            # page 4 must specifically have pages 2 and 3 done, contiguous by page
            # number, regardless of registration order or any gaps in all_known.
            required = [
                f"{thread_id}:{n}"
                for n in range(max(1, page_num - PRECEDING_PARTITIONS_REQUIRED), page_num)
            ]

            if all(p in all_materialized for p in required):
                run_requests.append(
                    RunRequest(
                        run_key=f"implicit-{partition_key}",
                        partition_key=partition_key,
                    )
                )
                triggered.append(partition_key)
            else:
                missing = [p for p in required if p not in all_materialized]
                context.log.info(
                    f"[implicit_reply_sensor_{thread_id}] skipping {partition_key!r} — "
                    f"required preceding partition(s) not yet enriched: {missing}"
                )

        context.advance_all_cursors()

        if not run_requests:
            return SensorResult(
                skip_reason="Preceding partitions not yet fully enriched for any new partition."
            )

        context.log.info(
            f"[implicit_reply_sensor_{thread_id}] triggering"
            f" {len(run_requests)} run(s): {triggered}"
        )
        return SensorResult(run_requests=run_requests)

    return implicit_sensor
