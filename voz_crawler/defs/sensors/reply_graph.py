from dagster import (
    AssetKey,
    DefaultSensorStatus,
    DynamicPartitionsDefinition,
    MultiAssetSensorEvaluationContext,
    RunRequest,
    SensorResult,
    multi_asset_sensor,
)


def build_implicit_sensor(
    thread_id: str,
    implicit_job,
    partitions_def: DynamicPartitionsDefinition,
):
    extract_mentions_key = AssetKey([thread_id, "extract_company_mentions"])
    partition_name = partitions_def.name

    @multi_asset_sensor(
        name=f"implicit_reply_sensor_{thread_id}",
        monitored_assets=[extract_mentions_key],
        job=implicit_job,
        default_status=DefaultSensorStatus.RUNNING,
        minimum_interval_seconds=30,
        description=(
            f"Triggers detect_implicit_replies for thread {thread_id} once all "
            "lower-numbered partitions have materialized extract_company_mentions."
        ),
    )
    def implicit_sensor(context: MultiAssetSensorEvaluationContext) -> SensorResult:
        new_events = context.latest_materialization_records_by_partition(extract_mentions_key)
        if not new_events:
            return SensorResult(skip_reason="No new extract_company_mentions materializations.")

        all_materialized: set[str] = set(
            context.instance.get_materialized_partitions(extract_mentions_key)
        )
        all_known: list[str] = sorted(
            context.instance.get_dynamic_partitions(partition_name),
            key=lambda k: int(k.rsplit(":", 1)[1]),
        )

        run_requests: list[RunRequest] = []
        triggered: list[str] = []

        for partition_key in new_events:
            page_num = int(partition_key.rsplit(":", 1)[1])
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
                    f"[implicit_reply_sensor_{thread_id}] skipping {partition_key!r} — "
                    f"{len(missing)} preceding partition(s) not yet enriched: {missing[:5]}"
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
