from dagster import (
    AssetKey,
    AssetSelection,
    DefaultSensorStatus,
    DynamicPartitionsDefinition,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)


def build_implicit_sensor(
    thread_id: str,
    implicit_job,
    partitions_def: DynamicPartitionsDefinition,
):
    """Returns the implicit_reply_sensor scoped to a single thread.

    Triggers detect_implicit_replies for a partition once all lower-numbered
    partitions in the same thread have materialized extract_company_mentions.
    """

    @sensor(
        name=f"implicit_reply_sensor_{thread_id}",
        job=implicit_job,
        default_status=DefaultSensorStatus.RUNNING,
        minimum_interval_seconds=60,
        description=(
            f"Triggers detect_implicit_replies for thread {thread_id} once all lower-numbered "
            "partitions have materialized extract_company_mentions."
        ),
    )
    def implicit_reply_sensor(context: SensorEvaluationContext) -> SensorResult:
        all_partitions: list[str] = context.instance.get_dynamic_partitions(partitions_def.name)
        if not all_partitions:
            return SensorResult(skip_reason="No partitions registered yet.")

        mentions_key = AssetKey([thread_id, "extract_company_mentions"])
        implicit_key = AssetKey([thread_id, "detect_implicit_replies"])

        mentions_done: set[str] = set(context.instance.get_materialized_partitions(mentions_key))
        implicit_done: set[str] = set(context.instance.get_materialized_partitions(implicit_key))

        sorted_partitions = sorted(all_partitions, key=lambda k: int(k.rsplit(":", 1)[1]))

        run_requests: list[RunRequest] = []
        for i, partition_key in enumerate(sorted_partitions):
            if partition_key in implicit_done:
                continue
            if partition_key not in mentions_done:
                continue
            preceding = sorted_partitions[:i]
            if all(p in mentions_done for p in preceding):
                run_requests.append(
                    RunRequest(
                        run_key=f"implicit-{partition_key}",
                        partition_key=partition_key,
                        asset_selection=AssetSelection.assets(implicit_key),
                    )
                )
            else:
                missing = [p for p in preceding if p not in mentions_done]
                context.log.info(
                    f"[implicit_reply_sensor_{thread_id}] holding {partition_key!r} — "
                    f"{len(missing)} preceding partition(s) not yet enriched: {missing[:5]}"
                )

        if not run_requests:
            return SensorResult(skip_reason="No partitions ready for implicit reply detection.")

        context.log.info(
            f"[implicit_reply_sensor_{thread_id}] queuing {len(run_requests)} partition(s): "
            f"{[r.partition_key for r in run_requests]}"
        )
        return SensorResult(run_requests=run_requests)

    return implicit_reply_sensor
