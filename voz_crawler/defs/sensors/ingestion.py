import datetime

from dagster import (
    AssetKey,
    DagsterRunStatus,
    DefaultSensorStatus,
    DynamicPartitionsDefinition,
    RunRequest,
    RunStatusSensorContext,
    SensorEvaluationContext,
    SensorResult,
    run_status_sensor,
    sensor,
)


def build_ingestion_sensors(
    thread_id: str,
    discover_job,
    crawl_job,
    partitions_def: DynamicPartitionsDefinition,
    posts_asset_key: AssetKey,
):
    """Returns (discover_sensor, crawl_sensor) scoped to a single thread."""

    @sensor(
        name=f"voz_discover_sensor_{thread_id}",
        job=discover_job,
        minimum_interval_seconds=60 * 60 * 6,
        default_status=DefaultSensorStatus.RUNNING,
        description=f"Triggers discover_pages_job_{thread_id} every 6 hours to pick up new pages.",
    )
    def discover_sensor(context: SensorEvaluationContext) -> SensorResult:
        run_key = (
            f"discover-{thread_id}-{datetime.date.today().isoformat()}-{context.cursor or '0'}"
        )
        return SensorResult(run_requests=[RunRequest(run_key=run_key)])

    @run_status_sensor(
        name=f"voz_crawl_sensor_{thread_id}",
        run_status=DagsterRunStatus.SUCCESS,
        monitored_jobs=[discover_job],
        request_job=crawl_job,
        default_status=DefaultSensorStatus.RUNNING,
        description=(
            f"When discover_pages_job_{thread_id} succeeds: submits crawl runs for "
            "unmaterialized partitions + last page (always re-crawled daily)."
        ),
    )
    def crawl_sensor(context: RunStatusSensorContext) -> SensorResult:
        all_keys: list[str] = context.instance.get_dynamic_partitions(partitions_def.name)
        if not all_keys:
            return SensorResult(skip_reason="No partitions registered yet.")

        materialized: set[str] = context.instance.get_materialized_partitions(posts_asset_key)
        today = datetime.date.today().isoformat()

        sorted_keys = sorted(all_keys, key=lambda k: int(k.rsplit(":", 1)[1]))
        last_key = sorted_keys[-1]

        run_requests: list[RunRequest] = []
        for page_key in sorted_keys:
            if page_key == last_key:
                run_requests.append(
                    RunRequest(
                        run_key=f"page-{page_key}-{today}",
                        partition_key=page_key,
                        tags={"dagster/concurrency_key": "voz_crawl"},
                    )
                )
            elif page_key not in materialized:
                run_requests.append(
                    RunRequest(
                        run_key=f"page-{page_key}",
                        partition_key=page_key,
                        tags={"dagster/concurrency_key": "voz_crawl"},
                    )
                )

        if not run_requests:
            return SensorResult(
                skip_reason="All pages materialized and last pages already crawled today."
            )

        context.log.info(
            f"Queuing {len(run_requests)} run(s): {[r.partition_key for r in run_requests]}"
        )
        return SensorResult(run_requests=run_requests)

    return discover_sensor, crawl_sensor
