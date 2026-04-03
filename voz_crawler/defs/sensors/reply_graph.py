from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    RunStatusSensorContext,
    SensorResult,
    run_status_sensor,
)

from voz_crawler.defs.jobs.ingestion import crawl_page_job
from voz_crawler.defs.jobs.reply_graph import reply_graph_job

_BACKFILL_TAG = "dagster/backfill"
_PARTITION_TAG = "dagster/partition"


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[crawl_page_job],
    request_job=reply_graph_job,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Trigger reply_graph_job for the same partition after crawl_page_job succeeds. "
        "Skips backfill runs — trigger reply_graph_job manually after a backfill completes."
    ),
)
def reply_graph_sensor(context: RunStatusSensorContext) -> SensorResult:
    # Skip backfill runs: concurrent partition runs would cause no data corruption
    # (partition-scoped logic is idempotent) but would queue redundant jobs.
    # One manual reply_graph_job run after the backfill is cleaner.
    if _BACKFILL_TAG in context.dagster_run.tags:
        backfill_id = context.dagster_run.tags[_BACKFILL_TAG]
        return SensorResult(
            skip_reason=(
                f"Skipping backfill run (backfill_id={backfill_id}). "
                "Run reply_graph_job manually after the backfill completes."
            )
        )

    partition_key = context.dagster_run.tags.get(_PARTITION_TAG)
    if not partition_key:
        return SensorResult(skip_reason="No partition key found on triggering run.")

    run_key = f"reply-graph-{partition_key}-{context.dagster_run.run_id[:8]}"
    return SensorResult(run_requests=[RunRequest(run_key=run_key, partition_key=partition_key)])
