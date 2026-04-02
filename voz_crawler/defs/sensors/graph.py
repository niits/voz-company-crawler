from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    RunStatusSensorContext,
    SensorResult,
    run_status_sensor,
)

from ..jobs.crawl import crawl_page_job
from ..jobs.graph import reply_graph_job


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[crawl_page_job],
    request_job=reply_graph_job,
    default_status=DefaultSensorStatus.RUNNING,
    description="Triggers reply_graph_job when crawl_page_job succeeds.",
)
def reply_graph_sensor(context: RunStatusSensorContext) -> SensorResult:
    run_key = f"graph-{context.dagster_run.run_id}"
    return SensorResult(run_requests=[RunRequest(run_key=run_key)])
