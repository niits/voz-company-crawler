import datetime

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    RunStatusSensorContext,
    SensorEvaluationContext,
    SensorResult,
    run_status_sensor,
    sensor,
)

from voz_crawler.defs.assets.ingestion import voz_page_posts_assets
from voz_crawler.defs.jobs.ingestion import crawl_page_job, discover_pages_job

(_POSTS_ASSET_KEY,) = voz_page_posts_assets.keys


@sensor(
    job=discover_pages_job,
    minimum_interval_seconds=60 * 60 * 6,
    default_status=DefaultSensorStatus.RUNNING,
    description="Triggers discover_pages_job every 6 hours to pick up new thread pages.",
)
def voz_discover_sensor(context: SensorEvaluationContext) -> SensorResult:
    # One run per evaluation window — run_key prevents duplicate runs within the window.
    run_key = f"discover-{datetime.date.today().isoformat()}-{context.cursor or '0'}"
    return SensorResult(run_requests=[RunRequest(run_key=run_key)])


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[discover_pages_job],
    request_job=crawl_page_job,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "When discover_pages_job succeeds: submits crawl runs for "
        "unmaterialized partitions + last page (always re-crawled daily)."
    ),
)
def voz_crawl_sensor(context: RunStatusSensorContext) -> SensorResult:
    all_keys: list[str] = sorted(context.instance.get_dynamic_partitions("voz_pages"), key=int)
    if not all_keys:
        return SensorResult(skip_reason="No partitions registered yet.")

    last_page_key = all_keys[-1]
    materialized: set[str] = context.instance.get_materialized_partitions(_POSTS_ASSET_KEY)

    today = datetime.date.today().isoformat()
    run_requests: list[RunRequest] = []

    for page_key in all_keys:
        if page_key == last_page_key:
            # Always re-crawl last page; daily run_key re-fires even after materialization.
            run_requests.append(
                RunRequest(
                    run_key=f"page-{page_key}-{today}",
                    partition_key=page_key,
                )
            )
        elif page_key not in materialized:
            # Historical page: stable run_key fires exactly once.
            run_requests.append(
                RunRequest(
                    run_key=f"page-{page_key}",
                    partition_key=page_key,
                )
            )

    if not run_requests:
        return SensorResult(
            skip_reason="All pages materialized and last page already crawled today."
        )

    context.log.info(
        f"Queuing {len(run_requests)} run(s): {[r.partition_key for r in run_requests]}"
    )
    return SensorResult(run_requests=run_requests)
