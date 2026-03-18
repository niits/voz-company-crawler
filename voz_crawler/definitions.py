import datetime

import dlt
from dagster import (
    AssetExecutionContext,
    AssetSelection,
    ConfigurableResource,
    DagsterRunStatus,
    DefaultSensorStatus,
    Definitions,
    DynamicPartitionsDefinition,
    EnvVar,
    OpExecutionContext,
    RunRequest,
    RunStatusSensorContext,
    SensorEvaluationContext,
    SensorResult,
    define_asset_job,
    job,
    op,
    run_status_sensor,
    sensor,
)
from dagster_dlt import DagsterDltResource, dlt_assets

from voz_crawler.sources.voz_thread import fetch_via_flaresolverr, is_cf_block, voz_page_source
from voz_crawler.utils.pagination import build_page_url, discover_total_pages


class PostgresResource(ConfigurableResource):
    """Connection parameters for the target PostgreSQL database.

    Fields are resolved from environment variables at runtime via EnvVar.
    The `url` property assembles the SQLAlchemy connection string used by dlt.
    """

    user: str
    password: str
    host: str
    port: int = 5432
    db: str

    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


class CrawlerResource(ConfigurableResource):
    """Runtime configuration for the Voz thread crawler.

    `http_delay_seconds` is retained for environment variable compatibility
    but is no longer used by the asset function (each run fetches one page).
    The `delay` property is kept for backwards compatibility.
    """

    thread_url: str
    flaresolverr_url: str
    http_delay_seconds: str = "2"
    http_timeout_seconds: int = 60

    @property
    def delay(self) -> float:
        return float(self.http_delay_seconds)


# Dynamic partition set — one partition key per thread page number (as string).
# The sensor adds new keys when total_pages grows.
voz_pages_partitions = DynamicPartitionsDefinition(name="voz_pages")

# Evaluated once at module-load time. dlt source construction is lazy:
# no HTTP requests are made. Used only by @dlt_assets to discover asset keys.
# Overridden at runtime with real config inside the asset function body.
_placeholder_source = voz_page_source(page_url="https://placeholder", flaresolverr_url="http://placeholder:8191")

_placeholder_pipeline = dlt.pipeline(
    pipeline_name="voz_crawler",
    destination=dlt.destinations.postgres(),
    dataset_name="raw",
)


@dlt_assets(
    dlt_source=_placeholder_source,
    dlt_pipeline=_placeholder_pipeline,
    name="voz_page_posts",
    group_name="voz",
    partitions_def=voz_pages_partitions,
)
def voz_page_posts_assets(
    context: AssetExecutionContext,
    dagster_dlt: DagsterDltResource,
    crawler: CrawlerResource,
    postgres: PostgresResource,
):
    """Crawl one Voz thread page (identified by partition key) and load posts.

    The partition key is the page number as a string (e.g. "1", "42").
    Idempotent via write_disposition='merge' + primary_key='post_id_on_site'.
    """
    page_num = int(context.partition_key)
    page_url = build_page_url(crawler.thread_url, page_num)
    context.log.info(f"Crawling page {page_num}: {page_url}")

    runtime_source = voz_page_source(
        page_url=page_url,
        flaresolverr_url=crawler.flaresolverr_url,
        http_timeout_seconds=crawler.http_timeout_seconds,
    )
    runtime_pipeline = dlt.pipeline(
        pipeline_name="voz_crawler",
        destination=dlt.destinations.postgres(credentials=postgres.url),
        dataset_name="raw",
    )
    try:
        yield from dagster_dlt.run(
            context=context,
            dlt_source=runtime_source,
            dlt_pipeline=runtime_pipeline,
        )
    finally:
        # Dispose SQLAlchemy engine to release pooled connections back to the OS.
        # Without this, each run's QueuePool (default: 5+10 connections) lingers
        # until GC, causing "too many clients" across sequential partition runs.
        try:
            with runtime_pipeline.destination_client() as client:
                client.sql_client._engine.dispose()
        except Exception:
            pass


crawl_page_job = define_asset_job(
    name="crawl_page_job",
    selection=AssetSelection.groups("voz"),
    partitions_def=voz_pages_partitions,
    description="Crawl one Voz.vn thread page partition and load posts to PostgreSQL.",
)


@op
def discover_pages_op(context: OpExecutionContext, crawler: CrawlerResource) -> int:
    """Fetch page 1 to discover total pages, then register new partition keys.

    Safe to run repeatedly — only adds keys that don't exist yet.
    Raises RuntimeError on Cloudflare block so the run shows as failed.
    """
    page1_url = build_page_url(crawler.thread_url, 1)
    status_code, html = fetch_via_flaresolverr(
        page1_url, crawler.flaresolverr_url, timeout=crawler.http_timeout_seconds
    )
    if is_cf_block(status_code, html):
        raise RuntimeError(f"Cloudflare blocked page 1 discovery (HTTP {status_code}).")

    total_pages = discover_total_pages(html)
    context.log.info(f"Discovered {total_pages} total pages.")

    existing_keys: set[str] = set(context.instance.get_dynamic_partitions("voz_pages"))
    new_keys = [str(p) for p in range(1, total_pages + 1) if str(p) not in existing_keys]
    if new_keys:
        context.instance.add_dynamic_partitions("voz_pages", new_keys)
        preview = new_keys[:5] + (["..."] if len(new_keys) > 5 else [])
        context.log.info(f"Added {len(new_keys)} new partition(s): {preview}")
    else:
        context.log.info("No new pages to register.")

    return total_pages


@job(description=(
    "Discover new Voz thread pages and register them as partitions. "
    "Run manually or before the first crawl."
))
def discover_pages_job():
    discover_pages_op()


# Derived from the @dlt_assets definition — avoids hardcoding the DagsterDltTranslator
# naming convention. voz_page_posts_assets has exactly one asset key (resource "posts").
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
    all_keys: list[str] = sorted(
        context.instance.get_dynamic_partitions("voz_pages"), key=int
    )
    if not all_keys:
        return SensorResult(skip_reason="No partitions registered yet.")

    last_page_key = all_keys[-1]
    materialized: set[str] = context.instance.get_materialized_partitions(_POSTS_ASSET_KEY)

    today = datetime.date.today().isoformat()
    run_requests: list[RunRequest] = []

    for page_key in all_keys:
        if page_key == last_page_key:
            # Always re-crawl last page; daily run_key re-fires even after materialization.
            run_requests.append(RunRequest(
                run_key=f"page-{page_key}-{today}",
                partition_key=page_key,
            ))
        elif page_key not in materialized:
            # Historical page: stable run_key fires exactly once.
            run_requests.append(RunRequest(
                run_key=f"page-{page_key}",
                partition_key=page_key,
            ))

    if not run_requests:
        return SensorResult(
            skip_reason="All pages materialized and last page already crawled today."
        )

    context.log.info(
        f"Queuing {len(run_requests)} run(s): {[r.partition_key for r in run_requests]}"
    )
    return SensorResult(run_requests=run_requests)


defs = Definitions(
    assets=[voz_page_posts_assets],
    jobs=[crawl_page_job, discover_pages_job],
    sensors=[voz_discover_sensor, voz_crawl_sensor],
    resources={
        "dagster_dlt": DagsterDltResource(),
        "postgres": PostgresResource(
            user=EnvVar("POSTGRES_USER"),
            password=EnvVar("POSTGRES_PASSWORD"),
            host=EnvVar("POSTGRES_HOST"),
            port=EnvVar.int("POSTGRES_PORT"),
            db=EnvVar("POSTGRES_DB"),
        ),
        "crawler": CrawlerResource(
            thread_url=EnvVar("VOZ_THREAD_URL"),
            flaresolverr_url=EnvVar("FLARESOLVERR_URL"),
            http_delay_seconds=EnvVar("HTTP_DELAY_SECONDS"),
            http_timeout_seconds=EnvVar.int("HTTP_TIMEOUT_SECONDS"),
        ),
    },
)
