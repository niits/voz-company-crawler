import os

from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
    DynamicPartitionsDefinition,
    EnvVar,
)
from dagster_dlt import DagsterDltResource
from dagster_openai import OpenAIResource

from voz_crawler.core.ingestion.html_source.pagination import extract_thread_id
from voz_crawler.defs.assets.ingestion import build_ingestion_assets
from voz_crawler.defs.assets.reply_graph import build_thread_assets
from voz_crawler.defs.jobs.ingestion import build_ingestion_jobs
from voz_crawler.defs.jobs.reply_graph import build_thread_jobs
from voz_crawler.defs.ops.ingestion import build_discover_op
from voz_crawler.defs.resources.arango_resource import ArangoDBResource
from voz_crawler.defs.resources.crawler_resource import CrawlerResource
from voz_crawler.defs.resources.postgres_resource import PostgresResource
from voz_crawler.defs.sensors.ingestion import build_ingestion_sensors
from voz_crawler.defs.sensors.reply_graph import build_implicit_sensor


def _build_thread_pipeline(thread_id: str) -> tuple[list, list, list]:
    """Builds all assets, jobs, and sensors for one thread URL.

    Each thread gets an isolated DynamicPartitionsDefinition so that
    partition registration, AutomationCondition evaluation, and job/sensor
    triggering are fully independent between threads.
    """
    partitions_def = DynamicPartitionsDefinition(name=f"voz_pages_{thread_id}")

    # ── Ingestion layer ──────────────────────────────────────────────────────
    ingestion_assets = build_ingestion_assets(thread_id, partitions_def)
    (posts_asset_key,) = ingestion_assets.keys

    discover_op = build_discover_op(thread_id, partitions_def)
    crawl_job, discover_job = build_ingestion_jobs(
        thread_id, partitions_def, posts_asset_key, discover_op
    )
    discover_sensor, crawl_sensor = build_ingestion_sensors(
        thread_id, discover_job, crawl_job, partitions_def, posts_asset_key
    )

    # ── Reply graph layer ────────────────────────────────────────────────────
    reply_assets = build_thread_assets(thread_id, partitions_def, posts_asset_key)
    reply_graph_job, implicit_job = build_thread_jobs(thread_id, partitions_def, posts_asset_key)
    implicit_sensor = build_implicit_sensor(thread_id, implicit_job, partitions_def)

    return (
        [ingestion_assets, *reply_assets],
        [discover_job, crawl_job, reply_graph_job, implicit_job],
        [discover_sensor, crawl_sensor, implicit_sensor],
    )


# ── Resolve thread IDs from env at load time ─────────────────────────────────
# os.environ is used directly (not EnvVar) because asset and job definitions
# must be built statically — EnvVar is lazy and resolves only at run time.
_thread_urls = [u.strip() for u in os.environ.get("VOZ_THREAD_URLS", "").split(",") if u.strip()]
_thread_ids = [extract_thread_id(url) for url in _thread_urls]

_all_assets: list = []
_all_jobs: list = []
_all_sensors: list = []

for _thread_id in _thread_ids:
    _assets, _jobs, _sensors = _build_thread_pipeline(_thread_id)
    _all_assets.extend(_assets)
    _all_jobs.extend(_jobs)
    _all_sensors.extend(_sensors)

defs = Definitions(
    assets=_all_assets,
    jobs=_all_jobs,
    sensors=[
        *_all_sensors,
        AutomationConditionSensorDefinition(
            "default_automation_condition_sensor",
            target=AssetSelection.all(),
            default_status=DefaultSensorStatus.RUNNING,
        ),
    ],
    resources={
        "dagster_dlt": DagsterDltResource(),
        "postgres": PostgresResource(
            user=EnvVar("POSTGRES_USER"),
            password=EnvVar("POSTGRES_PASSWORD"),
            host=EnvVar("POSTGRES_HOST"),
            port=EnvVar.int("POSTGRES_PORT"),
            db=EnvVar("POSTGRES_DB"),
            raw_schema=EnvVar("PG_RAW_SCHEMA"),
            posts_table=EnvVar("PG_POSTS_TABLE"),
        ),
        "crawler": CrawlerResource(
            thread_urls=EnvVar("VOZ_THREAD_URLS"),
            flaresolverr_url=EnvVar("FLARESOLVERR_URL"),
            http_delay_seconds=EnvVar("HTTP_DELAY_SECONDS"),
            http_timeout_seconds=EnvVar.int("HTTP_TIMEOUT_SECONDS"),
        ),
        "openai": OpenAIResource(api_key=EnvVar("OPENAI_API_KEY")),
        "arango": ArangoDBResource(
            host=EnvVar("ARANGO_HOST"),
            port=EnvVar.int("ARANGO_PORT"),
            db=EnvVar("ARANGO_DB"),
            username=EnvVar("ARANGO_USER"),
            password=EnvVar("ARANGO_PASSWORD"),
        ),
    },
)
