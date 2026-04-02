from dagster import Definitions, EnvVar
from dagster_dlt import DagsterDltResource

from .assets.ingestion import voz_page_posts_assets
from .assets.reply_graph import extract_explicit_edges, sync_posts_to_arango
from .jobs.crawl import crawl_page_job, discover_pages_job
from .resources.arango import ArangoResource
from .resources.crawler import CrawlerResource
from .resources.postgres import PostgresResource
from .sensors.crawl import voz_crawl_sensor, voz_discover_sensor

postgres_resource = PostgresResource(
    user=EnvVar("POSTGRES_USER"),
    password=EnvVar("POSTGRES_PASSWORD"),
    host=EnvVar("POSTGRES_HOST"),
    port=EnvVar.int("POSTGRES_PORT"),
    db=EnvVar("POSTGRES_DB"),
)

crawler_resource = CrawlerResource(
    thread_url=EnvVar("VOZ_THREAD_URL"),
    flaresolverr_url=EnvVar("FLARESOLVERR_URL"),
    http_delay_seconds=EnvVar("HTTP_DELAY_SECONDS"),
    http_timeout_seconds=EnvVar.int("HTTP_TIMEOUT_SECONDS"),
)

arango_resource = ArangoResource(
    host=EnvVar("ARANGO_HOST"),
    port=EnvVar.int("ARANGO_PORT"),
    password=EnvVar("ARANGO_ROOT_PASSWORD"),
    db=EnvVar("ARANGO_DB"),
)

defs = Definitions(
    assets=[voz_page_posts_assets, sync_posts_to_arango, extract_explicit_edges],
    jobs=[crawl_page_job, discover_pages_job],
    sensors=[voz_discover_sensor, voz_crawl_sensor],
    resources={
        "dagster_dlt": DagsterDltResource(),
        "postgres": postgres_resource,
        "crawler": crawler_resource,
        "arango": arango_resource,
    },
)
