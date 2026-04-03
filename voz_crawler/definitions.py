from dagster import (
    Definitions,
    EnvVar,
)
from dagster_dlt import DagsterDltResource

from voz_crawler.defs.assets.ingestion import voz_page_posts_assets
from voz_crawler.defs.jobs.ingestion import crawl_page_job, discover_pages_job
from voz_crawler.defs.resources.crawler_resource import CrawlerResource
from voz_crawler.defs.resources.postgres_resource import PostgresResource
from voz_crawler.defs.sensors.ingestion import voz_crawl_sensor, voz_discover_sensor

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
