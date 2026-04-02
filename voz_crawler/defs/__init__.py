from dagster import Definitions
from dagster_dlt import DagsterDltResource

from .ingestion.assets import voz_page_posts_assets
from .ingestion.jobs import crawl_page_job, discover_pages_job
from .ingestion.resources import crawler_resource, postgres_resource
from .ingestion.sensors import voz_crawl_sensor, voz_discover_sensor

defs = Definitions(
    assets=[voz_page_posts_assets],
    jobs=[crawl_page_job, discover_pages_job],
    sensors=[voz_discover_sensor, voz_crawl_sensor],
    resources={
        "dagster_dlt": DagsterDltResource(),
        "postgres": postgres_resource,
        "crawler": crawler_resource,
    },
)
