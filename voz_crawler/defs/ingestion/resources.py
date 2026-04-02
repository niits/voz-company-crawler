from dagster import ConfigurableResource, EnvVar


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
