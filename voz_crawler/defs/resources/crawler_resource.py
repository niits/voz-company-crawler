from dagster import ConfigurableResource


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
