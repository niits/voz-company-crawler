from dagster import ConfigurableResource

from voz_crawler.core.ingestion.html_source.pagination import extract_thread_id


class CrawlerResource(ConfigurableResource):
    """Runtime configuration for the Voz thread crawler.

    `thread_urls` is a comma-separated list of Voz thread URLs.
    Multiple threads are supported; the crawler discovers and crawls all of them.

    `http_delay_seconds` is retained for environment variable compatibility
    but is no longer used by the asset function (each run fetches one page).
    The `delay` property is kept for backwards compatibility.
    """

    thread_urls: str  # comma-separated list of thread URLs
    flaresolverr_url: str
    http_delay_seconds: str
    http_timeout_seconds: int

    def get_thread_urls(self) -> list[str]:
        return [u.strip() for u in self.thread_urls.split(",") if u.strip()]

    def thread_url_map(self) -> dict[str, str]:
        """Returns {thread_id: thread_url} for all configured threads."""
        return {extract_thread_id(url): url for url in self.get_thread_urls()}

    def url_for_thread(self, thread_id: str) -> str:
        """Resolve the thread URL for a given thread_id."""
        mapping = self.thread_url_map()
        if thread_id not in mapping:
            raise ValueError(f"No thread URL configured for thread_id={thread_id!r}")
        return mapping[thread_id]

    def url_for_partition(self, partition_key: str) -> str:
        """Resolve the thread URL for a given partition key ({thread_id}:{page})."""
        thread_id = partition_key.rsplit(":", 1)[0]
        return self.url_for_thread(thread_id)

    @property
    def delay(self) -> float:
        return float(self.http_delay_seconds)
