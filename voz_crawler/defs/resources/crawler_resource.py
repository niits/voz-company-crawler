from dagster import ConfigurableResource

from voz_crawler.core.ingestion.html_source.pagination import extract_thread_id


class CrawlerResource(ConfigurableResource):
    """Runtime configuration for the Voz thread crawler.

    `thread_urls` is a comma-separated list of full thread URLs — one per thread.
    `http_delay_seconds` is retained for environment variable compatibility only.
    """

    thread_urls: str
    flaresolverr_url: str
    http_delay_seconds: str
    http_timeout_seconds: int

    def _url_map(self) -> dict[str, str]:
        return {extract_thread_id(u): u.strip() for u in self.thread_urls.split(",") if u.strip()}

    def url_for_thread(self, thread_id: str) -> str:
        return self._url_map()[thread_id]

    def url_for_partition(self, partition_key: str) -> str:
        thread_id, _ = partition_key.rsplit(":", 1)
        return self._url_map()[thread_id]
