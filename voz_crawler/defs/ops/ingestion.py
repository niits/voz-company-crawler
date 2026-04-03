from dagster import OpExecutionContext, op

from voz_crawler.core.ingestion.html_source.pagination import build_page_url, discover_total_pages
from voz_crawler.defs.resources.crawler_resource import CrawlerResource
from voz_crawler.dlt.sources.voz_thread import fetch_via_flaresolverr, is_cf_block


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
