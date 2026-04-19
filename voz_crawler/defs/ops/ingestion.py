from dagster import DynamicPartitionsDefinition, OpExecutionContext, op

from voz_crawler.core.ingestion.html_source.pagination import (
    build_page_url,
    discover_total_pages,
)
from voz_crawler.defs.resources.crawler_resource import CrawlerResource
from voz_crawler.dlt.sources.voz_thread import fetch_via_flaresolverr, is_cf_block


def build_discover_op(thread_id: str, partitions_def: DynamicPartitionsDefinition):
    @op(name=f"discover_pages_op_{thread_id}")
    def discover_pages_op(context: OpExecutionContext, crawler: CrawlerResource) -> int:
        thread_url = crawler.url_for_thread(thread_id)
        page1_url = build_page_url(thread_url, 1)
        status_code, html = fetch_via_flaresolverr(
            page1_url, crawler.flaresolverr_url, timeout=crawler.http_timeout_seconds
        )
        if is_cf_block(status_code, html):
            raise RuntimeError(f"Cloudflare blocked page 1 discovery (HTTP {status_code}).")

        total_pages = discover_total_pages(html)
        context.log.info(f"[{thread_id}] Discovered {total_pages} total pages.")

        partition_name = partitions_def.name
        existing_keys: set[str] = set(context.instance.get_dynamic_partitions(partition_name))
        new_keys = [
            f"{thread_id}:{p}"
            for p in range(1, total_pages + 1)
            if f"{thread_id}:{p}" not in existing_keys
        ]
        if new_keys:
            context.instance.add_dynamic_partitions(partition_name, new_keys)
            preview = new_keys[:5] + (["..."] if len(new_keys) > 5 else [])
            context.log.info(f"[{thread_id}] Added {len(new_keys)} new partition(s): {preview}")
        else:
            context.log.info(f"[{thread_id}] No new pages to register.")

        return total_pages

    return discover_pages_op
