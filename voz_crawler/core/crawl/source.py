"""dlt source for a single Voz.vn thread page.

Data flow:
  voz_page_source(page_url)
      └─► voz_page_posts (resource)
              yields one dict per forum post (global merge by post_id_on_site)

No cursor or incremental logic — idempotency is handled by:
  write_disposition="merge" + primary_key="post_id_on_site"
"""

import dlt

from voz_crawler.core.crawl.fetcher import Fetcher
from voz_crawler.core.crawl.parser import extract_posts


@dlt.resource(
    name="posts",
    write_disposition="merge",
    primary_key="post_id_on_site",
)
def voz_page_posts(
    page_url: str,
    flaresolverr_url: str,
    http_timeout_seconds: int = 60,
) -> dlt.sources.TDataItems:
    """Fetch a single Voz thread page via FlareSolverr and yield all post records."""
    fetcher = Fetcher(flaresolverr_url=flaresolverr_url, timeout=http_timeout_seconds)
    status_code, html = fetcher.fetch(page_url)
    if Fetcher.is_cf_block(status_code, html):
        raise RuntimeError(
            f"Cloudflare blocked {page_url} (HTTP {status_code}). Try again later."
        )
    for post in extract_posts(html):
        if not post["post_id_on_site"]:
            continue
        try:
            post_id_int = int(post["post_id_on_site"])
        except (ValueError, TypeError):
            continue
        yield {
            "post_id_on_site": post_id_int,
            "page_url": page_url,
            "author_username": post["author_username"],
            "author_id_on_site": post["author_id_on_site"],
            "posted_at_raw": post["posted_at_raw"],
            "raw_content_html": post["raw_content_html"],
            "raw_content_text": post["raw_content_text"],
        }


@dlt.source(name="voz")
def voz_page_source(
    page_url: str,
    flaresolverr_url: str,
    http_timeout_seconds: int = 60,
) -> dlt.sources.TDataItems:
    """dlt source for a single Voz.vn thread page."""
    return voz_page_posts(
        page_url=page_url,
        flaresolverr_url=flaresolverr_url,
        http_timeout_seconds=http_timeout_seconds,
    )
