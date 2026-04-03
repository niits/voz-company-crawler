"""
dlt source for a single Voz.vn thread page.

Data flow:
  voz_page_source(page_url)
      └─► voz_page_posts (resource)
              yields one dict per forum post (global merge by post_id_on_site)

No cursor or incremental logic — idempotency is handled by:
  write_disposition="merge" + primary_key="post_id_on_site"

Orchestration (which pages to crawl, when to re-crawl the last page) is owned
by the Dagster sensor in definitions.py via DynamicPartitionsDefinition.

HTTP fetching is delegated to FlareSolverr, which runs a real Chrome browser to
bypass Cloudflare challenges. FlareSolverr must be reachable at FLARESOLVERR_URL.
"""

import dlt
import requests

from voz_crawler.core.ingestion.html_source.html_parser import extract_posts

_CF_BLOCK_MARKERS = ("Just a moment", "cf-browser-verification")
_CF_BLOCK_STATUSES = (403, 429, 503)


def fetch_via_flaresolverr(url: str, flaresolverr_url: str, timeout: int = 60) -> tuple[int, str]:
    """Fetch a URL through FlareSolverr. Returns (status_code, html)."""
    payload = {
        "cmd": "request.get",
        "url": url,
        "maxTimeout": timeout * 1000,
    }
    r = requests.post(
        f"{flaresolverr_url.rstrip('/')}/v1",
        json=payload,
        timeout=timeout + 10,
    )
    r.raise_for_status()
    data = r.json()
    if data["status"] != "ok":
        raise RuntimeError(f"FlareSolverr error: {data.get('message', data)}")
    solution = data["solution"]
    return solution["status"], solution["response"]


def is_cf_block(status_code: int, html: str) -> bool:
    if status_code in _CF_BLOCK_STATUSES:
        snippet = html[:2000]
        return any(m in snippet for m in _CF_BLOCK_MARKERS)
    return False


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
    """Fetch a single Voz thread page via FlareSolverr and yield all post records.

    Cloudflare block raises RuntimeError, bubbling up to Dagster as a run failure.
    """
    status_code, html = fetch_via_flaresolverr(
        page_url, flaresolverr_url, timeout=http_timeout_seconds
    )
    if is_cf_block(status_code, html):
        raise RuntimeError(f"Cloudflare blocked {page_url} (HTTP {status_code}). Try again later.")

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
