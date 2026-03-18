from bs4 import BeautifulSoup


def build_page_url(base_thread_url: str, page_number: int) -> str:
    """Build a XenForo thread page URL.

    XenForo URL pattern:
      page 1: https://voz.vn/t/slug.ID/
      page N: https://voz.vn/t/slug.ID/page-N
    """
    base = base_thread_url.rstrip("/")
    if page_number == 1:
        return base + "/"
    return f"{base}/page-{page_number}"


def discover_total_pages(html: str) -> int:
    """Parse XenForo pagination nav to find total page count.

    For large threads XenForo renders a limited window, e.g. "1 2 3 … 48 49 50".
    The last page always carries the CSS class `pageNav-page--last`; we check
    that first for reliability, then fall back to max() over all visible links.
    """
    soup = BeautifulSoup(html, "lxml")

    # XenForo renders pageNav as a <div class="pageNav">, not a <nav> element.
    nav = soup.select_one(".pageNav")
    if not nav:
        return 1

    # Each visible page link is in <li class="pageNav-page ..."><a>.
    # The "..." gap item has class pageNav-page--skip and its text is not a digit,
    # so filtering by isdigit() naturally excludes it.
    page_numbers = [
        int(a.get_text(strip=True))
        for a in nav.select("li.pageNav-page a")
        if a.get_text(strip=True).isdigit()
    ]
    return max(page_numbers) if page_numbers else 1
