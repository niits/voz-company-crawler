from bs4 import BeautifulSoup

from voz_crawler.core.entities.parsed_post import ParsedPost


def extract_posts(html: str) -> list[ParsedPost]:
    """Parse XenForo thread page HTML and extract raw post records.

    XenForo post structure:
      <article class="message" data-author="username" id="js-post-XXXXX">
        <div class="message-userDetails">...</div>
        <div class="message-attribution">
          <time class="u-dt" datetime="2024-01-15T10:30:00+0700">...</time>
        </div>
        <div class="message-body">
          <div class="bbWrapper">...post content...</div>
        </div>
      </article>

    Returns a list of dicts matching the raw.posts schema (minus DB-managed fields).
    """
    soup = BeautifulSoup(html, "lxml")
    posts = []

    for position, article in enumerate(soup.select("article.message[data-author]"), start=1):
        post_id = _extract_post_id(article)
        author_username = article.get("data-author", "").strip() or None
        author_id = _extract_author_id(article)
        posted_at_raw = _extract_posted_at(article)
        raw_content_html, raw_content_text = _extract_body(article)

        posts.append(
            ParsedPost(
                post_id_on_site=post_id,
                post_position=position,
                author_username=author_username,
                author_id_on_site=author_id,
                posted_at_raw=posted_at_raw,
                raw_content_html=raw_content_html,
                raw_content_text=raw_content_text,
            )
        )

    return posts


def _extract_post_id(article) -> str | None:
    """Extract Voz post ID from element id like 'js-post-12345678'."""
    elem_id = article.get("id", "")
    if elem_id.startswith("js-post-"):
        return elem_id[len("js-post-") :]
    return None


def _extract_author_id(article) -> str | None:
    """Extract author's user ID from profile link href."""
    link = article.select_one("a.username[data-user-id]")
    if link:
        return link.get("data-user-id")
    return None


def _extract_posted_at(article) -> str | None:
    """Extract raw datetime string from <time class='u-dt' datetime='...'>."""
    time_tag = article.select_one("time.u-dt")
    if time_tag:
        return time_tag.get("datetime") or time_tag.get_text(strip=True) or None
    return None


def _extract_body(article) -> tuple[str, str]:
    """Extract post body as raw HTML and plain text."""
    body = article.select_one("div.bbWrapper")
    if not body:
        return "", ""
    raw_html = str(body)
    raw_text = body.get_text(separator="\n", strip=True)
    return raw_html, raw_text
