import re

from bs4 import BeautifulSoup


def normalize_post_html(html: str) -> dict:
    """Parse XenForo post HTML and separate author's own text from quoted blocks.

    Returns:
        {
            "own_text": str,          # author's actual content; empty string if post is pure quote
            "quoted_blocks": [
                {
                    "author": str | None,           # data-quote attribute; None if unattributed
                    "source_post_id": int | None,   # data-source post id; None if unattributed
                    "text": str,
                }
            ]
        }

    Smilie image tags (<img class="smilie">) are stripped.
    Emote codes (e.g. :sweat:) are removed from own_text.
    """
    soup = BeautifulSoup(html, "lxml")
    quoted_blocks = []

    for blockquote in soup.select("blockquote[data-source]"):
        author_raw = blockquote.get("data-quote", None) or None
        source_raw = blockquote.get("data-source", "")  # e.g. "post: 21997699"
        source_post_id = None
        if source_raw.startswith("post: "):
            try:
                source_post_id = int(source_raw[len("post: ") :].strip())
            except ValueError:
                pass

        block_text = blockquote.get_text(separator=" ", strip=True)
        quoted_blocks.append(
            {
                "author": author_raw,
                "source_post_id": source_post_id,
                "text": block_text,
            }
        )
        # Remove blockquote from tree so it doesn't appear in own_text
        blockquote.decompose()

    # Strip smilie images
    for img in soup.select("img.smilie"):
        img.decompose()

    own_text = soup.get_text(separator=" ", strip=True)
    # Remove inline emote codes like :sweat: :D
    own_text = re.sub(r":[a-z_]+:", "", own_text).strip()
    # Collapse multiple whitespace
    own_text = re.sub(r"\s+", " ", own_text).strip()

    return {"own_text": own_text, "quoted_blocks": quoted_blocks}
