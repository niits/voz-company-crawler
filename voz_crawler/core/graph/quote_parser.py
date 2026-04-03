from bs4 import BeautifulSoup


def extract_quote_edges(html: str, source_post_id: int) -> list[dict]:
    """Parse XenForo HTML for explicit quote blocks and return edge dicts.

    XenForo renders quoted posts as:
      <blockquote class="bbCodeBlock--quote" data-source="post: 21997699" ...>

    The quoting post is `source_post_id`; the quoted post is the ID in data-source.
    Returns list of dicts ready for ArangoDB quotes collection bulk import.
    """
    soup = BeautifulSoup(html, "lxml")
    edges = []
    for ordinal, blockquote in enumerate(soup.select("blockquote[data-source]"), start=1):
        raw = blockquote.get("data-source", "")  # e.g. "post: 21997699"
        if not raw.startswith("post: "):
            continue
        try:
            target_id = int(raw[len("post: ") :].strip())
        except ValueError:
            continue
        edges.append(
            {
                "_from": f"posts/{source_post_id}",
                "_to": f"posts/{target_id}",
                "_key": f"{source_post_id}_{target_id}_{ordinal}",
                "quote_ordinal": ordinal,
                "confidence": 1.0,
                "method": "html_metadata",
            }
        )
    return edges
