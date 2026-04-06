from bs4 import BeautifulSoup

from voz_crawler.core.entities.graph import GraphEdge


def extract_quote_edges(html: str, source_post_id: int, partition_key: str) -> list[GraphEdge]:
    """Parse XenForo HTML for explicit quote blocks and return edge entities.

    XenForo renders quoted posts as:
      <blockquote class="bbCodeBlock--quote" data-source="post: 21997699" ...>

    The quoting post is ``source_post_id``; the quoted post is the ID in data-source.
    """
    soup = BeautifulSoup(html, "lxml")
    edges: list[GraphEdge] = []
    for ordinal, blockquote in enumerate(soup.select("blockquote[data-source]"), start=1):
        raw = blockquote.get("data-source", "")  # e.g. "post: 21997699"
        if not raw.startswith("post: "):
            continue
        try:
            target_id = int(raw[len("post: ") :].strip())
        except ValueError:
            continue
        edges.append(
            GraphEdge(
                from_post_id=source_post_id,
                to_post_id=target_id,
                edge_key=f"{source_post_id}_{target_id}_{ordinal}",
                quote_ordinal=ordinal,
                confidence=1.0,
                method="html_metadata",
                partition_key=partition_key,
            )
        )
    return edges
