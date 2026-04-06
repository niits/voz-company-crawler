from voz_crawler.core.entities.graph import GraphEdge
from voz_crawler.core.entities.raw_post import RawPost
from voz_crawler.core.graph.quote_parser import extract_quote_edges


def build_edges(rows: list[RawPost], partition_key: str) -> list[GraphEdge]:
    """Parse blockquotes from each post's HTML and return edge entities tagged with partition_key."""
    edges: list[GraphEdge] = []
    for r in rows:
        edges.extend(
            extract_quote_edges(r.raw_content_html or "", r.post_id_on_site, partition_key)
        )
    return edges
