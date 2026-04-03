from sqlalchemy import create_engine, text

from voz_crawler.core.graph.quote_parser import extract_quote_edges


def fetch_posts_with_blockquotes(engine_url: str, page_url: str) -> list:
    """Query raw.voz__posts for posts that contain XenForo quote blocks."""
    engine = create_engine(engine_url)
    try:
        with engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT post_id_on_site, raw_content_html"
                    " FROM raw.voz__posts"
                    " WHERE page_url = :page_url"
                    "   AND raw_content_html LIKE '%blockquote%'"
                    " ORDER BY post_id_on_site"
                ),
                {"page_url": page_url},
            ).fetchall()
    finally:
        engine.dispose()


def drop_partition_edges(db, partition_key: str) -> int:
    """Delete all quotes edges for a partition. Returns the count of dropped edges."""
    result = db.aql.execute(
        "FOR e IN quotes FILTER e.partition_key == @pk REMOVE e IN quotes RETURN 1",
        bind_vars={"pk": partition_key},
    )
    return sum(1 for _ in result)


def build_edges(rows: list, partition_key: str) -> list[dict]:
    """Parse blockquotes from each post's HTML and return edge dicts tagged with partition_key."""
    edges = []
    for r in rows:
        for edge in extract_quote_edges(r.raw_content_html, r.post_id_on_site):
            edge["partition_key"] = partition_key
            edges.append(edge)
    return edges


def insert_edges(quotes_col, edges: list[dict]) -> None:
    """Bulk-insert quote edges. Ignores duplicates (edge relationships are immutable)."""
    if edges:
        quotes_col.import_bulk(edges, on_duplicate="ignore")
