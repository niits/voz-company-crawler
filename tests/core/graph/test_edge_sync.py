"""Tests for voz_crawler.core.graph.edge_sync.build_edges.

Pure function — delegates HTML parsing to quote_parser (tested separately).
Tests here verify the wiring: RawPost list → GraphEdge list.
"""

from voz_crawler.core.entities.raw_post import RawPost
from voz_crawler.core.graph.edge_sync import build_edges

PARTITION_KEY = "test:fixtures"

BLOCKQUOTE_HTML = (
    '<div class="bbWrapper">'
    '<blockquote class="bbCodeBlock--quote" data-source="post: 2000">'
    "quoted text"
    "</blockquote>"
    "reply body"
    "</div>"
)


def _post(post_id: int, html: str | None) -> RawPost:
    return RawPost(
        post_id_on_site=post_id,
        author_username=None,
        author_id_on_site=None,
        posted_at_raw=None,
        raw_content_text=None,
        raw_content_html=html,
    )


def test_empty_rows_returns_no_edges():
    assert build_edges([], PARTITION_KEY) == []


def test_post_without_blockquote_returns_no_edges():
    assert build_edges([_post(1001, "<div>plain text</div>")], PARTITION_KEY) == []


def test_post_with_none_html_returns_no_edges():
    assert build_edges([_post(1001, None)], PARTITION_KEY) == []


def test_post_with_blockquote_returns_one_edge():
    edges = build_edges([_post(1002, BLOCKQUOTE_HTML)], PARTITION_KEY)

    assert len(edges) == 1
    assert edges[0].from_post_id == 1002
    assert edges[0].to_post_id == 2000
    assert edges[0].partition_key == PARTITION_KEY


def test_multiple_posts_edges_are_concatenated():
    html_two_quotes = (
        '<blockquote class="bbCodeBlock--quote" data-source="post: 100">q1</blockquote>'
        '<blockquote class="bbCodeBlock--quote" data-source="post: 200">q2</blockquote>'
    )
    rows = [
        _post(1001, "<div>no quote</div>"),
        _post(1002, BLOCKQUOTE_HTML),
        _post(1003, html_two_quotes),
    ]
    edges = build_edges(rows, PARTITION_KEY)

    assert len(edges) == 3  # 0 + 1 + 2
    from_ids = {e.from_post_id for e in edges}
    assert 1001 not in from_ids
    assert 1002 in from_ids
    assert 1003 in from_ids
