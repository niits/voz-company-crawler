"""Tests for voz_crawler.core.graph.quote_parser.extract_quote_edges.

Pure function — no external services, no mocking required.
"""

from voz_crawler.core.graph.quote_parser import extract_quote_edges

PARTITION_KEY = "test:fixtures"
SOURCE_POST_ID = 1002


def test_empty_html_returns_no_edges():
    assert extract_quote_edges("", SOURCE_POST_ID, PARTITION_KEY) == []


def test_html_without_blockquote_returns_no_edges():
    html = "<div>just a regular post with no quotes</div>"
    assert extract_quote_edges(html, SOURCE_POST_ID, PARTITION_KEY) == []


def test_single_valid_blockquote():
    html = (
        '<blockquote class="bbCodeBlock--quote" data-source="post: 2000">quoted text</blockquote>'
    )
    edges = extract_quote_edges(html, SOURCE_POST_ID, PARTITION_KEY)

    assert len(edges) == 1
    e = edges[0]
    assert e.from_vertex == f"posts/{SOURCE_POST_ID}"
    assert e.to_vertex == "posts/2000"
    assert e.key == f"{SOURCE_POST_ID}_2000_1"
    assert e.quote_ordinal == 1
    assert e.confidence == 1.0
    assert e.method == "html_metadata"
    assert e.partition_key == PARTITION_KEY


def test_multiple_blockquotes_ordinals_increment():
    html = (
        '<blockquote class="bbCodeBlock--quote" data-source="post: 100">q1</blockquote>'
        '<blockquote class="bbCodeBlock--quote" data-source="post: 200">q2</blockquote>'
    )
    edges = extract_quote_edges(html, SOURCE_POST_ID, PARTITION_KEY)

    assert len(edges) == 2
    assert edges[0].to_vertex == "posts/100"
    assert edges[0].quote_ordinal == 1
    assert edges[1].to_vertex == "posts/200"
    assert edges[1].quote_ordinal == 2


def test_blockquote_without_data_source_is_skipped():
    html = '<blockquote class="bbCodeBlock--quote">no data-source attr</blockquote>'
    assert extract_quote_edges(html, SOURCE_POST_ID, PARTITION_KEY) == []


def test_data_source_with_non_post_prefix_is_skipped():
    html = '<blockquote class="bbCodeBlock--quote" data-source="member: 99">text</blockquote>'
    assert extract_quote_edges(html, SOURCE_POST_ID, PARTITION_KEY) == []


def test_data_source_with_non_numeric_id_is_skipped():
    html = '<blockquote class="bbCodeBlock--quote" data-source="post: abc">text</blockquote>'
    assert extract_quote_edges(html, SOURCE_POST_ID, PARTITION_KEY) == []
