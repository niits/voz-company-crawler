"""Tests for voz_crawler.core.graph.normalizer.normalize_post_html.

Pure function — no external services, no mocking required.
"""

from voz_crawler.core.graph.normalizer import normalize_post_html


def test_empty_html_returns_empty_own_text():
    result = normalize_post_html("")
    assert result == {"own_text": "", "quoted_blocks": []}


def test_plain_text_with_no_quotes():
    result = normalize_post_html("<div>hello world</div>")
    assert result["own_text"] == "hello world"
    assert result["quoted_blocks"] == []


def test_single_quote_is_extracted_and_removed_from_own_text():
    html = (
        '<div><blockquote data-source="post: 100" data-quote="alice">quoted</blockquote>'
        "my reply</div>"
    )
    result = normalize_post_html(html)

    assert result["own_text"] == "my reply"
    assert len(result["quoted_blocks"]) == 1
    assert result["quoted_blocks"][0] == {
        "author": "alice",
        "source_post_id": 100,
        "text": "quoted",
    }


def test_nested_quote_does_not_raise():
    """Regression test: a quote-of-a-quote used to crash normalize_post_html.

    bs4's select() resolves matches upfront, so a nested <blockquote> is
    already in the result list when the outer one gets decompose()'d,
    tearing down the nested tag's attrs before it's visited.
    """
    html = (
        "<div>"
        '<blockquote data-source="post: 1" data-quote="A">'
        "outer text"
        '<blockquote data-source="post: 2" data-quote="B">inner text</blockquote>'
        "more outer"
        "</blockquote>"
        "after quote"
        "</div>"
    )
    result = normalize_post_html(html)

    assert result["own_text"] == "after quote"
    assert len(result["quoted_blocks"]) == 1
    assert result["quoted_blocks"][0]["author"] == "A"
    assert result["quoted_blocks"][0]["source_post_id"] == 1
    assert "inner text" in result["quoted_blocks"][0]["text"]


def test_smilie_images_are_stripped():
    html = '<div>lol <img class="smilie" src="x.png">funny</div>'
    result = normalize_post_html(html)
    assert result["own_text"] == "lol funny"


def test_emote_codes_are_removed():
    html = "<div>nice :sweat: work</div>"
    result = normalize_post_html(html)
    assert result["own_text"] == "nice work"
