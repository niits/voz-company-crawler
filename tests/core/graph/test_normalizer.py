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


def test_quote_chrome_is_stripped_from_quoted_text():
    """Regression test: XenForo's "<author> said:" attribution link and
    "Click to expand..." trigger were leaking into quoted_blocks[].text
    because it was captured via blockquote.get_text() on the whole tag.

    Real markup sampled from raw.posts (post_id_on_site=39964595).
    """
    html = (
        '<div class="bbWrapper">'
        '<blockquote class="bbCodeBlock bbCodeBlock--expandable bbCodeBlock--quote js-expandWatch" '
        'data-attributes="member: 1676001" data-quote="duonga1ne1" data-source="post: 39963697">'
        '<div class="bbCodeBlock-title">'
        '<a class="bbCodeBlock-sourceJump" data-content-selector="#post-39963697" '
        'data-xf-click="attribution" href="/goto/post?id=39963697" rel="nofollow">'
        "duonga1ne1 said:</a>"
        "</div>"
        '<div class="bbCodeBlock-content">'
        '<div class="bbCodeBlock-expandContent js-expandContent">'
        "Bác pv role gì đấy, đợt e pv bên mảng data với devops"
        "</div>"
        '<div class="bbCodeBlock-expandLink js-expandLink">'
        '<a role="button" tabindex="0">Click to expand...</a></div>'
        "</div>"
        "</blockquote>"
        "họ build platform gì thế ạ</div>"
    )
    result = normalize_post_html(html)

    assert (
        result["quoted_blocks"][0]["text"]
        == "Bác pv role gì đấy, đợt e pv bên mảng data với devops"
    )
    assert "said:" not in result["quoted_blocks"][0]["text"]
    assert "Click to expand" not in result["quoted_blocks"][0]["text"]
    assert result["own_text"] == "họ build platform gì thế ạ"


def test_spoiler_toggle_button_is_stripped():
    """Regression test: the "Spoiler: <title>" toggle button's text was
    leaking into own_text since nothing stripped .bbCodeSpoiler-button.

    Real markup sampled from raw.posts (12 posts contain a spoiler block).
    """
    html = (
        '<div class="bbWrapper">nhờ các bác recommend'
        '<div class="bbCodeSpoiler">'
        '<button class="button bbCodeSpoiler-button button--longText" '
        'data-original-title="Click to reveal or hide spoiler" data-xf-click="toggle" '
        'type="button">'
        '<span class="button-text"><span>Spoiler: '
        '<span class="bbCodeSpoiler-button-title">uncen</span></span></span>'
        "</button>"
        '<div class="bbCodeSpoiler-content">'
        '<div class="bbCodeBlock bbCodeBlock--spoiler">'
        '<div class="bbCodeBlock-content">Em năm 3 nha các bác</div>'
        "</div></div></div></div>"
    )
    result = normalize_post_html(html)

    assert result["own_text"] == "nhờ các bác recommend Em năm 3 nha các bác"
    assert "Spoiler" not in result["own_text"]


def test_code_block_language_label_is_stripped():
    """Regression test: .bbCodeBlock-title is reused outside <blockquote> as
    a code-block language label (e.g. "PHP:"), which also leaked into text.
    """
    html = (
        '<div class="bbWrapper">'
        '<div class="bbCodeBlock bbCodeBlock--screenLimited bbCodeBlock--code">'
        '<div class="bbCodeBlock-title">PHP:</div>'
        '<div class="bbCodeBlock-content" dir="ltr">'
        '<pre class="bbCodeCode language-php"><code class="language-php">echo 1;</code></pre>'
        "</div></div></div>"
    )
    result = normalize_post_html(html)

    assert "PHP:" not in result["own_text"]
    assert "echo 1;" in result["own_text"]


def test_smilie_images_are_stripped():
    html = '<div>lol <img class="smilie" src="x.png">funny</div>'
    result = normalize_post_html(html)
    assert result["own_text"] == "lol funny"


def test_emote_codes_are_removed():
    html = "<div>nice :sweat: work</div>"
    result = normalize_post_html(html)
    assert result["own_text"] == "nice work"
