from voz_crawler.utils.html_parser import extract_quote_edges

_SAMPLE_HTML = """
<article class="message" data-author="darkrose" id="js-post-21998630">
  <div class="message-body">
    <div class="bbWrapper">
      <blockquote class="bbCodeBlock bbCodeBlock--quote"
        data-source="post: 21997699"
        data-quote="Vipluckystar">
        quoted text
      </blockquote>
      <blockquote class="bbCodeBlock bbCodeBlock--quote"
        data-source="post: 21997700"
        data-quote="OtherUser">
        second quote
      </blockquote>
      Some reply text
    </div>
  </div>
</article>
<article class="message" data-author="plainuser" id="js-post-21998700">
  <div class="message-body">
    <div class="bbWrapper">No quotes here</div>
  </div>
</article>
"""


def test_extracts_quote_edges():
    edges = extract_quote_edges(_SAMPLE_HTML)
    assert len(edges) == 2


def test_edge_fields():
    edges = extract_quote_edges(_SAMPLE_HTML)
    first = edges[0]
    assert first["from_post_id"] == "21998630"
    assert first["to_post_id"] == "21997699"
    assert first["quote_ordinal"] == 1
    assert first["source_author"] == "darkrose"
    assert first["target_author"] == "Vipluckystar"


def test_second_quote_ordinal():
    edges = extract_quote_edges(_SAMPLE_HTML)
    assert edges[1]["quote_ordinal"] == 2
    assert edges[1]["to_post_id"] == "21997700"


def test_no_quotes_returns_empty():
    html = "<article class='message' data-author='x' id='js-post-1'></article>"
    assert extract_quote_edges(html) == []
