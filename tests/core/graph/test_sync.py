from types import SimpleNamespace
from unittest.mock import MagicMock, call

from voz_crawler.core.graph.sync import GraphSyncService


def _make_row(**kwargs):
    return SimpleNamespace(**kwargs)


def _make_service(aql_results=None, post_rows=None, html_rows=None):
    """Helper: build a GraphSyncService with fully mocked dependencies."""
    arango_db = MagicMock()
    posts_repo = MagicMock()

    # Default AQL cursor returns 0 (no existing data)
    aql_cursor = MagicMock()
    aql_cursor.__iter__ = MagicMock(return_value=iter(aql_results or [0]))
    arango_db.aql.execute.return_value = aql_cursor

    posts_repo.fetch_new_posts.return_value = post_rows or []
    posts_repo.fetch_html_for_posts.return_value = html_rows or []

    return GraphSyncService(arango_db=arango_db, posts_repo=posts_repo), arango_db, posts_repo


def test_sync_posts_returns_zero_stats_when_no_new_posts():
    svc, _, _ = _make_service(post_rows=[])
    stats = svc.sync_posts()
    assert stats["records_processed"] == 0


def test_sync_posts_inserts_each_row():
    rows = [
        _make_row(
            post_id_on_site=101, author_username="alice", author_id_on_site="1",
            posted_at_raw="2024-01-01T10:00:00", raw_content_text="hello",
        ),
        _make_row(
            post_id_on_site=102, author_username="bob", author_id_on_site="2",
            posted_at_raw="2024-01-01T11:00:00", raw_content_text="world",
        ),
    ]
    svc, arango_db, _ = _make_service(post_rows=rows)
    stats = svc.sync_posts()
    assert stats["records_processed"] == 2
    assert stats["last_processed_post_id"] == 102
    posts_col = arango_db.collection("Posts")
    assert posts_col.insert.call_count == 2


def test_sync_posts_uses_max_id_as_cursor():
    svc, arango_db, posts_repo = _make_service(aql_results=[55], post_rows=[])
    svc.sync_posts()
    posts_repo.fetch_new_posts.assert_called_once_with(after_post_id=55)


def test_extract_explicit_edges_upserts_quotes():
    html = (
        '<article class="message" data-author="alice" id="js-post-200">'
        '<div class="message-body"><div class="bbWrapper">'
        '<blockquote data-source="post: 100" data-quote="bob">q</blockquote>'
        '</div></div></article>'
    )
    rows = [_make_row(post_id_on_site=200, raw_content_html=html)]
    svc, arango_db, _ = _make_service(html_rows=rows)
    stats = svc.extract_explicit_edges()
    assert stats["edges_processed"] == 1
    quotes_col = arango_db.collection("quotes")
    quotes_col.insert.assert_called_once()
    doc = quotes_col.insert.call_args[0][0]
    assert doc["_from"] == "Posts/200"
    assert doc["_to"] == "Posts/100"
    assert doc["method"] == "html_metadata"
    assert doc["confidence"] == 1.0


def test_extract_explicit_edges_returns_zero_for_no_quotes():
    rows = [_make_row(post_id_on_site=300, raw_content_html="<p>no quotes</p>")]
    svc, _, _ = _make_service(html_rows=rows)
    stats = svc.extract_explicit_edges()
    assert stats["edges_processed"] == 0
