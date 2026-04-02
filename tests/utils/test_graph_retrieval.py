from unittest.mock import MagicMock

from voz_crawler.utils.graph_retrieval import get_thread_context


def _make_arango_db(posts):
    db = MagicMock()
    cursor = MagicMock()
    cursor.__iter__ = MagicMock(return_value=iter(posts))
    db.aql.execute.return_value = cursor
    return db


def test_returns_posts_sorted_by_time():
    posts = [
        {"post_id": 2, "posted_at": "2024-01-01T12:00:00Z"},
        {"post_id": 1, "posted_at": "2024-01-01T10:00:00Z"},
    ]
    db = _make_arango_db(posts)
    result = get_thread_context(post_id=1, arango_db=db)
    assert [p["post_id"] for p in result] == [1, 2]


def test_isolated_node_returns_single_post():
    posts = [{"post_id": 42, "posted_at": "2024-01-01T10:00:00Z"}]
    db = _make_arango_db(posts)
    result = get_thread_context(post_id=42, arango_db=db)
    assert len(result) == 1
    assert result[0]["post_id"] == 42


def test_empty_graph_returns_empty():
    db = _make_arango_db([])
    result = get_thread_context(post_id=99, arango_db=db)
    assert result == []


def test_passes_max_depth_to_aql():
    db = _make_arango_db([])
    get_thread_context(post_id=1, arango_db=db, max_depth=5)
    _, kwargs = db.aql.execute.call_args
    assert kwargs["bind_vars"]["max_depth"] == 5
