from unittest.mock import MagicMock

from voz_crawler.core.graph.schema import ensure_schema


def _make_mock_db(has_collections=(), has_graph=False, post_count=200):
    db = MagicMock()
    db.has_collection.side_effect = lambda name: name in has_collections
    db.has_graph.return_value = has_graph
    posts_col = MagicMock()
    posts_col.indexes.return_value = []
    posts_col.count.return_value = post_count
    db.collection.return_value = posts_col
    return db


def test_creates_all_collections_on_empty_db():
    db = _make_mock_db()
    ensure_schema(db)
    db.create_collection.assert_any_call("Posts")
    db.create_collection.assert_any_call("quotes", edge=True)
    db.create_collection.assert_any_call("implicit_replies", edge=True)


def test_creates_named_graph():
    db = _make_mock_db()
    ensure_schema(db)
    db.create_graph.assert_called_once()
    args, kwargs = db.create_graph.call_args
    assert args[0] == "reply_graph"


def test_skips_existing_collections():
    db = _make_mock_db(has_collections=("Posts", "quotes", "implicit_replies"), has_graph=True)
    posts_col = MagicMock()
    posts_col.indexes.return_value = [{"type": "vector"}]
    db.collection.return_value = posts_col
    ensure_schema(db)
    db.create_collection.assert_not_called()
    db.create_graph.assert_not_called()


def test_creates_vector_index_when_missing():
    db = _make_mock_db(post_count=200)  # enough training points
    ensure_schema(db)
    posts_col = db.collection("Posts")
    posts_col.add_index.assert_called_once()
    idx_arg = posts_col.add_index.call_args[0][0]
    assert idx_arg["type"] == "vector"
    assert idx_arg["params"]["dimension"] == 1536


def test_skips_vector_index_when_too_few_posts():
    db = _make_mock_db(post_count=50)  # fewer than nLists=100
    ensure_schema(db)
    posts_col = db.collection("Posts")
    posts_col.add_index.assert_not_called()
