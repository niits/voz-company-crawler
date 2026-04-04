"""Tests for voz_crawler.core.repository.GraphRepository.

Uses a MagicMock arango db (arango_db fixture from conftest.py).
Each test section configures the mock's return values to exercise one method.
"""

from unittest.mock import call

from voz_crawler.core.entities.arango import ArangoEdge, ArangoPost, EmbedItem, EmbedPatch
from voz_crawler.core.repository.graph_repository import GraphRepository

PARTITION_KEY = "test:fixtures"


def _repo(arango_db):
    return GraphRepository(db=arango_db)


def _make_post(key: str = "1001") -> ArangoPost:
    return ArangoPost(
        key=key,
        post_id=int(key),
        author_username="user",
        author_id="1",
        posted_at="2024-01-01",
        content_text="some text",
        content_hash="abc123",
        partition_key=PARTITION_KEY,
        thread_url="https://voz.vn/t/test",
        page_number=1,
        embedding=None,
    )


def _make_edge(from_id: int = 1002, to_id: int = 2000) -> ArangoEdge:
    return ArangoEdge(
        from_vertex=f"posts/{from_id}",
        to_vertex=f"posts/{to_id}",
        key=f"{from_id}_{to_id}_1",
        quote_ordinal=1,
        confidence=1.0,
        method="html_metadata",
        partition_key=PARTITION_KEY,
    )


# ── ensure_schema ─────────────────────────────────────────────────────────────


def test_ensure_schema_creates_posts_collection(arango_db):
    _repo(arango_db).ensure_schema()
    arango_db.create_collection.assert_any_call("posts")


def test_ensure_schema_creates_quotes_edge_collection(arango_db):
    _repo(arango_db).ensure_schema()
    arango_db.create_collection.assert_any_call("quotes", edge=True)


def test_ensure_schema_creates_graph(arango_db):
    _repo(arango_db).ensure_schema()
    arango_db.create_graph.assert_called_once()
    graph_name = arango_db.create_graph.call_args[0][0]
    assert graph_name == "reply_graph"


def test_ensure_schema_skips_create_when_already_exists(arango_db):
    arango_db.has_collection.return_value = True
    arango_db.has_graph.return_value = True
    _repo(arango_db).ensure_schema()
    arango_db.create_collection.assert_not_called()
    arango_db.create_graph.assert_not_called()


# ── get_existing_hashes ───────────────────────────────────────────────────────


def test_get_existing_hashes_returns_key_hash_dict(arango_db):
    arango_db.aql.execute.return_value = iter([
        {"k": "1001", "h": "hash_a"},
        {"k": "1002", "h": "hash_b"},
    ])
    result = _repo(arango_db).get_existing_hashes(PARTITION_KEY)
    assert result == {"1001": "hash_a", "1002": "hash_b"}


def test_get_existing_hashes_empty_partition_returns_empty_dict(arango_db):
    arango_db.aql.execute.return_value = iter([])
    result = _repo(arango_db).get_existing_hashes(PARTITION_KEY)
    assert result == {}


# ── upsert_posts ──────────────────────────────────────────────────────────────


def test_upsert_posts_calls_import_bulk(arango_db):
    docs = [_make_post("1001"), _make_post("1002")]
    _repo(arango_db).upsert_posts(docs)
    arango_db.collection("posts").import_bulk.assert_called_once()


def test_upsert_posts_uses_replace_on_duplicate(arango_db):
    docs = [_make_post("1001")]
    _repo(arango_db).upsert_posts(docs)
    _, kwargs = arango_db.collection("posts").import_bulk.call_args
    assert kwargs.get("on_duplicate") == "replace"


def test_upsert_posts_empty_list_does_not_call_import_bulk(arango_db):
    _repo(arango_db).upsert_posts([])
    arango_db.collection("posts").import_bulk.assert_not_called()


def test_upsert_posts_serializes_with_aliases(arango_db):
    doc = _make_post("1001")
    _repo(arango_db).upsert_posts([doc])
    payload = arango_db.collection("posts").import_bulk.call_args[0][0]
    assert "_key" in payload[0]  # alias used, not 'key'


# ── fetch_posts_needing_embedding ─────────────────────────────────────────────


def test_fetch_posts_needing_embedding_returns_embed_items(arango_db):
    arango_db.aql.execute.return_value = iter([
        {"key": "1001", "text": "hello world"},
        {"key": "1002", "text": "another post"},
    ])
    items = _repo(arango_db).fetch_posts_needing_embedding(PARTITION_KEY)
    assert len(items) == 2
    assert all(isinstance(i, EmbedItem) for i in items)


def test_fetch_posts_needing_embedding_empty_returns_empty(arango_db):
    arango_db.aql.execute.return_value = iter([])
    items = _repo(arango_db).fetch_posts_needing_embedding(PARTITION_KEY)
    assert items == []


# ── update_post_embeddings ────────────────────────────────────────────────────


def test_update_post_embeddings_calls_update_many(arango_db):
    patches = [EmbedPatch(key="1001", embedding=[0.1, 0.2], embedding_model="text-embedding-3-small")]
    _repo(arango_db).update_post_embeddings(patches)
    arango_db.collection("posts").update_many.assert_called_once()


def test_update_post_embeddings_empty_list_does_not_call_update_many(arango_db):
    _repo(arango_db).update_post_embeddings([])
    arango_db.collection("posts").update_many.assert_not_called()


# ── insert_edges ──────────────────────────────────────────────────────────────


def test_insert_edges_calls_import_bulk(arango_db):
    edges = [_make_edge()]
    _repo(arango_db).insert_edges(edges)
    arango_db.collection("quotes").import_bulk.assert_called_once()


def test_insert_edges_uses_ignore_on_duplicate(arango_db):
    edges = [_make_edge()]
    _repo(arango_db).insert_edges(edges)
    _, kwargs = arango_db.collection("quotes").import_bulk.call_args
    assert kwargs.get("on_duplicate") == "ignore"


def test_insert_edges_empty_list_does_not_call_import_bulk(arango_db):
    _repo(arango_db).insert_edges([])
    arango_db.collection("quotes").import_bulk.assert_not_called()


# ── drop_partition_edges ──────────────────────────────────────────────────────


def test_drop_partition_edges_returns_count(arango_db):
    arango_db.aql.execute.return_value = iter([1, 1, 1])
    count = _repo(arango_db).drop_partition_edges(PARTITION_KEY)
    assert count == 3


def test_drop_partition_edges_empty_partition_returns_zero(arango_db):
    arango_db.aql.execute.return_value = iter([])
    count = _repo(arango_db).drop_partition_edges(PARTITION_KEY)
    assert count == 0
