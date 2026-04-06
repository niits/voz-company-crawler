"""Integration tests for ArangoGraphRepository against a real ArangoDB instance.

Requires: docker compose -f docker/dev/docker-compose.yml up arangodb -d

Run with: pytest tests/integration/ -m integration
"""

import pytest

from voz_crawler.core.entities.graph import EmbedPatch, GraphEdge, GraphPost
from voz_crawler.core.repository.arango_repository import ArangoGraphRepository

PARTITION_KEY = "integ:page:1"
THREAD_URL = "https://voz.vn/t/test"


def _repo(db) -> ArangoGraphRepository:
    return ArangoGraphRepository(db=db)


def _post(post_id: int, text: str = "content long enough to embed") -> GraphPost:
    return GraphPost(
        post_id=post_id,
        author_username="tester",
        author_id=str(post_id * 10),
        posted_at="2024-06-01T10:00:00+07:00",
        content_text=text,
        content_hash=f"hash_{post_id}",
        partition_key=PARTITION_KEY,
        thread_url=THREAD_URL,
        page_number=1,
        embedding=None,
    )


def _edge(from_id: int, to_id: int) -> GraphEdge:
    return GraphEdge(
        from_post_id=from_id,
        to_post_id=to_id,
        edge_key=f"{from_id}_{to_id}_1",
        quote_ordinal=1,
        confidence=1.0,
        method="html_metadata",
        partition_key=PARTITION_KEY,
    )


@pytest.mark.integration
def test_ensure_schema_creates_posts_collection(arango_test_db):
    repo = _repo(arango_test_db)
    repo.ensure_schema()
    assert arango_test_db.has_collection("posts")


@pytest.mark.integration
def test_ensure_schema_creates_quotes_edge_collection(arango_test_db):
    repo = _repo(arango_test_db)
    repo.ensure_schema()
    assert arango_test_db.has_collection("quotes")


@pytest.mark.integration
def test_ensure_schema_creates_reply_graph(arango_test_db):
    repo = _repo(arango_test_db)
    repo.ensure_schema()
    assert arango_test_db.has_graph("reply_graph")


@pytest.mark.integration
def test_ensure_schema_is_idempotent(arango_test_db):
    repo = _repo(arango_test_db)
    repo.ensure_schema()
    repo.ensure_schema()  # second call must not raise
    assert arango_test_db.has_collection("posts")


@pytest.mark.integration
def test_upsert_and_get_existing_hashes(arango_test_db):
    repo = _repo(arango_test_db)
    repo.ensure_schema()

    posts = [_post(9001), _post(9002)]
    repo.upsert_posts(posts)

    hashes = repo.get_existing_hashes(PARTITION_KEY)
    assert "9001" in hashes
    assert "9002" in hashes
    assert hashes["9001"] == "hash_9001"


@pytest.mark.integration
def test_upsert_replace_semantics(arango_test_db):
    """Upserting again with a different hash replaces the document."""
    repo = _repo(arango_test_db)
    repo.ensure_schema()

    repo.upsert_posts([_post(9003)])
    modified = GraphPost(
        post_id=9003,
        author_username="tester",
        author_id="90030",
        posted_at="2024-06-01T10:00:00+07:00",
        content_text="updated text",
        content_hash="new_hash",
        partition_key=PARTITION_KEY,
        thread_url=THREAD_URL,
        page_number=1,
        embedding=None,
    )
    repo.upsert_posts([modified])

    hashes = repo.get_existing_hashes(PARTITION_KEY)
    assert hashes["9003"] == "new_hash"


@pytest.mark.integration
def test_fetch_posts_needing_embedding(arango_test_db):
    repo = _repo(arango_test_db)
    repo.ensure_schema()
    repo.upsert_posts([_post(9010), _post(9011)])

    items = repo.fetch_posts_needing_embedding(PARTITION_KEY)
    post_ids = {i.post_id for i in items}
    assert 9010 in post_ids
    assert 9011 in post_ids


@pytest.mark.integration
def test_update_post_embeddings(arango_test_db):
    repo = _repo(arango_test_db)
    repo.ensure_schema()
    repo.upsert_posts([_post(9020)])

    patch = EmbedPatch(post_id=9020, embedding=[0.1, 0.2, 0.3], embedding_model="test-model")
    repo.update_post_embeddings([patch])

    # After patching, post should no longer appear in needing-embedding list
    items = repo.fetch_posts_needing_embedding(PARTITION_KEY)
    assert 9020 not in {i.post_id for i in items}


@pytest.mark.integration
def test_insert_and_drop_edges(arango_test_db):
    repo = _repo(arango_test_db)
    repo.ensure_schema()
    repo.upsert_posts([_post(9030), _post(9031)])

    edges = [_edge(9031, 9030)]
    repo.insert_edges(edges)

    dropped = repo.drop_partition_edges(PARTITION_KEY)
    assert dropped >= 1


@pytest.mark.integration
def test_insert_edges_ignore_duplicate(arango_test_db):
    """Inserting the same edge twice must not raise."""
    repo = _repo(arango_test_db)
    repo.ensure_schema()
    repo.upsert_posts([_post(9040), _post(9041)])

    edge = _edge(9041, 9040)
    repo.insert_edges([edge])
    repo.insert_edges([edge])  # duplicate — must be silently ignored


@pytest.mark.integration
def test_drop_partition_edges_returns_zero_when_empty(arango_test_db):
    repo = _repo(arango_test_db)
    repo.ensure_schema()
    count = repo.drop_partition_edges("integ:nonexistent:999")
    assert count == 0
