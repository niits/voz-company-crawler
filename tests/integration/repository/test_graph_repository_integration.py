"""Integration tests for GraphRepository against a real ArangoDB instance.

Each test uses the ``clean_arango_db`` fixture which truncates posts/quotes
before running, so tests are fully isolated despite sharing one session-DB.

Run with:
  uv run pytest tests/integration/ -m integration
"""

import pytest

from voz_crawler.core.entities.arango import ArangoEdge, EmbedPatch, NormalizedPostDoc, RawPostDoc
from voz_crawler.core.repository.graph_repository import GraphRepository

PARTITION_KEY = "integ:page:1"
OTHER_PARTITION = "integ:page:2"
THREAD_URL = "https://voz.vn/t/test"


def _repo(db) -> GraphRepository:
    return GraphRepository(db=db)


def _post(
    key: str, content_hash: str = "hash_x", text: str = "long enough content here"
) -> RawPostDoc:
    return RawPostDoc(
        key=key,
        post_id=int(key),
        author_username="tester",
        author_id=f"{key}0",
        posted_at="2024-06-01T10:00:00+07:00",
        content_text=text,
        content_hash=content_hash,
        partition_key=PARTITION_KEY,
        thread_url=THREAD_URL,
        page_number=1,
    )


def _edge(from_key: str, to_key: str, ordinal: int = 1) -> ArangoEdge:
    return ArangoEdge(
        from_vertex=f"posts/{from_key}",
        to_vertex=f"posts/{to_key}",
        key=f"{from_key}_{to_key}_{ordinal}",
        quote_ordinal=ordinal,
        confidence=1.0,
        method="html_metadata",
        partition_key=PARTITION_KEY,
    )


# ── ensure_schema ─────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_ensure_schema_creates_collections(clean_arango_db):
    db = clean_arango_db
    assert db.has_collection("posts")
    assert db.has_collection("quotes")


@pytest.mark.integration
def test_ensure_schema_creates_graph(clean_arango_db):
    assert clean_arango_db.has_graph("reply_graph")


@pytest.mark.integration
def test_ensure_schema_idempotent(clean_arango_db):
    """Calling ensure_schema twice must not raise."""
    _repo(clean_arango_db).ensure_schema()
    _repo(clean_arango_db).ensure_schema()


# ── upsert_posts + get_existing_hashes ───────────────────────────────────────


@pytest.mark.integration
def test_upsert_and_read_back_hashes(clean_arango_db):
    repo = _repo(clean_arango_db)
    repo.upsert_posts([_post("8001", "hash_a"), _post("8002", "hash_b")])

    hashes = repo.get_existing_hashes(PARTITION_KEY)
    assert hashes == {"8001": "hash_a", "8002": "hash_b"}


@pytest.mark.integration
def test_upsert_replace_updates_hash(clean_arango_db):
    """Re-upserting the same key with a new hash overwrites the old document."""
    repo = _repo(clean_arango_db)
    repo.upsert_posts([_post("8003", "old_hash")])
    repo.upsert_posts([_post("8003", "new_hash")])

    hashes = repo.get_existing_hashes(PARTITION_KEY)
    assert hashes["8003"] == "new_hash"


@pytest.mark.integration
def test_upsert_replace_resets_layer2_fields(clean_arango_db):
    """Replace semantics must reset Layer 2 fields (embedding, content_class) on content change.

    Simulate: first upsert a RawPostDoc, then manually patch a Layer 2 field (embedding)
    directly on the document, then re-upsert with new hash. The replace should clear
    the Layer 2 field since it's not part of RawPostDoc.
    """
    repo = _repo(clean_arango_db)
    repo.upsert_posts([_post("8004", "hash_v1")])
    # Manually patch an embedding (simulates compute_embeddings having run)
    clean_arango_db.collection("posts").update({"_key": "8004", "embedding": [0.1, 0.2, 0.3]})
    assert clean_arango_db.collection("posts").get("8004")["embedding"] is not None

    # Re-upsert with new content hash → on_duplicate=replace clears embedding
    repo.upsert_posts([_post("8004", "hash_v2")])

    doc = clean_arango_db.collection("posts").get("8004")
    assert doc.get("embedding") is None


@pytest.mark.integration
def test_get_existing_hashes_only_for_partition(clean_arango_db):
    """Hashes from other partitions must not bleed into the result."""
    repo = _repo(clean_arango_db)
    post_other = RawPostDoc(
        key="9001",
        post_id=9001,
        author_username="u",
        author_id="90010",
        posted_at=None,
        content_text="other partition text",
        content_hash="other_hash",
        partition_key=OTHER_PARTITION,
        thread_url=THREAD_URL,
        page_number=2,
    )
    repo.upsert_posts([_post("8010"), post_other])
    hashes = repo.get_existing_hashes(PARTITION_KEY)
    assert "8010" in hashes
    assert "9001" not in hashes


# ── fetch_posts_needing_embedding ─────────────────────────────────────────────


@pytest.mark.integration
def test_fetch_posts_needing_embedding_returns_null_embedding_posts(clean_arango_db):
    repo = _repo(clean_arango_db)
    repo.upsert_posts([_post("8020"), _post("8021")])
    # fetch_posts_needing_embedding requires normalized_own_text to be populated first
    repo.update_post_normalizations(
        [
            NormalizedPostDoc(
                key="8020",
                normalized_own_text="long enough normalized text here",
                normalization_version=1,
            ),
            NormalizedPostDoc(
                key="8021",
                normalized_own_text="long enough normalized text here",
                normalization_version=1,
            ),
        ]
    )
    items = repo.fetch_posts_needing_embedding(PARTITION_KEY)
    keys = {i.key for i in items}
    assert "8020" in keys
    assert "8021" in keys


@pytest.mark.integration
def test_fetch_posts_needing_embedding_excludes_already_embedded(clean_arango_db):
    repo = _repo(clean_arango_db)
    repo.upsert_posts([_post("8030")])
    repo.update_post_normalizations(
        [
            NormalizedPostDoc(
                key="8030",
                normalized_own_text="long enough normalized text here",
                normalization_version=1,
            ),
        ]
    )
    repo.update_post_embeddings([EmbedPatch(key="8030", embedding=[0.1, 0.2], embedding_model="m")])
    items = repo.fetch_posts_needing_embedding(PARTITION_KEY)
    assert "8030" not in {i.key for i in items}


@pytest.mark.integration
def test_fetch_posts_needing_embedding_excludes_short_content(clean_arango_db):
    """Posts with content shorter than 20 chars must be excluded."""
    repo = _repo(clean_arango_db)
    short_post = RawPostDoc(
        key="8040",
        post_id=8040,
        author_username="u",
        author_id="1",
        posted_at=None,
        content_text="too short",
        content_hash="hash_short",
        partition_key=PARTITION_KEY,
        thread_url=THREAD_URL,
        page_number=1,
    )
    repo.upsert_posts([short_post])
    items = repo.fetch_posts_needing_embedding(PARTITION_KEY)
    assert "8040" not in {i.key for i in items}


# ── update_post_embeddings ────────────────────────────────────────────────────


@pytest.mark.integration
def test_update_post_embeddings_persists_vector(clean_arango_db):
    repo = _repo(clean_arango_db)
    repo.upsert_posts([_post("8050")])
    repo.update_post_embeddings(
        [EmbedPatch(key="8050", embedding=[0.1, 0.2, 0.3], embedding_model="test-model")]
    )
    doc = clean_arango_db.collection("posts").get("8050")
    assert doc["embedding"] == [0.1, 0.2, 0.3]
    assert doc["embedding_model"] == "test-model"


# ── drop_partition_edges + insert_edges ───────────────────────────────────────


@pytest.mark.integration
def test_insert_and_drop_edges(clean_arango_db):
    repo = _repo(clean_arango_db)
    repo.upsert_posts([_post("8060"), _post("8061")])
    repo.insert_edges([_edge("8061", "8060")])

    dropped = repo.drop_partition_edges(PARTITION_KEY)
    assert dropped == 1
    assert clean_arango_db.collection("quotes").count() == 0


@pytest.mark.integration
def test_insert_edges_deduplication(clean_arango_db):
    """Inserting the same edge twice must not create duplicates."""
    repo = _repo(clean_arango_db)
    repo.upsert_posts([_post("8070"), _post("8071")])
    edge = _edge("8071", "8070")
    repo.insert_edges([edge])
    repo.insert_edges([edge])
    assert clean_arango_db.collection("quotes").count() == 1


@pytest.mark.integration
def test_drop_edges_only_affects_target_partition(clean_arango_db):
    """drop_partition_edges must not delete edges from other partitions."""
    repo = _repo(clean_arango_db)
    repo.upsert_posts([_post("8080"), _post("8081")])

    edge_this = _edge("8081", "8080")
    edge_other = ArangoEdge(
        from_vertex="posts/8081",
        to_vertex="posts/8080",
        key="8081_8080_other",
        quote_ordinal=1,
        confidence=1.0,
        method="html_metadata",
        partition_key=OTHER_PARTITION,
    )
    repo.insert_edges([edge_this, edge_other])
    repo.drop_partition_edges(PARTITION_KEY)

    remaining = list(clean_arango_db.aql.execute("FOR e IN quotes RETURN e"))
    assert len(remaining) == 1
    assert remaining[0]["partition_key"] == OTHER_PARTITION


@pytest.mark.integration
def test_drop_edges_returns_zero_when_none_exist(clean_arango_db):
    assert _repo(clean_arango_db).drop_partition_edges("integ:nonexistent:99") == 0
