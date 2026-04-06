"""Unit tests for Neo4jGraphRepository.

Uses the neo4j_mock fixture (driver, session, result) from conftest.py.
All Cypher strings are checked for key patterns rather than exact text
to avoid coupling tests to whitespace/formatting.
"""

from unittest.mock import MagicMock

from voz_crawler.core.entities.graph import EmbedItem, EmbedPatch, GraphEdge, GraphPost
from voz_crawler.core.repository.neo4j_repository import Neo4jGraphRepository

PARTITION_KEY = "test:fixtures"
DATABASE = "test_voz_graph"


def _repo(driver):
    return Neo4jGraphRepository(driver=driver, database=DATABASE)


def _make_post(post_id: int = 1001) -> GraphPost:
    return GraphPost(
        post_id=post_id,
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


def _make_edge(from_id: int = 1002, to_id: int = 2000) -> GraphEdge:
    return GraphEdge(
        from_post_id=from_id,
        to_post_id=to_id,
        edge_key=f"{from_id}_{to_id}_1",
        quote_ordinal=1,
        confidence=1.0,
        method="html_metadata",
        partition_key=PARTITION_KEY,
    )


def _cypher_calls(session):
    """Return list of Cypher strings passed to session.run()."""
    return [call.args[0] for call in session.run.call_args_list]


# ── ensure_schema ─────────────────────────────────────────────────────────────


def test_ensure_schema_runs_at_least_two_statements(neo4j_mock):
    driver, session, _ = neo4j_mock
    _repo(driver).ensure_schema()
    assert session.run.call_count >= 2


def test_ensure_schema_creates_uniqueness_constraint(neo4j_mock):
    driver, session, _ = neo4j_mock
    _repo(driver).ensure_schema()
    calls = _cypher_calls(session)
    assert any("CONSTRAINT" in c and "post_id" in c for c in calls)


def test_ensure_schema_creates_partition_key_index(neo4j_mock):
    driver, session, _ = neo4j_mock
    _repo(driver).ensure_schema()
    calls = _cypher_calls(session)
    assert any("INDEX" in c and "partition_key" in c for c in calls)


def test_ensure_schema_attempts_vector_index(neo4j_mock):
    driver, session, _ = neo4j_mock
    _repo(driver).ensure_schema()
    calls = _cypher_calls(session)
    assert any("VECTOR" in c for c in calls)


def test_ensure_schema_survives_vector_index_failure(neo4j_mock):
    """Vector index creation is wrapped in try/except — should not raise."""
    driver, session, _ = neo4j_mock

    call_count = [0]

    def run_side_effect(cypher, **kwargs):
        call_count[0] += 1
        if "VECTOR" in cypher:
            raise Exception("community edition does not support vector index")
        return neo4j_mock[2]

    session.run.side_effect = run_side_effect
    _repo(driver).ensure_schema()  # must not raise


# ── get_existing_hashes ───────────────────────────────────────────────────────


def test_get_existing_hashes_returns_str_keyed_dict(neo4j_mock):
    driver, session, result = neo4j_mock
    result.__iter__ = MagicMock(
        return_value=iter(
            [
                {"post_id": 1001, "content_hash": "hash_a"},
                {"post_id": 1002, "content_hash": "hash_b"},
            ]
        )
    )
    out = _repo(driver).get_existing_hashes(PARTITION_KEY)
    assert out == {"1001": "hash_a", "1002": "hash_b"}


def test_get_existing_hashes_empty_partition(neo4j_mock):
    driver, session, result = neo4j_mock
    result.__iter__ = MagicMock(return_value=iter([]))
    assert _repo(driver).get_existing_hashes(PARTITION_KEY) == {}


def test_get_existing_hashes_passes_partition_key(neo4j_mock):
    driver, session, result = neo4j_mock
    result.__iter__ = MagicMock(return_value=iter([]))
    _repo(driver).get_existing_hashes("page:42")
    kwargs = session.run.call_args.kwargs
    assert kwargs.get("pk") == "page:42"


# ── upsert_posts ──────────────────────────────────────────────────────────────


def test_upsert_posts_calls_session_run(neo4j_mock):
    driver, session, _ = neo4j_mock
    _repo(driver).upsert_posts([_make_post()])
    session.run.assert_called_once()


def test_upsert_posts_uses_merge_and_unwind(neo4j_mock):
    driver, session, _ = neo4j_mock
    _repo(driver).upsert_posts([_make_post()])
    cypher = session.run.call_args.args[0]
    assert "MERGE" in cypher
    assert "UNWIND" in cypher


def test_upsert_posts_payload_has_int_post_id(neo4j_mock):
    driver, session, _ = neo4j_mock
    _repo(driver).upsert_posts([_make_post(1001)])
    rows = session.run.call_args.kwargs["rows"]
    assert rows[0]["post_id"] == 1001
    assert isinstance(rows[0]["post_id"], int)


def test_upsert_posts_no_arango_key_field(neo4j_mock):
    driver, session, _ = neo4j_mock
    _repo(driver).upsert_posts([_make_post()])
    rows = session.run.call_args.kwargs["rows"]
    assert "_key" not in rows[0]
    assert "key" not in rows[0]


def test_upsert_posts_empty_list_skips_run(neo4j_mock):
    driver, session, _ = neo4j_mock
    _repo(driver).upsert_posts([])
    session.run.assert_not_called()


# ── fetch_posts_needing_embedding ─────────────────────────────────────────────


def test_fetch_posts_needing_embedding_returns_embed_items(neo4j_mock):
    driver, session, result = neo4j_mock
    result.__iter__ = MagicMock(
        return_value=iter(
            [
                {"post_id": 1001, "text": "hello world"},
                {"post_id": 1002, "text": "another post"},
            ]
        )
    )
    items = _repo(driver).fetch_posts_needing_embedding(PARTITION_KEY)
    assert len(items) == 2
    assert all(isinstance(i, EmbedItem) for i in items)
    assert items[0].post_id == 1001  # int


def test_fetch_posts_needing_embedding_empty(neo4j_mock):
    driver, session, result = neo4j_mock
    result.__iter__ = MagicMock(return_value=iter([]))
    assert _repo(driver).fetch_posts_needing_embedding(PARTITION_KEY) == []


# ── update_post_embeddings ────────────────────────────────────────────────────


def test_update_post_embeddings_calls_run(neo4j_mock):
    driver, session, _ = neo4j_mock
    patches = [EmbedPatch(post_id=1001, embedding=[0.1, 0.2], embedding_model="m")]
    _repo(driver).update_post_embeddings(patches)
    session.run.assert_called_once()


def test_update_post_embeddings_payload_has_int_post_id(neo4j_mock):
    driver, session, _ = neo4j_mock
    patches = [EmbedPatch(post_id=1001, embedding=[0.1], embedding_model="m")]
    _repo(driver).update_post_embeddings(patches)
    rows = session.run.call_args.kwargs["patches"]
    assert rows[0]["post_id"] == 1001
    assert "_key" not in rows[0]


def test_update_post_embeddings_empty_skips_run(neo4j_mock):
    driver, session, _ = neo4j_mock
    _repo(driver).update_post_embeddings([])
    session.run.assert_not_called()


# ── drop_partition_edges ──────────────────────────────────────────────────────


def test_drop_partition_edges_returns_count(neo4j_mock):
    driver, session, result = neo4j_mock
    result.single.return_value = {"total": 3}
    assert _repo(driver).drop_partition_edges(PARTITION_KEY) == 3


def test_drop_partition_edges_none_result_returns_zero(neo4j_mock):
    driver, session, result = neo4j_mock
    result.single.return_value = None
    assert _repo(driver).drop_partition_edges(PARTITION_KEY) == 0


def test_drop_partition_edges_cypher_matches_partition(neo4j_mock):
    driver, session, result = neo4j_mock
    result.single.return_value = {"total": 0}
    _repo(driver).drop_partition_edges("page:5")
    assert session.run.call_args.kwargs.get("pk") == "page:5"


# ── insert_edges ──────────────────────────────────────────────────────────────


def test_insert_edges_calls_run(neo4j_mock):
    driver, session, _ = neo4j_mock
    _repo(driver).insert_edges([_make_edge()])
    session.run.assert_called_once()


def test_insert_edges_uses_merge(neo4j_mock):
    driver, session, _ = neo4j_mock
    _repo(driver).insert_edges([_make_edge()])
    cypher = session.run.call_args.args[0]
    assert "MERGE" in cypher


def test_insert_edges_payload_has_int_post_ids(neo4j_mock):
    driver, session, _ = neo4j_mock
    _repo(driver).insert_edges([_make_edge(1002, 2000)])
    rows = session.run.call_args.kwargs["rows"]
    assert rows[0]["from_post_id"] == 1002
    assert rows[0]["to_post_id"] == 2000
    assert "from_vertex" not in rows[0]
    assert "to_vertex" not in rows[0]


def test_insert_edges_empty_skips_run(neo4j_mock):
    driver, session, _ = neo4j_mock
    _repo(driver).insert_edges([])
    session.run.assert_not_called()
