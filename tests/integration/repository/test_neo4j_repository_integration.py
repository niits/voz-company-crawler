"""Integration tests for Neo4jGraphRepository against a real Neo4j instance.

Requires: docker compose -f docker/dev/docker-compose.yml up neo4j -d
          (wait ~30s for GenAI plugin to download on first start)

Run with: pytest tests/integration/ -m integration
"""

import pytest

from voz_crawler.core.entities.graph import EmbedPatch, GraphEdge, GraphPost
from voz_crawler.core.repository.neo4j_repository import Neo4jGraphRepository

PARTITION_KEY = "integ:page:1"
THREAD_URL = "https://voz.vn/t/test"


def _repo(driver, db_name) -> Neo4jGraphRepository:
    return Neo4jGraphRepository(driver=driver, database=db_name)


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
def test_ensure_schema_runs_without_error(neo4j_test_driver):
    driver, db_name = neo4j_test_driver
    repo = _repo(driver, db_name)
    repo.ensure_schema()  # should not raise


@pytest.mark.integration
def test_ensure_schema_is_idempotent(neo4j_test_driver):
    driver, db_name = neo4j_test_driver
    repo = _repo(driver, db_name)
    repo.ensure_schema()
    repo.ensure_schema()  # second call must not raise


@pytest.mark.integration
def test_upsert_and_get_existing_hashes(neo4j_test_driver):
    driver, db_name = neo4j_test_driver
    repo = _repo(driver, db_name)
    repo.ensure_schema()

    repo.upsert_posts([_post(8001), _post(8002)])

    hashes = repo.get_existing_hashes(PARTITION_KEY)
    assert "8001" in hashes
    assert "8002" in hashes
    assert hashes["8001"] == "hash_8001"


@pytest.mark.integration
def test_upsert_replace_semantics(neo4j_test_driver):
    """MERGE + SET replaces all properties on re-upsert."""
    driver, db_name = neo4j_test_driver
    repo = _repo(driver, db_name)
    repo.ensure_schema()

    repo.upsert_posts([_post(8003)])
    modified = GraphPost(
        post_id=8003,
        author_username="tester",
        author_id="80030",
        posted_at="2024-06-01T10:00:00+07:00",
        content_text="updated text",
        content_hash="new_hash_8003",
        partition_key=PARTITION_KEY,
        thread_url=THREAD_URL,
        page_number=1,
        embedding=None,
    )
    repo.upsert_posts([modified])

    hashes = repo.get_existing_hashes(PARTITION_KEY)
    assert hashes["8003"] == "new_hash_8003"


@pytest.mark.integration
def test_fetch_posts_needing_embedding(neo4j_test_driver):
    driver, db_name = neo4j_test_driver
    repo = _repo(driver, db_name)
    repo.ensure_schema()
    repo.upsert_posts([_post(8010), _post(8011)])

    items = repo.fetch_posts_needing_embedding(PARTITION_KEY)
    post_ids = {i.post_id for i in items}
    assert 8010 in post_ids
    assert 8011 in post_ids


@pytest.mark.integration
def test_update_post_embeddings(neo4j_test_driver):
    driver, db_name = neo4j_test_driver
    repo = _repo(driver, db_name)
    repo.ensure_schema()
    repo.upsert_posts([_post(8020)])

    patch = EmbedPatch(post_id=8020, embedding=[0.1, 0.2, 0.3], embedding_model="test-model")
    repo.update_post_embeddings([patch])

    # After patching, post must no longer appear in the needing-embedding list
    items = repo.fetch_posts_needing_embedding(PARTITION_KEY)
    assert 8020 not in {i.post_id for i in items}


@pytest.mark.integration
def test_insert_edges_and_drop(neo4j_test_driver):
    driver, db_name = neo4j_test_driver
    repo = _repo(driver, db_name)
    repo.ensure_schema()
    repo.upsert_posts([_post(8030), _post(8031)])

    repo.insert_edges([_edge(8031, 8030)])
    dropped = repo.drop_partition_edges(PARTITION_KEY)
    assert dropped >= 1


@pytest.mark.integration
def test_insert_edges_merge_deduplication(neo4j_test_driver):
    """MERGE ensures the same relationship is not created twice."""
    driver, db_name = neo4j_test_driver
    repo = _repo(driver, db_name)
    repo.ensure_schema()
    repo.upsert_posts([_post(8040), _post(8041)])

    edge = _edge(8041, 8040)
    repo.insert_edges([edge])
    repo.insert_edges([edge])  # second insert must not raise or duplicate

    # Verify exactly one relationship exists via raw Cypher
    with driver.session(database=db_name) as s:
        result = s.run(
            "MATCH ()-[r:QUOTES {edge_key: $key}]->() RETURN count(r) AS cnt",
            key=edge.edge_key,
        )
        record = result.single()
        assert record["cnt"] == 1


@pytest.mark.integration
def test_drop_partition_edges_returns_zero_when_empty(neo4j_test_driver):
    driver, db_name = neo4j_test_driver
    repo = _repo(driver, db_name)
    repo.ensure_schema()
    count = repo.drop_partition_edges("integ:nonexistent:999")
    assert count == 0
