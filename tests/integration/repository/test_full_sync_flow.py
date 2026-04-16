"""Integration test: RawRepository + GraphRepository combined sync flow.

Simulates exactly what the Dagster asset sync_posts_to_graph does:
  1. fetch rows from Postgres (via RawRepository)
  2. get existing hashes from ArangoDB
  3. diff → build_upsert_docs → upsert
  4. verify only changed posts are re-upserted on second run

Requires both Postgres and ArangoDB running.
"""

import pytest
from sqlalchemy import create_engine, text

from tests.conftest import (
    SEED_ROWS,
    TEST_PAGE_NUMBER,
    TEST_PAGE_URL,
    TEST_PARTITION_KEY,
    TEST_THREAD_URL,
)
from voz_crawler.core.graph.post_sync import build_upsert_docs
from voz_crawler.core.repository.graph_repository import GraphRepository
from voz_crawler.core.repository.raw_repository import RawRepository

_CREATE = """
    CREATE TABLE IF NOT EXISTS voz__posts (
        post_id_on_site INTEGER PRIMARY KEY,
        page_url TEXT, author_username TEXT,
        author_id_on_site TEXT, posted_at_raw TEXT,
        raw_content_html TEXT, raw_content_text TEXT
    )
"""
_INSERT = """
    INSERT OR IGNORE INTO voz__posts
        (post_id_on_site, page_url, author_username, author_id_on_site,
         posted_at_raw, raw_content_html, raw_content_text)
    VALUES
        (:post_id_on_site, :page_url, :author_username, :author_id_on_site,
         :posted_at_raw, :raw_content_html, :raw_content_text)
"""


@pytest.fixture
def sqlite_engine():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    with engine.begin() as conn:
        conn.execute(text(_CREATE))
        for row in SEED_ROWS:
            conn.execute(text(_INSERT), row)
    yield engine
    engine.dispose()


@pytest.mark.integration
def test_first_sync_upserts_all_posts(clean_arango_db, sqlite_engine):
    raw_repo = RawRepository(engine=sqlite_engine, schema=None, table="voz__posts")
    graph_repo = GraphRepository(db=clean_arango_db)

    rows = raw_repo.fetch_posts(TEST_PAGE_URL)
    existing = graph_repo.get_existing_hashes(TEST_PARTITION_KEY)
    docs, skipped = build_upsert_docs(
        rows, existing, TEST_PARTITION_KEY, TEST_THREAD_URL, TEST_PAGE_NUMBER
    )

    assert skipped == 0
    assert len(docs) == 2
    graph_repo.upsert_posts(docs)

    hashes = graph_repo.get_existing_hashes(TEST_PARTITION_KEY)
    assert "1001" in hashes
    assert "1002" in hashes


@pytest.mark.integration
def test_second_sync_skips_unchanged_posts(clean_arango_db, sqlite_engine):
    raw_repo = RawRepository(engine=sqlite_engine, schema=None, table="voz__posts")
    graph_repo = GraphRepository(db=clean_arango_db)

    rows = raw_repo.fetch_posts(TEST_PAGE_URL)

    # First sync
    docs, _ = build_upsert_docs(rows, {}, TEST_PARTITION_KEY, TEST_THREAD_URL, TEST_PAGE_NUMBER)
    graph_repo.upsert_posts(docs)

    # Second sync — same rows, same content
    existing = graph_repo.get_existing_hashes(TEST_PARTITION_KEY)
    docs2, skipped2 = build_upsert_docs(
        rows, existing, TEST_PARTITION_KEY, TEST_THREAD_URL, TEST_PAGE_NUMBER
    )

    assert skipped2 == 2
    assert len(docs2) == 0


@pytest.mark.integration
def test_changed_content_triggers_re_upsert(clean_arango_db, sqlite_engine):
    from sqlalchemy import text as t

    raw_repo = RawRepository(engine=sqlite_engine, schema=None, table="voz__posts")
    graph_repo = GraphRepository(db=clean_arango_db)

    rows = raw_repo.fetch_posts(TEST_PAGE_URL)
    docs, _ = build_upsert_docs(rows, {}, TEST_PARTITION_KEY, TEST_THREAD_URL, TEST_PAGE_NUMBER)
    graph_repo.upsert_posts(docs)

    # Simulate content change for post 1001
    with sqlite_engine.begin() as conn:
        conn.execute(
            t(
                "UPDATE voz__posts SET raw_content_text = 'updated content here' WHERE post_id_on_site = 1001"
            )
        )

    rows_updated = raw_repo.fetch_posts(TEST_PAGE_URL)
    existing = graph_repo.get_existing_hashes(TEST_PARTITION_KEY)
    docs2, skipped2 = build_upsert_docs(
        rows_updated, existing, TEST_PARTITION_KEY, TEST_THREAD_URL, TEST_PAGE_NUMBER
    )

    assert skipped2 == 1  # post 1002 unchanged
    assert len(docs2) == 1  # post 1001 re-upserted
    assert docs2[0].key == "1001"


@pytest.mark.integration
def test_changed_post_resets_embedding(clean_arango_db, sqlite_engine):
    """After a content change, the re-upserted post must have embedding=None."""
    from sqlalchemy import text as t

    from voz_crawler.core.entities.arango import EmbedPatch

    raw_repo = RawRepository(engine=sqlite_engine, schema=None, table="voz__posts")
    graph_repo = GraphRepository(db=clean_arango_db)

    rows = raw_repo.fetch_posts(TEST_PAGE_URL)
    docs, _ = build_upsert_docs(rows, {}, TEST_PARTITION_KEY, TEST_THREAD_URL, TEST_PAGE_NUMBER)
    graph_repo.upsert_posts(docs)

    # Patch embeddings as if compute_embeddings ran
    graph_repo.update_post_embeddings(
        [
            EmbedPatch(key="1001", embedding=[0.1, 0.2], embedding_model="m"),
            EmbedPatch(key="1002", embedding=[0.3, 0.4], embedding_model="m"),
        ]
    )

    # Change content of post 1001
    with sqlite_engine.begin() as conn:
        conn.execute(
            t(
                "UPDATE voz__posts SET raw_content_text = 'totally new content here' WHERE post_id_on_site = 1001"
            )
        )

    rows_updated = raw_repo.fetch_posts(TEST_PAGE_URL)
    existing = graph_repo.get_existing_hashes(TEST_PARTITION_KEY)
    docs2, _ = build_upsert_docs(
        rows_updated, existing, TEST_PARTITION_KEY, TEST_THREAD_URL, TEST_PAGE_NUMBER
    )
    graph_repo.upsert_posts(docs2)

    doc = clean_arango_db.collection("posts").get("1001")
    assert doc.get("embedding") is None, "embedding must be reset after content change"
