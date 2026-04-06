"""Fixtures for repository unit tests.

pg_engine   — SQLite in-memory, seeded with SEED_ROWS (no schema prefix, SQLite compat).
arango_db   — MagicMock wired to return sensible defaults; each test configures it further.
neo4j_mock  — (driver, session, result) triple wired as a context manager for neo4j driver.
"""

from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text

from tests.conftest import SEED_ROWS

_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS voz__posts (
        post_id_on_site INTEGER PRIMARY KEY,
        page_url        TEXT,
        author_username TEXT,
        author_id_on_site TEXT,
        posted_at_raw   TEXT,
        raw_content_html TEXT,
        raw_content_text TEXT
    )
"""

_INSERT_ROW = """
    INSERT OR IGNORE INTO voz__posts
        (post_id_on_site, page_url, author_username, author_id_on_site,
         posted_at_raw, raw_content_html, raw_content_text)
    VALUES
        (:post_id_on_site, :page_url, :author_username, :author_id_on_site,
         :posted_at_raw, :raw_content_html, :raw_content_text)
"""


@pytest.fixture
def pg_engine():
    """SQLite in-memory engine seeded with SEED_ROWS."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    with engine.begin() as conn:
        conn.execute(text(_CREATE_TABLE))
        for row in SEED_ROWS:
            conn.execute(text(_INSERT_ROW), row)
    yield engine
    engine.dispose()


@pytest.fixture
def arango_db():
    """MagicMock standing in for a python-arango Database instance.

    Default wiring:
    - has_collection / has_graph → False (ensure_schema always creates)
    - aql.execute → returns [] (override per-test as needed)
    - collection() → a fresh MagicMock with import_bulk / update_many / add_persistent_index
    """
    db = MagicMock()
    db.has_collection.return_value = False
    db.has_graph.return_value = False
    db.aql.execute.return_value = iter([])
    col = MagicMock()
    db.collection.return_value = col
    return db


@pytest.fixture
def neo4j_mock():
    """(driver, session, result) triple mocking the neo4j Python driver.

    Wiring:
    - driver.session() is a context manager that yields ``session``
    - session.run() returns ``result``
    - result.__iter__ returns an empty iterator (override per-test)
    - result.single() returns None (override per-test)

    Usage in tests::

        def test_something(neo4j_mock):
            driver, session, result = neo4j_mock
            result.__iter__ = MagicMock(return_value=iter([{"post_id": 1, "content_hash": "x"}]))
            repo = Neo4jGraphRepository(driver=driver, database="test")
            ...
    """
    driver = MagicMock()
    session = MagicMock()
    result = MagicMock()

    result.single.return_value = None
    result.__iter__ = MagicMock(return_value=iter([]))

    session.run.return_value = result
    driver.session.return_value.__enter__ = MagicMock(return_value=session)
    driver.session.return_value.__exit__ = MagicMock(return_value=False)

    return driver, session, result
