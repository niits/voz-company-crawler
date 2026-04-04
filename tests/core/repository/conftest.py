"""Fixtures for repository tests.

pg_engine  — SQLite in-memory, seeded with SEED_ROWS (no schema prefix, SQLite compat).
arango_db  — MagicMock wired to return sensible defaults; each test configures it further.
"""

import pytest
from sqlalchemy import create_engine, text
from unittest.mock import MagicMock

from tests.conftest import SEED_ROWS, TEST_PAGE_URL

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
    """SQLite in-memory engine seeded with SEED_ROWS.

    RawRepository is constructed with schema=None so SQLAlchemy emits
    plain table names without a schema prefix (SQLite has no schema support).
    """
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    with engine.begin() as conn:
        conn.execute(text(_CREATE_TABLE))
        for row in SEED_ROWS:
            conn.execute(text(_INSERT_ROW), row)
    yield engine
    engine.dispose()


@pytest.fixture
def arango_db():
    """MagicMock that stands in for a python-arango Database instance.

    Default wiring:
    - has_collection / has_graph → False (so ensure_schema always creates)
    - aql.execute → returns [] (override per-test as needed)
    - collection() → a fresh MagicMock with import_bulk / update_many / add_persistent_index
    """
    db = MagicMock()
    db.has_collection.return_value = False
    db.has_graph.return_value = False

    # aql cursor defaults to empty — override in tests that need data
    db.aql.execute.return_value = iter([])

    # collection() returns a consistent mock regardless of name
    col = MagicMock()
    db.collection.return_value = col

    return db
