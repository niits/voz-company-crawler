"""Fixtures for integration tests.

Requires ArangoDB running — start with:
  docker compose -f docker/dev/docker-compose.yml up arangodb -d

Run integration tests:
  uv run pytest tests/integration/ -m integration

Skip them (unit-only):
  uv run pytest tests/core/
"""

import os

import pytest


@pytest.fixture(scope="session")
def arango_test_db():
    """Session-scoped ArangoDB Database handle pointing at a fresh test database.

    Creates ``test_voz_graph_integ`` at session start, drops it at teardown.
    Each test gets a clean slate via the function-scoped ``clean_arango_db``
    fixture below.

    Skips the entire session if ArangoDB is not reachable.
    """
    from arango import ArangoClient

    host = os.getenv("ARANGO_HOST", "localhost")
    port = int(os.getenv("ARANGO_PORT", "8529"))
    password = os.getenv("ARANGO_ROOT_PASSWORD", "change_me")
    db_name = "test_voz_graph_integ"

    try:
        client = ArangoClient(hosts=f"http://{host}:{port}")
        sys_db = client.db("_system", username="root", password=password)
        sys_db.properties()  # connectivity check
    except Exception as exc:
        pytest.skip(f"ArangoDB not reachable at {host}:{port} — {exc}")

    if sys_db.has_database(db_name):
        sys_db.delete_database(db_name)
    sys_db.create_database(db_name)
    db = client.db(db_name, username="root", password=password)

    yield db

    if sys_db.has_database(db_name):
        sys_db.delete_database(db_name)


@pytest.fixture
def clean_arango_db(arango_test_db):
    """Function-scoped fixture: truncates all collections before each test.

    Ensures tests don't bleed data into one another even though they share
    the same session-scoped database connection.
    """
    from voz_crawler.core.repository.graph_repository import GraphRepository

    repo = GraphRepository(db=arango_test_db)
    repo.ensure_schema()

    # Truncate between tests so each starts from empty collections
    for col_name in ("posts", "quotes"):
        if arango_test_db.has_collection(col_name):
            arango_test_db.collection(col_name).truncate()

    yield arango_test_db
