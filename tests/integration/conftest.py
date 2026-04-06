"""Fixtures for integration tests.

Both fixtures require real Docker services to be running.
Skip integration tests with:  pytest tests/unit/
Run them with:  pytest tests/integration/ -m integration

Services needed:
  docker compose -f docker/dev/docker-compose.yml up arangodb neo4j -d
"""

import os

import pytest


@pytest.fixture(scope="session")
def arango_test_db():
    """Session-scoped ArangoDB Database handle pointing at a fresh test database.

    Creates ``test_voz_graph`` at the start of the session and drops it at teardown
    so tests always start from a clean state.

    Requires ArangoDB running at ARANGO_HOST:ARANGO_PORT (defaults: localhost:8529).
    """
    from arango import ArangoClient

    host = os.getenv("ARANGO_HOST", "localhost")
    port = int(os.getenv("ARANGO_PORT", "8529"))
    root_password = os.getenv("ARANGO_ROOT_PASSWORD", "changeme")

    try:
        client = ArangoClient(hosts=f"http://{host}:{port}")
        sys_db = client.db("_system", username="root", password=root_password)
    except Exception as exc:
        pytest.skip(f"ArangoDB not reachable at {host}:{port} — {exc}")

    db_name = "test_voz_graph_integ"
    if sys_db.has_database(db_name):
        sys_db.delete_database(db_name)
    sys_db.create_database(db_name)
    db = client.db(db_name, username="root", password=root_password)

    yield db

    if sys_db.has_database(db_name):
        sys_db.delete_database(db_name)


@pytest.fixture(scope="session")
def neo4j_test_driver():
    """Session-scoped Neo4j Driver connected to a real Neo4j instance.

    Uses database ``test_voz_graph_integ``, created fresh at session start
    and dropped at teardown.

    Requires Neo4j running at NEO4J_URI (default: bolt://localhost:7687).
    """
    from neo4j import GraphDatabase

    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "changeme")
    db_name = "testvozgraphinteg"  # Neo4j db names must be lowercase, no underscores > 63 chars

    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
    except Exception as exc:
        pytest.skip(f"Neo4j not reachable at {uri} — {exc}")

    # Create fresh test database
    with driver.session(database="system") as s:
        s.run("CREATE DATABASE $db IF NOT EXISTS", db=db_name)

    # Clear all nodes/edges in case of leftover data
    with driver.session(database=db_name) as s:
        s.run("MATCH (n) DETACH DELETE n")

    yield driver, db_name

    # Teardown: drop the test database
    with driver.session(database="system") as s:
        s.run("DROP DATABASE $db IF EXISTS", db=db_name)

    driver.close()
