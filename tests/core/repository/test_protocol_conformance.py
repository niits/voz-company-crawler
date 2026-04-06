"""Verify both backends satisfy the GraphRepository Protocol at runtime.

@runtime_checkable on the Protocol means isinstance() checks all declared
method names exist on the object — a lightweight structural type check.
"""

from unittest.mock import MagicMock

from voz_crawler.core.repository.arango_repository import ArangoGraphRepository
from voz_crawler.core.repository.graph_repository import GraphRepository
from voz_crawler.core.repository.neo4j_repository import Neo4jGraphRepository


def test_arango_satisfies_graph_repository_protocol():
    repo = ArangoGraphRepository(db=MagicMock())
    assert isinstance(repo, GraphRepository)


def test_neo4j_satisfies_graph_repository_protocol():
    repo = Neo4jGraphRepository(driver=MagicMock(), database="test")
    assert isinstance(repo, GraphRepository)
