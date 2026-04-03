from unittest.mock import MagicMock, patch, call

from voz_crawler.core.graph.client import ArangoGraphClient


def _make_client(mock_arango_client):
    """Patch ArangoClient and return an ArangoGraphClient instance."""
    mock_db = MagicMock()
    mock_arango_client.return_value.db.return_value = mock_db
    client = ArangoGraphClient(
        host="localhost", port=8529,
        username="root", password="pw",
        db_name="test_db",
    )
    return client, mock_db


def test_db_returns_authenticated_database():
    with patch("voz_crawler.core.graph.client.ArangoClient") as mock_arango:
        client, mock_db = _make_client(mock_arango)
        result = client.db()
    assert result is mock_db


def test_db_calls_ensure_schema_on_first_access():
    with patch("voz_crawler.core.graph.client.ArangoClient") as mock_arango, \
         patch("voz_crawler.core.graph.client.ensure_schema") as mock_schema:
        client, _ = _make_client(mock_arango)
        client.db()
    mock_schema.assert_called_once()


def test_ensure_schema_called_only_once():
    with patch("voz_crawler.core.graph.client.ArangoClient") as mock_arango, \
         patch("voz_crawler.core.graph.client.ensure_schema") as mock_schema:
        client, _ = _make_client(mock_arango)
        client.db()
        client.db()
        client.db()
    mock_schema.assert_called_once()


def test_db_returns_same_instance_on_repeated_calls():
    with patch("voz_crawler.core.graph.client.ArangoClient") as mock_arango:
        client, mock_db = _make_client(mock_arango)
        first = client.db()
        second = client.db()
    assert first is second
