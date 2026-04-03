from unittest.mock import MagicMock, call

from voz_crawler.core.llm.embeddings import EmbeddingService


def _make_service(pending_posts=None):
    """Build EmbeddingService with mocked OpenAI client and ArangoDB."""
    client = MagicMock()
    arango_db = MagicMock()

    cursor = MagicMock()
    cursor.__iter__ = MagicMock(return_value=iter(pending_posts or []))
    arango_db.aql.execute.return_value = cursor

    return EmbeddingService(client=client), client, arango_db


def test_returns_zero_when_no_pending_posts():
    svc, _, arango_db = _make_service(pending_posts=[])
    stats = svc.compute_missing(arango_db)
    assert stats["records_processed"] == 0


def test_does_not_call_openai_when_no_pending():
    svc, mock_client, arango_db = _make_service(pending_posts=[])
    svc.compute_missing(arango_db)
    mock_client.embeddings.create.assert_not_called()


def test_batches_calls_correctly():
    """25 posts with batch_size=20 should produce 2 OpenAI API calls."""
    posts = [{"_key": str(i), "text": f"post {i}"} for i in range(25)]
    svc, mock_client, arango_db = _make_service(pending_posts=posts)
    svc = EmbeddingService(client=mock_client, batch_size=20)

    mock_emb = MagicMock()
    mock_emb.embedding = [0.1] * 1536
    mock_client.embeddings.create.return_value.data = [mock_emb] * 20

    svc.compute_missing(arango_db)
    assert mock_client.embeddings.create.call_count == 2


def test_updates_posts_with_embedding():
    posts = [{"_key": "42", "text": "some text"}]
    svc, mock_client, arango_db = _make_service(pending_posts=posts)
    svc = EmbeddingService(client=mock_client, batch_size=20)

    mock_emb = MagicMock()
    mock_emb.embedding = [0.5] * 1536
    mock_client.embeddings.create.return_value.data = [mock_emb]

    svc.compute_missing(arango_db)

    posts_col = arango_db.collection("Posts")
    posts_col.update.assert_called_once()
    update_doc = posts_col.update.call_args[0][0]
    assert update_doc["_key"] == "42"
    assert update_doc["embedding"] == [0.5] * 1536
    assert update_doc["embedding_model"] == "text-embedding-3-small"


def test_returns_correct_count():
    posts = [{"_key": str(i), "text": f"t{i}"} for i in range(5)]
    svc, mock_client, arango_db = _make_service(pending_posts=posts)
    svc = EmbeddingService(client=mock_client, batch_size=20)
    mock_emb = MagicMock()
    mock_emb.embedding = [0.1] * 1536
    mock_client.embeddings.create.return_value.data = [mock_emb] * 5
    stats = svc.compute_missing(arango_db)
    assert stats["records_processed"] == 5
