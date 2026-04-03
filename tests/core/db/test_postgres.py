from unittest.mock import MagicMock, patch

from voz_crawler.core.db.postgres import PostgresClient
from voz_crawler.core.db.posts import PostsRepository


# ── PostgresClient ─────────────────────────────────────────────────────────────

def test_engine_created_once():
    with patch("voz_crawler.core.db.postgres.create_engine") as mock_create:
        mock_create.return_value = MagicMock()
        client = PostgresClient(url="postgresql://user:pw@host/db")
        _ = client.engine
        _ = client.engine
    mock_create.assert_called_once_with(
        "postgresql://user:pw@host/db",
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )


def test_dispose_delegates_to_engine():
    with patch("voz_crawler.core.db.postgres.create_engine") as mock_create:
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine
        client = PostgresClient(url="postgresql://user:pw@host/db")
        client.dispose()
    mock_engine.dispose.assert_called_once()


# ── PostsRepository ────────────────────────────────────────────────────────────

def _make_repo(rows):
    """Return a PostsRepository backed by a mock engine that yields rows."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.fetchall.return_value = rows
    return PostsRepository(engine=mock_engine), mock_conn


def test_fetch_new_posts_queries_correct_table():
    repo, mock_conn = _make_repo([])
    repo.fetch_new_posts(after_post_id=0)
    sql_text = str(mock_conn.execute.call_args[0][0])
    assert "raw.voz__posts" in sql_text
    assert "post_id_on_site" in sql_text


def test_fetch_new_posts_passes_last_id():
    repo, mock_conn = _make_repo([])
    repo.fetch_new_posts(after_post_id=42)
    params = mock_conn.execute.call_args[0][1]
    assert params["last_id"] == 42


def test_fetch_html_for_posts_queries_correct_columns():
    repo, mock_conn = _make_repo([])
    repo.fetch_html_for_posts(after_post_id=0)
    sql_text = str(mock_conn.execute.call_args[0][0])
    assert "raw_content_html" in sql_text


def test_fetch_html_for_posts_passes_last_id():
    repo, mock_conn = _make_repo([])
    repo.fetch_html_for_posts(after_post_id=99)
    params = mock_conn.execute.call_args[0][1]
    assert params["last_id"] == 99
