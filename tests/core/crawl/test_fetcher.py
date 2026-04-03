from unittest.mock import MagicMock, patch

from voz_crawler.core.crawl.fetcher import Fetcher


def _make_fetcher():
    return Fetcher(flaresolverr_url="http://localhost:8191", timeout=30)


def test_fetch_returns_status_and_html():
    fetcher = _make_fetcher()
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "status": "ok",
        "solution": {"status": 200, "response": "<html>ok</html>"},
    }
    with patch.object(fetcher._session, "post", return_value=mock_response) as mock_post:
        status, html = fetcher.fetch("http://example.com")
    assert status == 200
    assert html == "<html>ok</html>"
    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert call_kwargs[0][0] == "http://localhost:8191/v1"


def test_fetch_raises_on_flaresolverr_error():
    fetcher = _make_fetcher()
    mock_response = MagicMock()
    mock_response.json.return_value = {"status": "error", "message": "timeout"}
    with patch.object(fetcher._session, "post", return_value=mock_response):
        try:
            fetcher.fetch("http://example.com")
            assert False, "Expected RuntimeError"
        except RuntimeError as e:
            assert "FlareSolverr error" in str(e)


def test_is_cf_block_by_status_code_and_marker():
    fetcher = _make_fetcher()
    assert fetcher.is_cf_block(403, "Just a moment...") is True
    assert fetcher.is_cf_block(503, "cf-browser-verification") is True


def test_is_cf_block_false_for_200():
    fetcher = _make_fetcher()
    assert fetcher.is_cf_block(200, "<html>normal page</html>") is False


def test_is_cf_block_false_for_403_without_marker():
    fetcher = _make_fetcher()
    assert fetcher.is_cf_block(403, "<html>not cloudflare</html>") is False


def test_fetch_raises_on_http_error():
    import pytest
    from requests.exceptions import HTTPError
    fetcher = _make_fetcher()
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = HTTPError("503")
    with patch.object(fetcher._session, "post", return_value=mock_response):
        with pytest.raises(HTTPError):
            fetcher.fetch("http://example.com")


def test_session_reused_across_calls():
    fetcher = _make_fetcher()
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "status": "ok",
        "solution": {"status": 200, "response": "<html/>"},
    }
    with patch.object(fetcher._session, "post", return_value=mock_response) as mock_post:
        fetcher.fetch("http://a.com")
        fetcher.fetch("http://b.com")
        # Both calls go through the same session object
        assert mock_post.call_count == 2
