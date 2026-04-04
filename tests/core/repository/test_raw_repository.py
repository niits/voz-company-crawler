"""Tests for voz_crawler.core.repository.RawRepository.

Uses a SQLite in-memory engine (pg_engine fixture from conftest.py).
schema=None avoids SQLite's lack of schema namespacing.
"""

from voz_crawler.core.repository.raw_repository import RawRepository
from tests.conftest import TEST_PAGE_URL


def _repo(pg_engine):
    return RawRepository(engine=pg_engine, schema=None, table="voz__posts")


# ── fetch_posts ───────────────────────────────────────────────────────────────


def test_fetch_posts_returns_all_rows_for_page(pg_engine):
    rows = _repo(pg_engine).fetch_posts(TEST_PAGE_URL)
    assert len(rows) == 2


def test_fetch_posts_ordered_by_post_id(pg_engine):
    rows = _repo(pg_engine).fetch_posts(TEST_PAGE_URL)
    ids = [r.post_id_on_site for r in rows]
    assert ids == sorted(ids)


def test_fetch_posts_raw_content_html_is_none(pg_engine):
    rows = _repo(pg_engine).fetch_posts(TEST_PAGE_URL)
    assert all(r.raw_content_html is None for r in rows)


def test_fetch_posts_text_fields_populated(pg_engine):
    rows = _repo(pg_engine).fetch_posts(TEST_PAGE_URL)
    assert all(r.raw_content_text for r in rows)
    assert all(r.author_username for r in rows)


def test_fetch_posts_unknown_url_returns_empty(pg_engine):
    rows = _repo(pg_engine).fetch_posts("https://does-not-exist.local/")
    assert rows == []


# ── fetch_posts_with_blockquotes ──────────────────────────────────────────────


def test_fetch_posts_with_blockquotes_filters_to_blockquote_rows(pg_engine):
    rows = _repo(pg_engine).fetch_posts_with_blockquotes(TEST_PAGE_URL)
    assert len(rows) == 1
    assert "blockquote" in (rows[0].raw_content_html or "")


def test_fetch_posts_with_blockquotes_post_id_correct(pg_engine):
    rows = _repo(pg_engine).fetch_posts_with_blockquotes(TEST_PAGE_URL)
    # post 1002 is the one seeded with BLOCKQUOTE_HTML
    assert rows[0].post_id_on_site == 1002


def test_fetch_posts_with_blockquotes_text_is_none(pg_engine):
    rows = _repo(pg_engine).fetch_posts_with_blockquotes(TEST_PAGE_URL)
    assert all(r.raw_content_text is None for r in rows)


def test_fetch_posts_with_blockquotes_unknown_url_returns_empty(pg_engine):
    rows = _repo(pg_engine).fetch_posts_with_blockquotes("https://does-not-exist.local/")
    assert rows == []
