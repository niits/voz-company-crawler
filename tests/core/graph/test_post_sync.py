"""Tests for voz_crawler.core.graph.post_sync.

Pure functions — no external services, no mocking required.
"""

import hashlib

from voz_crawler.core.entities.raw_post import RawPost
from voz_crawler.core.graph.post_sync import build_upsert_docs, content_hash

PARTITION_KEY = "test:fixtures"
THREAD_URL = "https://voz.vn/t/test"
PAGE_NUMBER = 1


def _make_post(post_id: int, text: str | None = "some text") -> RawPost:
    return RawPost(
        post_id_on_site=post_id,
        author_username="user",
        author_id_on_site="1",
        posted_at_raw="2024-01-01T10:00:00+0700",
        raw_content_text=text,
        raw_content_html=None,
    )


# ── content_hash ──────────────────────────────────────────────────────────────


def test_content_hash_is_sha256_hex():
    result = content_hash("hello")
    assert result == hashlib.sha256(b"hello").hexdigest()
    assert len(result) == 64


def test_content_hash_none_treated_as_empty_string():
    assert content_hash(None) == hashlib.sha256(b"").hexdigest()


# ── build_upsert_docs ─────────────────────────────────────────────────────────


def test_empty_rows_returns_empty():
    docs, skipped = build_upsert_docs([], {}, PARTITION_KEY, THREAD_URL, PAGE_NUMBER)
    assert docs == []
    assert skipped == 0


def test_all_new_rows_when_hashes_empty():
    rows = [_make_post(1001, "text one"), _make_post(1002, "text two")]
    docs, skipped = build_upsert_docs(rows, {}, PARTITION_KEY, THREAD_URL, PAGE_NUMBER)

    assert len(docs) == 2
    assert skipped == 0
    assert all(d.embedding is None for d in docs)
    assert all(d.partition_key == PARTITION_KEY for d in docs)


def test_doc_fields_mapped_from_raw_post():
    row = _make_post(1001, "hello world")
    docs, _ = build_upsert_docs([row], {}, PARTITION_KEY, THREAD_URL, PAGE_NUMBER)

    d = docs[0]
    assert d.key == "1001"
    assert d.post_id == 1001
    assert d.content_hash == content_hash("hello world")
    assert d.thread_url == THREAD_URL
    assert d.page_number == PAGE_NUMBER


def test_unchanged_row_is_skipped():
    row = _make_post(1001, "unchanged text")
    existing = {"1001": content_hash("unchanged text")}
    docs, skipped = build_upsert_docs([row], existing, PARTITION_KEY, THREAD_URL, PAGE_NUMBER)

    assert docs == []
    assert skipped == 1


def test_changed_row_is_included():
    row = _make_post(1001, "new text")
    existing = {"1001": content_hash("old text")}
    docs, skipped = build_upsert_docs([row], existing, PARTITION_KEY, THREAD_URL, PAGE_NUMBER)

    assert len(docs) == 1
    assert skipped == 0
    assert docs[0].content_hash == content_hash("new text")


def test_mixed_changed_and_unchanged():
    rows = [_make_post(1001, "same"), _make_post(1002, "changed")]
    existing = {"1001": content_hash("same"), "1002": content_hash("old")}
    docs, skipped = build_upsert_docs(rows, existing, PARTITION_KEY, THREAD_URL, PAGE_NUMBER)

    assert len(docs) == 1
    assert skipped == 1
    assert docs[0].key == "1002"


def test_embedding_always_none_on_upsert_doc():
    row = _make_post(1001, "any text")
    docs, _ = build_upsert_docs([row], {}, PARTITION_KEY, THREAD_URL, PAGE_NUMBER)
    assert docs[0].embedding is None
