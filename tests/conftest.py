"""Shared test fixtures and constants.

Seed data mirrors the rows used in the legacy integration_test.py script,
kept here so all test modules can reference the same baseline dataset.
"""


# ── Constants ─────────────────────────────────────────────────────────────────

TEST_PAGE_URL = "https://integration-test.local/page/1"
TEST_PARTITION_KEY = "test:fixtures"
TEST_THREAD_URL = "https://voz.vn/t/test"
TEST_PAGE_NUMBER = 1

BLOCKQUOTE_HTML = (
    '<div class="bbWrapper">'
    '<blockquote class="bbCodeBlock--quote" data-source="post: 2000">'
    "quoted text"
    "</blockquote>"
    "reply body"
    "</div>"
)

SEED_ROWS = [
    {
        "post_id_on_site": 1001,
        "page_url": TEST_PAGE_URL,
        "author_username": "user_alpha",
        "author_id_on_site": "101",
        "posted_at_raw": "2024-01-01T10:00:00+0700",
        "raw_content_html": "<div>plain post content here</div>",
        "raw_content_text": "plain post content here, long enough for embedding",
    },
    {
        "post_id_on_site": 1002,
        "page_url": TEST_PAGE_URL,
        "author_username": "user_beta",
        "author_id_on_site": "102",
        "posted_at_raw": "2024-01-01T11:00:00+0700",
        "raw_content_html": BLOCKQUOTE_HTML,
        "raw_content_text": "this is a reply to the quoted post, long enough for embedding",
    },
]
