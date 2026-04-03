import hashlib

from sqlalchemy import create_engine, text


def content_hash(raw_text: str | None) -> str:
    return hashlib.sha256((raw_text or "").encode()).hexdigest()


def fetch_posts(engine_url: str, page_url: str) -> list:
    """Query raw.voz__posts for all posts on a given page URL."""
    engine = create_engine(engine_url)
    try:
        with engine.connect() as conn:
            return list(
                conn.execute(
                    text(
                        "SELECT post_id_on_site, author_username, author_id_on_site,"
                        " posted_at_raw, raw_content_text"
                        " FROM raw.voz__posts"
                        " WHERE page_url = :page_url"
                        " ORDER BY post_id_on_site"
                    ),
                    {"page_url": page_url},
                ).fetchall()
            )
    finally:
        engine.dispose()


def get_existing_hashes(db, partition_key: str) -> dict[str, str]:
    """Return {_key: content_hash} for all posts already in ArangoDB for this partition."""
    cursor = db.aql.execute(
        "FOR p IN posts FILTER p.partition_key == @pk RETURN {k: p._key, h: p.content_hash}",
        bind_vars={"pk": partition_key},
    )
    return {doc["k"]: doc["h"] for doc in cursor}


def build_upsert_docs(
    rows: list,
    existing_hashes: dict[str, str],
    partition_key: str,
    thread_url: str,
    page_number: int,
) -> tuple[list[dict], int]:
    """Diff rows against existing hashes.

    Returns (docs_to_upsert, skipped_count).
    Changed or new posts are included with embedding=None to trigger re-embedding.
    Unchanged posts (hash match) are skipped.
    """
    to_upsert = []
    skipped = 0
    for r in rows:
        key = str(r.post_id_on_site)
        new_hash = content_hash(r.raw_content_text)
        if existing_hashes.get(key) == new_hash:
            skipped += 1
            continue
        to_upsert.append(
            {
                "_key": key,
                "post_id": r.post_id_on_site,
                "author_username": r.author_username,
                "author_id": r.author_id_on_site,
                "posted_at": r.posted_at_raw,
                "content_text": r.raw_content_text,
                "content_hash": new_hash,
                "partition_key": partition_key,
                "thread_url": thread_url,
                "page_number": page_number,
                "embedding": None,  # reset — triggers re-embedding in compute_embeddings
            }
        )
    return to_upsert, skipped


def upsert_posts(posts_col, docs: list[dict]) -> None:
    """Bulk-upsert post documents. Uses replace semantics to reset embedding on changes."""
    if docs:
        posts_col.import_bulk(docs, on_duplicate="replace")
