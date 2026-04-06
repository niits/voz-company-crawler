import hashlib

from voz_crawler.core.entities.graph import GraphPost
from voz_crawler.core.entities.raw_post import RawPost


def content_hash(raw_text: str | None) -> str:
    return hashlib.sha256((raw_text or "").encode()).hexdigest()


def build_upsert_docs(
    rows: list[RawPost],
    existing_hashes: dict[str, str],
    partition_key: str,
    thread_url: str,
    page_number: int,
) -> tuple[list[GraphPost], int]:
    """Diff rows against existing hashes.

    Returns (posts_to_upsert, skipped_count).
    Changed or new posts are included with embedding=None to trigger re-embedding.
    Unchanged posts (hash match) are skipped.
    """
    to_upsert: list[GraphPost] = []
    skipped = 0
    for r in rows:
        new_hash = content_hash(r.raw_content_text)
        if existing_hashes.get(str(r.post_id_on_site)) == new_hash:
            skipped += 1
            continue
        to_upsert.append(
            GraphPost(
                post_id=r.post_id_on_site,
                author_username=r.author_username,
                author_id=r.author_id_on_site,
                posted_at=r.posted_at_raw.isoformat() if r.posted_at_raw else None,
                content_text=r.raw_content_text,
                content_hash=new_hash,
                partition_key=partition_key,
                thread_url=thread_url,
                page_number=page_number,
                embedding=None,
            )
        )
    return to_upsert, skipped
