#!/usr/bin/env python
"""Smoke test for ArangoGraphRepository against a live ArangoDB instance.

Runs a full round-trip: schema → upsert posts → hash lookup → embedding fetch
→ patch embeddings → insert edges → drop edges.

Usage:
    uv run python scripts/test_arango.py

Environment variables (override defaults for local dev):
    ARANGO_HOST            default: localhost
    ARANGO_PORT            default: 8529
    ARANGO_ROOT_PASSWORD   default: changeme

Exit codes: 0 = all checks passed, 1 = at least one check failed.
"""

import os
import sys
import time

PARTITION_KEY = "smoke:page:1"
DB_NAME = "smoke_voz_graph"


def _check(label: str, condition: bool) -> bool:
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {label}")
    return condition


def main() -> int:
    host = os.getenv("ARANGO_HOST", "localhost")
    port = int(os.getenv("ARANGO_PORT", "8529"))
    root_pw = os.getenv("ARANGO_ROOT_PASSWORD", "changeme")

    print(f"Connecting to ArangoDB at http://{host}:{port} ...")
    try:
        from arango import ArangoClient

        client = ArangoClient(hosts=f"http://{host}:{port}")
        sys_db = client.db("_system", username="root", password=root_pw)
        sys_db.version()  # connectivity check
    except Exception as exc:
        print(f"  [FAIL] Cannot connect: {exc}")
        return 1

    print("  [PASS] Connected")

    # Fresh test database
    if sys_db.has_database(DB_NAME):
        sys_db.delete_database(DB_NAME)
    sys_db.create_database(DB_NAME)
    db = client.db(DB_NAME, username="root", password=root_pw)

    from voz_crawler.core.entities.graph import EmbedPatch, GraphEdge, GraphPost
    from voz_crawler.core.repository.arango_repository import ArangoGraphRepository

    repo = ArangoGraphRepository(db=db)
    all_pass = True

    print("\n── Schema ──────────────────────────────────────────")
    t0 = time.monotonic()
    repo.ensure_schema()
    elapsed = time.monotonic() - t0
    all_pass &= _check("ensure_schema() without error", True)
    all_pass &= _check("posts collection exists", db.has_collection("posts"))
    all_pass &= _check("quotes edge collection exists", db.has_collection("quotes"))
    all_pass &= _check("reply_graph named graph exists", db.has_graph("reply_graph"))
    all_pass &= _check("ensure_schema() idempotent (second call)", True)
    repo.ensure_schema()
    print(f"  (schema setup: {elapsed * 1000:.0f}ms)")

    print("\n── Post upsert ─────────────────────────────────────")
    posts = [
        GraphPost(
            post_id=1001,
            author_username="alice",
            author_id="101",
            posted_at="2024-01-01",
            content_text="Hello from ArangoDB smoke test, long enough for embedding",
            content_hash="hash_1001",
            partition_key=PARTITION_KEY,
            thread_url="https://voz.vn/t/test",
            page_number=1,
            embedding=None,
        ),
        GraphPost(
            post_id=1002,
            author_username="bob",
            author_id="102",
            posted_at="2024-01-01",
            content_text="Reply post, also long enough for embedding purposes here",
            content_hash="hash_1002",
            partition_key=PARTITION_KEY,
            thread_url="https://voz.vn/t/test",
            page_number=1,
            embedding=None,
        ),
    ]
    repo.upsert_posts(posts)
    hashes = repo.get_existing_hashes(PARTITION_KEY)
    all_pass &= _check("upserted 2 posts visible in hashes", len(hashes) == 2)
    all_pass &= _check("hash value correct for post 1001", hashes.get("1001") == "hash_1001")

    print("\n── Embedding fetch ─────────────────────────────────")
    to_embed = repo.fetch_posts_needing_embedding(PARTITION_KEY)
    all_pass &= _check("fetch_posts_needing_embedding returns 2 items", len(to_embed) == 2)
    all_pass &= _check("EmbedItem.post_id is int", isinstance(to_embed[0].post_id, int))

    print("\n── Embedding patch ─────────────────────────────────")
    patches = [EmbedPatch(post_id=1001, embedding=[0.1] * 10, embedding_model="smoke-model")]
    repo.update_post_embeddings(patches)
    still_needing = repo.fetch_posts_needing_embedding(PARTITION_KEY)
    remaining_ids = {i.post_id for i in still_needing}
    all_pass &= _check("post 1001 no longer needs embedding", 1001 not in remaining_ids)
    all_pass &= _check("post 1002 still needs embedding", 1002 in remaining_ids)

    print("\n── Edge insert + drop ──────────────────────────────")
    edges = [
        GraphEdge(
            from_post_id=1002,
            to_post_id=1001,
            edge_key="1002_1001_1",
            quote_ordinal=1,
            confidence=1.0,
            method="html_metadata",
            partition_key=PARTITION_KEY,
        )
    ]
    repo.insert_edges(edges)
    repo.insert_edges(edges)  # duplicate must be ignored
    dropped = repo.drop_partition_edges(PARTITION_KEY)
    all_pass &= _check(f"drop_partition_edges returned {dropped} (>= 1)", dropped >= 1)

    # Cleanup
    sys_db.delete_database(DB_NAME)

    print("\n" + ("=" * 50))
    print("Result: ALL PASSED" if all_pass else "Result: SOME TESTS FAILED")
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
