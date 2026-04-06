#!/usr/bin/env python
"""Smoke test for Neo4jGraphRepository against a live Neo4j instance.

Runs a full round-trip: schema → upsert posts → hash lookup → embedding fetch
→ patch embeddings → insert edges → drop edges.

Usage:
    uv run python scripts/test_neo4j.py

Environment variables (override defaults for local dev):
    NEO4J_URI        default: bolt://localhost:7687
    NEO4J_USER       default: neo4j
    NEO4J_PASSWORD   default: changeme

Note: On first run with the GenAI plugin, Neo4j downloads the plugin at startup
— allow ~30 seconds for the container to become ready.

Exit codes: 0 = all checks passed, 1 = at least one check failed.
"""

import os
import sys
import time

PARTITION_KEY = "smoke:page:1"
DB_NAME = "smoketest"  # lowercase, no special chars for Neo4j community


def _check(label: str, condition: bool) -> bool:
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {label}")
    return condition


def main() -> int:
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "changeme")

    print(f"Connecting to Neo4j at {uri} ...")
    try:
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
    except Exception as exc:
        print(f"  [FAIL] Cannot connect: {exc}")
        return 1

    print("  [PASS] Connected")

    # Create fresh test database
    try:
        with driver.session(database="system") as s:
            s.run("DROP DATABASE $db IF EXISTS", db=DB_NAME)
            s.run("CREATE DATABASE $db IF NOT EXISTS", db=DB_NAME)
    except Exception as exc:
        # Community edition: may only support default "neo4j" database
        print(f"  [WARN] Could not create separate test DB ({exc}), using 'neo4j'")
        global DB_NAME
        DB_NAME = "neo4j"
        with driver.session(database=DB_NAME) as s:
            s.run("MATCH (n) DETACH DELETE n")  # wipe clean

    from voz_crawler.core.entities.graph import EmbedPatch, GraphEdge, GraphPost
    from voz_crawler.core.repository.neo4j_repository import Neo4jGraphRepository

    repo = Neo4jGraphRepository(driver=driver, database=DB_NAME)
    all_pass = True

    print("\n── Schema ──────────────────────────────────────────")
    t0 = time.monotonic()
    try:
        repo.ensure_schema()
        elapsed = time.monotonic() - t0
        all_pass &= _check("ensure_schema() without error", True)
        print(f"  (schema setup: {elapsed * 1000:.0f}ms)")
    except Exception as exc:
        all_pass &= _check(f"ensure_schema() raised: {exc}", False)

    all_pass &= _check("ensure_schema() idempotent (second call)", True)
    try:
        repo.ensure_schema()
    except Exception as exc:
        all_pass &= _check(f"second ensure_schema() raised: {exc}", False)

    # Verify constraint exists
    with driver.session(database=DB_NAME) as s:
        result = s.run("SHOW CONSTRAINTS YIELD name WHERE name = 'post_id_unique' RETURN name")
        constraint_exists = result.single() is not None
    all_pass &= _check("post_id_unique constraint created", constraint_exists)

    print("\n── Post upsert ─────────────────────────────────────")
    posts = [
        GraphPost(
            post_id=1001,
            author_username="alice",
            author_id="101",
            posted_at="2024-01-01",
            content_text="Hello from Neo4j smoke test, long enough for embedding",
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

    print("\n── Replace semantics ───────────────────────────────")
    updated = GraphPost(
        post_id=1001,
        author_username="alice",
        author_id="101",
        posted_at="2024-01-01",
        content_text="Updated content",
        content_hash="hash_1001_v2",
        partition_key=PARTITION_KEY,
        thread_url="https://voz.vn/t/test",
        page_number=1,
        embedding=None,
    )
    repo.upsert_posts([updated])
    hashes2 = repo.get_existing_hashes(PARTITION_KEY)
    all_pass &= _check("re-upsert replaces hash", hashes2.get("1001") == "hash_1001_v2")

    print("\n── Embedding fetch ─────────────────────────────────")
    to_embed = repo.fetch_posts_needing_embedding(PARTITION_KEY)
    all_pass &= _check("fetch_posts_needing_embedding returns items", len(to_embed) >= 1)
    all_pass &= _check("EmbedItem.post_id is int", isinstance(to_embed[0].post_id, int))

    print("\n── Embedding patch ─────────────────────────────────")
    patches = [EmbedPatch(post_id=1001, embedding=[0.1] * 10, embedding_model="smoke-model")]
    repo.update_post_embeddings(patches)
    still_needing = repo.fetch_posts_needing_embedding(PARTITION_KEY)
    all_pass &= _check(
        "post 1001 no longer needs embedding",
        1001 not in {i.post_id for i in still_needing},
    )

    print("\n── Edge insert + deduplication + drop ──────────────")
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
    repo.insert_edges(edges)  # duplicate MERGE must be silently ignored

    # Verify exactly one relationship
    with driver.session(database=DB_NAME) as s:
        result = s.run("MATCH ()-[r:QUOTES {edge_key: '1002_1001_1'}]->() RETURN count(r) AS cnt")
        cnt = result.single()["cnt"]
    all_pass &= _check(f"exactly 1 QUOTES relationship after double-insert (got {cnt})", cnt == 1)

    dropped = repo.drop_partition_edges(PARTITION_KEY)
    all_pass &= _check(f"drop_partition_edges returned {dropped} (>= 1)", dropped >= 1)

    # Cleanup
    try:
        with driver.session(database="system") as s:
            s.run("DROP DATABASE $db IF EXISTS", db=DB_NAME)
    except Exception:
        pass
    driver.close()

    print("\n" + ("=" * 50))
    print("Result: ALL PASSED" if all_pass else "Result: SOME TESTS FAILED")
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
