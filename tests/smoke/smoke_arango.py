"""Smoke tests for the ArangoDB-backed reply graph pipeline.

Verifies end-to-end data flow through the three pipeline stages in the same
order that Dagster assets execute them:

  Stage 1 — sync_posts_to_graph  : RawPost rows → post nodes (hash-diffed)
  Stage 2 — extract_explicit_edges: HTML quotes  → edge documents
  Stage 3 — compute_embeddings   : null-embedding posts → embedded posts

Run manually after starting Docker services:
  docker compose -f docker/dev/docker-compose.yml up arangodb -d
  uv run python tests/smoke/smoke_arango.py

Exit code 0 = all checks passed. Non-zero = at least one failure.
"""

import os
import sys
import traceback

# ── configuration ─────────────────────────────────────────────────────────────

ARANGO_HOST = os.getenv("ARANGO_HOST", "localhost")
ARANGO_PORT = int(os.getenv("ARANGO_PORT", "8529"))
ARANGO_PASSWORD = os.getenv("ARANGO_ROOT_PASSWORD", "change_me")
SMOKE_DB_NAME = "smoke_voz_graph"

PARTITION_KEY = "smoke:page:1"
THREAD_URL = "https://voz.vn/t/smoke-test.000000/"
PAGE_NUMBER = 1

BLOCKQUOTE_HTML = (
    '<div class="bbWrapper">'
    '<blockquote class="bbCodeBlock--quote" data-source="post: 2001">'
    "some quoted text"
    "</blockquote>"
    "reply body long enough to embed"
    "</div>"
)

# ── helpers ───────────────────────────────────────────────────────────────────

_PASS = "\033[32m✓\033[0m"
_FAIL = "\033[31m✗\033[0m"
_failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  {_PASS} {name}")
    else:
        msg = f"{name}" + (f": {detail}" if detail else "")
        print(f"  {_FAIL} {msg}")
        _failures.append(msg)


def section(title: str) -> None:
    print(f"\n── {title} {'─' * max(0, 50 - len(title))}")


# ── stage builders ────────────────────────────────────────────────────────────

def _make_raw_posts():
    from voz_crawler.core.entities.raw_post import RawPost
    from datetime import datetime, timezone

    return [
        RawPost(
            post_id_on_site=2001,
            author_username="smoke_user_a",
            author_id_on_site="901",
            posted_at_raw=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            raw_content_html="<div>original post long enough for embedding check</div>",
            raw_content_text="original post long enough for embedding check",
        ),
        RawPost(
            post_id_on_site=2002,
            author_username="smoke_user_b",
            author_id_on_site="902",
            posted_at_raw=datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
            raw_content_html=BLOCKQUOTE_HTML,
            raw_content_text="reply body long enough to embed",
        ),
    ]


def _make_repo(db):
    from voz_crawler.core.repository.graph_repository import GraphRepository
    return GraphRepository(db=db)


# ── smoke stages ─────────────────────────────────────────────────────────────

def smoke_connectivity(db) -> bool:
    """Stage 0: verify we can reach ArangoDB and our test DB exists."""
    section("Stage 0 — Connectivity")
    try:
        props = db.properties()
        check("database reachable", True)
        check("correct database name", props["name"] == SMOKE_DB_NAME)
        return True
    except Exception as exc:
        check("database reachable", False, str(exc))
        return False


def smoke_schema(db) -> None:
    """Stage 0b: ensure_schema creates expected collections and graph."""
    section("Stage 0b — Schema")
    repo = _make_repo(db)
    repo.ensure_schema()
    check("posts collection exists", db.has_collection("posts"))
    check("quotes edge collection exists", db.has_collection("quotes"))
    check("reply_graph exists", db.has_graph("reply_graph"))
    check("ensure_schema is idempotent", True)  # second call below must not raise
    repo.ensure_schema()


def smoke_stage1_post_sync(db) -> None:
    """Stage 1: simulate sync_posts_to_graph — RawPosts → ArangoPost nodes."""
    section("Stage 1 — Post Sync (sync_posts_to_graph)")
    from voz_crawler.core.graph.post_sync import build_upsert_docs

    repo = _make_repo(db)
    rows = _make_raw_posts()
    existing_hashes = repo.get_existing_hashes(PARTITION_KEY)
    docs, skipped = build_upsert_docs(rows, existing_hashes, PARTITION_KEY, THREAD_URL, PAGE_NUMBER)

    check("build_upsert_docs returns 2 docs on first run", len(docs) == 2, f"got {len(docs)}")
    check("no posts skipped on first run", skipped == 0, f"skipped={skipped}")
    repo.upsert_posts(docs)

    hashes = repo.get_existing_hashes(PARTITION_KEY)
    check("post 2001 persisted", "2001" in hashes, f"keys={list(hashes)}")
    check("post 2002 persisted", "2002" in hashes)

    # Re-run with same rows — all should be skipped (hash match)
    docs2, skipped2 = build_upsert_docs(rows, hashes, PARTITION_KEY, THREAD_URL, PAGE_NUMBER)
    check("re-sync skips unchanged posts", skipped2 == 2, f"skipped={skipped2}")
    check("re-sync upserts nothing", len(docs2) == 0, f"docs={len(docs2)}")


def smoke_stage2_edge_sync(db) -> None:
    """Stage 2: simulate extract_explicit_edges — HTML quotes → ArangoEdge documents."""
    section("Stage 2 — Edge Sync (extract_explicit_edges)")
    from voz_crawler.core.entities.raw_post import RawPost
    from voz_crawler.core.graph.edge_sync import build_edges
    from datetime import datetime, timezone

    rows_with_html = _make_raw_posts()
    # edge_sync needs raw_content_html — which post_sync drops; use the original rows
    edges = build_edges(rows_with_html, PARTITION_KEY)
    check("build_edges finds 1 edge", len(edges) == 1, f"got {len(edges)}")
    check("edge _from = posts/2002", edges[0].from_vertex == "posts/2002", edges[0].from_vertex)
    check("edge _to = posts/2001", edges[0].to_vertex == "posts/2001", edges[0].to_vertex)

    repo = _make_repo(db)
    dropped_before = repo.drop_partition_edges(PARTITION_KEY)
    check("drop before insert returns 0 (fresh db)", dropped_before == 0, f"got {dropped_before}")

    repo.insert_edges(edges)
    dropped = repo.drop_partition_edges(PARTITION_KEY)
    check("drop after insert returns 1", dropped == 1, f"got {dropped}")

    # Idempotency: re-insert + re-drop should work the same
    repo.insert_edges(edges)
    repo.insert_edges(edges)  # duplicate — should be ignored
    count = db.collection("quotes").count()
    check("duplicate insert deduplication", count == 1, f"count={count}")


def smoke_stage3_embeddings(db) -> None:
    """Stage 3: verify fetch_posts_needing_embedding and update_post_embeddings."""
    section("Stage 3 — Embeddings (compute_embeddings, without real OpenAI call)")
    from voz_crawler.core.entities.arango import EmbedPatch

    repo = _make_repo(db)
    needing = repo.fetch_posts_needing_embedding(PARTITION_KEY)
    check("2 posts need embedding after sync", len(needing) == 2, f"got {len(needing)}")

    # Simulate what embed_and_update does (without hitting OpenAI)
    fake_patches = [
        EmbedPatch(key=item.key, embedding=[0.1, 0.2, 0.3], embedding_model="smoke-model")
        for item in needing
    ]
    repo.update_post_embeddings(fake_patches)

    needing_after = repo.fetch_posts_needing_embedding(PARTITION_KEY)
    check("0 posts need embedding after patching", len(needing_after) == 0, f"got {len(needing_after)}")

    doc = db.collection("posts").get("2001")
    check("embedding vector persisted on post 2001", doc.get("embedding") == [0.1, 0.2, 0.3])
    check("embedding_model persisted", doc.get("embedding_model") == "smoke-model")


# ── entrypoint ────────────────────────────────────────────────────────────────

def main() -> int:
    print("=" * 56)
    print("  Smoke test — ArangoDB reply graph pipeline")
    print("=" * 56)

    # Connect and create a fresh smoke database
    try:
        from arango import ArangoClient
        client = ArangoClient(hosts=f"http://{ARANGO_HOST}:{ARANGO_PORT}")
        sys_db = client.db("_system", username="root", password=ARANGO_PASSWORD)
        if sys_db.has_database(SMOKE_DB_NAME):
            sys_db.delete_database(SMOKE_DB_NAME)
        sys_db.create_database(SMOKE_DB_NAME)
        db = client.db(SMOKE_DB_NAME, username="root", password=ARANGO_PASSWORD)
    except Exception as exc:
        print(f"\n{_FAIL} Cannot connect to ArangoDB at {ARANGO_HOST}:{ARANGO_PORT}")
        print(f"  {exc}")
        return 1

    try:
        if not smoke_connectivity(db):
            return 1
        smoke_schema(db)
        smoke_stage1_post_sync(db)
        smoke_stage2_edge_sync(db)
        smoke_stage3_embeddings(db)
    except Exception:
        print(f"\n{_FAIL} Unexpected exception:")
        traceback.print_exc()
        _failures.append("unexpected exception — see traceback above")
    finally:
        # Always clean up the smoke database
        try:
            sys_db.delete_database(SMOKE_DB_NAME)
        except Exception:
            pass

    print("\n" + "=" * 56)
    if _failures:
        print(f"  {_FAIL} {len(_failures)} check(s) failed:")
        for f in _failures:
            print(f"    • {f}")
        return 1
    else:
        print(f"  {_PASS} All checks passed.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
