import json
import os

from dagster import AssetExecutionContext, MetadataValue, asset
from openai import OpenAI
from sqlalchemy import create_engine, text

from voz_crawler.utils.arango_setup import ensure_schema
from voz_crawler.utils.html_parser import extract_quote_edges

from ..resources.arango import ArangoResource
from ..resources.postgres import PostgresResource

_EMBED_MODEL = "text-embedding-3-small"
_EMBED_BATCH = 20

_IMPLICIT_AQL = """
FOR doc IN Posts
  FILTER doc.posted_at >= DATE_SUBTRACT(@post_time, 10, "hour")
  FILTER doc.posted_at < @post_time
  FILTER doc.author_username != @author
  FILTER doc.embedding != null
  SORT APPROX_NEAR_COSINE(doc.embedding, @query_embedding) DESC
  LIMIT 10
  LET sim = COSINE_SIMILARITY(doc.embedding, @query_embedding)
  FILTER sim >= 0.65
  LET delta_h = DATE_DIFF(doc.posted_at, @post_time, "h")
  RETURN MERGE(doc, {
    similarity: sim,
    temporal_score: EXP(-delta_h / 1.36),
    combined_score: 0.6 * sim + 0.4 * EXP(-delta_h / 1.36)
  })
"""

_LLM_PROMPT = """\
Below are {n} consecutive posts from a Vietnamese IT company review thread.

{posts}

Identify pairs (A, B) where post B is an implicit reply to post A (the user did not \
press the quote button but is directly responding to the content of post A).

Only return pairs with confidence >= 0.7. Do not create an edge if an HTML quote already exists.

Output JSON only:
[{{"from": <post_id>, "to": <post_id>, "confidence": 0.85, "reason": "..."}}]
"""


@asset(
    group_name="reply_graph",
    description="Sync new posts from PostgreSQL into ArangoDB Posts collection (incremental).",
)
def sync_posts_to_arango(
    context: AssetExecutionContext,
    arango: ArangoResource,
    postgres: PostgresResource,
) -> None:
    db = arango.get_client()
    ensure_schema(db)

    # Cursor: max post_id already in ArangoDB (MAX of empty collection returns null → 0)
    cursor = db.aql.execute("RETURN MAX(FOR p IN Posts RETURN p.post_id)")
    last_id = int(next(cursor) or 0)
    context.log.info(f"Syncing posts with post_id_on_site > {last_id}")

    engine = create_engine(postgres.url)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT post_id_on_site, author_username, author_id_on_site, "
                "posted_at_raw, raw_content_html, raw_content_text "
                "FROM raw.voz__posts "
                "WHERE post_id_on_site > :last_id "
                "ORDER BY post_id_on_site"
            ),
            {"last_id": last_id},
        ).fetchall()

    if not rows:
        context.log.info("No new posts to sync.")
        context.add_output_metadata({"records_processed": MetadataValue.int(0)})
        engine.dispose()
        return

    posts_col = db.collection("Posts")
    for row in rows:
        posts_col.insert(
            {
                "_key": str(row.post_id_on_site),
                "post_id": row.post_id_on_site,
                "author_username": row.author_username,
                "author_id": row.author_id_on_site,
                "posted_at": row.posted_at_raw,
                "content_text": row.raw_content_text,
                "embedding": None,
                "embedding_model": None,
            },
            overwrite=True,
        )

    engine.dispose()
    context.log.info(f"Synced {len(rows)} posts.")
    context.add_output_metadata({
        "last_processed_post_id": MetadataValue.int(rows[-1].post_id_on_site),
        "records_processed": MetadataValue.int(len(rows)),
    })


@asset(
    deps=["sync_posts_to_arango"],
    group_name="reply_graph",
    description="Parse HTML quote blocks and upsert explicit edges into the quotes collection.",
)
def extract_explicit_edges(
    context: AssetExecutionContext,
    arango: ArangoResource,
    postgres: PostgresResource,
) -> None:
    db = arango.get_client()

    cursor = db.aql.execute(
        "RETURN MAX(FOR e IN quotes RETURN TO_NUMBER(PARSE_IDENTIFIER(e._from).key))"
    )
    last_from_id = int(next(cursor) or 0)
    context.log.info(f"Extracting explicit edges for post_id > {last_from_id}")

    engine = create_engine(postgres.url)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT post_id_on_site, raw_content_html "
                "FROM raw.voz__posts "
                "WHERE post_id_on_site > :last_id "
                "ORDER BY post_id_on_site"
            ),
            {"last_id": last_from_id},
        ).fetchall()
    engine.dispose()

    quotes_col = db.collection("quotes")
    count = 0
    for row in rows:
        edges = extract_quote_edges(row.raw_content_html or "")
        for edge in edges:
            doc = {
                "_from": f"Posts/{edge['from_post_id']}",
                "_to": f"Posts/{edge['to_post_id']}",
                "source_author": edge["source_author"],
                "target_author": edge["target_author"],
                "quote_ordinal": edge["quote_ordinal"],
                "confidence": 1.0,
                "method": "html_metadata",
            }
            quotes_col.insert(doc, overwrite=True)
            count += 1

    context.log.info(f"Upserted {count} quote edges.")
    context.add_output_metadata({"edges_processed": MetadataValue.int(count)})


@asset(
    deps=["sync_posts_to_arango"],
    group_name="reply_graph",
    description="Batch-embed posts without embeddings using text-embedding-3-small.",
)
def compute_embeddings(
    context: AssetExecutionContext,
    arango: ArangoResource,
) -> None:
    db = arango.get_client()

    cursor = db.aql.execute(
        "FOR p IN Posts FILTER p.embedding == null RETURN {_key: p._key, text: p.content_text}"
    )
    pending = list(cursor)
    if not pending:
        context.log.info("No posts need embeddings.")
        context.add_output_metadata({"records_processed": MetadataValue.int(0)})
        return

    context.log.info(f"Embedding {len(pending)} posts.")
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    posts_col = db.collection("Posts")
    total = 0

    for i in range(0, len(pending), _EMBED_BATCH):
        batch = pending[i : i + _EMBED_BATCH]
        texts = [p["text"] or "" for p in batch]
        response = client.embeddings.create(model=_EMBED_MODEL, input=texts)
        for post, emb_obj in zip(batch, response.data):
            posts_col.update({
                "_key": post["_key"],
                "embedding": emb_obj.embedding,
                "embedding_model": _EMBED_MODEL,
            })
        total += len(batch)

    context.log.info(f"Embedded {total} posts.")
    context.add_output_metadata({"records_processed": MetadataValue.int(total)})


@asset(
    deps=["compute_embeddings"],
    group_name="reply_graph",
    description="Detect implicit replies via temporal+ANN+LLM and upsert into implicit_replies.",
)
def detect_implicit_edges(
    context: AssetExecutionContext,
    arango: ArangoResource,
) -> None:
    db = arango.get_client()
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    all_cursor = db.aql.execute(
        "FOR p IN Posts FILTER p.embedding != null "
        "SORT p.posted_at ASC "
        "RETURN {_key: p._key, post_id: p.post_id, author_username: p.author_username, "
        "posted_at: p.posted_at, content_text: p.content_text, embedding: p.embedding}"
    )
    posts = list(all_cursor)
    if not posts:
        context.log.info("No embedded posts to process.")
        context.add_output_metadata({"edges_upserted": MetadataValue.int(0)})
        return

    # Build set of existing explicit quote pairs for dedup
    existing_quotes: set[tuple[str, str]] = set()
    qcursor = db.aql.execute("FOR e IN quotes RETURN {from: e._from, to: e._to}")
    for edge in qcursor:
        existing_quotes.add((edge["from"], edge["to"]))

    implicit_col = db.collection("implicit_replies")
    total_edges = 0
    PAGE = 20

    for page_start in range(0, len(posts), PAGE):
        page = posts[page_start : page_start + PAGE]
        page = [p for p in page if len(p.get("content_text") or "") >= 20]
        if not page:
            continue

        # Check if any candidates exist for this page
        candidates_exist = False
        for post in page:
            cands = list(db.aql.execute(
                _IMPLICIT_AQL,
                bind_vars={
                    "post_time": post["posted_at"],
                    "author": post["author_username"],
                    "query_embedding": post["embedding"],
                },
            ))
            if cands:
                candidates_exist = True
                break

        if not candidates_exist:
            continue

        # Stage 3: single LLM call for the full page
        posts_text = "\n\n".join(
            f"[{p['post_id']}] {p['author_username']}: {(p['content_text'] or '')[:300]}"
            for p in page
        )
        prompt = _LLM_PROMPT.format(n=len(page), posts=posts_text)

        pairs: list[dict] = []
        for attempt in range(2):
            try:
                resp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                )
                pairs = json.loads(resp.choices[0].message.content.strip())
                break
            except Exception as exc:
                if attempt == 1:
                    context.log.warning(
                        f"LLM call failed for page at post {page[0]['post_id']}: {exc}. Skipping."
                    )
                    pairs = []

        for pair in pairs:
            if pair.get("confidence", 0) < 0.75:
                continue
            from_key = f"Posts/{pair['from']}"
            to_key = f"Posts/{pair['to']}"
            if (from_key, to_key) in existing_quotes:
                continue
            implicit_col.insert(
                {
                    "_from": from_key,
                    "_to": to_key,
                    "llm_confidence": pair["confidence"],
                    "llm_reasoning": pair.get("reason", ""),
                    "method": "semantic",
                },
                overwrite=True,
            )
            total_edges += 1

    context.log.info(f"Upserted {total_edges} implicit reply edges.")
    context.add_output_metadata({"edges_upserted": MetadataValue.int(total_edges)})
