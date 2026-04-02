import os

from dagster import AssetExecutionContext, MetadataValue, asset
from sqlalchemy import create_engine, text

from voz_crawler.utils.arango_setup import ensure_schema
from voz_crawler.utils.html_parser import extract_quote_edges

from ..resources.arango import ArangoResource
from ..resources.postgres import PostgresResource


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
