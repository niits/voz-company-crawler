import os

from dagster import AssetExecutionContext, MetadataValue, asset
from sqlalchemy import create_engine, text

from voz_crawler.utils.arango_setup import ensure_schema

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
