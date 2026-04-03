from voz_crawler.core.crawl.parser import extract_quote_edges
from voz_crawler.core.db.posts import PostsRepository


class GraphSyncService:
    """Syncs PostgreSQL posts into ArangoDB and extracts explicit quote edges."""

    def __init__(self, arango_db, posts_repo: PostsRepository) -> None:
        self._db = arango_db
        self._repo = posts_repo

    def sync_posts(self) -> dict:
        """Incremental sync: copy new posts from PostgreSQL into ArangoDB Posts."""
        cursor = self._db.aql.execute(
            "RETURN MAX(FOR p IN Posts RETURN p.post_id)"
        )
        last_id = int(next(iter(cursor)) or 0)

        rows = self._repo.fetch_new_posts(after_post_id=last_id)
        if not rows:
            return {"records_processed": 0}

        posts_col = self._db.collection("Posts")
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

        return {
            "records_processed": len(rows),
            "last_processed_post_id": rows[-1].post_id_on_site,
        }

    def extract_explicit_edges(self) -> dict:
        """Parse HTML quotes and upsert explicit edges into the quotes collection."""
        cursor = self._db.aql.execute(
            "RETURN MAX(FOR e IN quotes RETURN TO_NUMBER(PARSE_IDENTIFIER(e._from).key))"
        )
        last_from_id = int(next(iter(cursor)) or 0)

        rows = self._repo.fetch_html_for_posts(after_post_id=last_from_id)
        quotes_col = self._db.collection("quotes")
        count = 0
        for row in rows:
            edges = extract_quote_edges(row.raw_content_html or "")
            for edge in edges:
                quotes_col.insert(
                    {
                        "_from": f"Posts/{edge['from_post_id']}",
                        "_to": f"Posts/{edge['to_post_id']}",
                        "source_author": edge["source_author"],
                        "target_author": edge["target_author"],
                        "quote_ordinal": edge["quote_ordinal"],
                        "confidence": 1.0,
                        "method": "html_metadata",
                    },
                    overwrite=True,
                )
                count += 1

        return {"edges_processed": count}
