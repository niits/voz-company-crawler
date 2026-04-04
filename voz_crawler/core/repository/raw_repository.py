from sqlalchemy import Column, MetaData, String, Table, select
from sqlalchemy.engine import Engine

from voz_crawler.core.entities.raw_post import RawPost


class RawRepository:
    """Read-only access to the raw ingestion schema in PostgreSQL.

    Constructed by PostgresResource.get_repository() — not instantiated directly.
    The engine lifecycle (create/dispose) is managed by PostgresResource.

    The target table is injected at construction time so the class stays free
    of any hard-coded table names; dlt-generated names (e.g. voz__posts) are
    resolved by the resource configuration.
    """

    def __init__(self, engine: Engine, schema: str, table: str) -> None:
        self._engine = engine
        metadata = MetaData()
        self._table = Table(
            table,
            metadata,
            Column("post_id_on_site", String),
            Column("author_username", String),
            Column("author_id_on_site", String),
            Column("posted_at_raw", String),
            Column("raw_content_text", String),
            Column("raw_content_html", String),
            Column("page_url", String),
            schema=schema,
        )

    def fetch_posts(self, page_url: str) -> list[RawPost]:
        """Return all posts for a given page URL, ordered by post_id_on_site."""
        t = self._table
        stmt = (
            select(
                t.c.post_id_on_site,
                t.c.author_username,
                t.c.author_id_on_site,
                t.c.posted_at_raw,
                t.c.raw_content_text,
            )
            .where(t.c.page_url == page_url)
            .order_by(t.c.post_id_on_site)
        )
        with self._engine.connect() as conn:
            return [
                RawPost(
                    post_id_on_site=r["post_id_on_site"],
                    author_username=r["author_username"],
                    author_id_on_site=r["author_id_on_site"],
                    posted_at_raw=r["posted_at_raw"],
                    raw_content_text=r["raw_content_text"],
                    raw_content_html=None,
                )
                for r in conn.execute(stmt).mappings()
            ]

    def fetch_posts_with_blockquotes(self, page_url: str) -> list[RawPost]:
        """Return posts that contain XenForo quote blocks for a given page URL."""
        t = self._table
        stmt = (
            select(t.c.post_id_on_site, t.c.raw_content_html)
            .where(t.c.page_url == page_url)
            .where(t.c.raw_content_html.like("%blockquote%"))
            .order_by(t.c.post_id_on_site)
        )
        with self._engine.connect() as conn:
            return [
                RawPost(
                    post_id_on_site=r["post_id_on_site"],
                    author_username=None,
                    author_id_on_site=None,
                    posted_at_raw=None,
                    raw_content_text=None,
                    raw_content_html=r["raw_content_html"],
                )
                for r in conn.execute(stmt).mappings()
            ]
