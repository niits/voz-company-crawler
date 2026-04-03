from sqlalchemy import text
from sqlalchemy.engine import Engine


class PostsRepository:
    """SQL queries over the raw.voz__posts table."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def fetch_new_posts(self, after_post_id: int) -> list:
        with self._engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT post_id_on_site, author_username, author_id_on_site, "
                    "posted_at_raw, raw_content_html, raw_content_text "
                    "FROM raw.voz__posts "
                    "WHERE post_id_on_site > :last_id "
                    "ORDER BY post_id_on_site"
                ),
                {"last_id": after_post_id},
            ).fetchall()

    def fetch_html_for_posts(self, after_post_id: int) -> list:
        with self._engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT post_id_on_site, raw_content_html "
                    "FROM raw.voz__posts "
                    "WHERE post_id_on_site > :last_id "
                    "ORDER BY post_id_on_site"
                ),
                {"last_id": after_post_id},
            ).fetchall()
