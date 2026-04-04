from datetime import datetime

from sqlmodel import SQLModel


class RawPost(SQLModel):
    """Validated entity for a post row read from the PostgreSQL raw schema.

    Both repository methods return this type; fields not selected in a given
    query are passed as None explicitly at the construction site.
    """

    post_id_on_site: int
    author_username: str | None
    author_id_on_site: str | None
    posted_at_raw: datetime | None
    raw_content_text: str | None
    raw_content_html: str | None
