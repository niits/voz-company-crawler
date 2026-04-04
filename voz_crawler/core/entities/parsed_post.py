from sqlmodel import SQLModel


class ParsedPost(SQLModel):
    """Validated entity for a post scraped from XenForo HTML.

    Produced by html_parser.extract_posts() before any DB interaction.
    """

    post_id_on_site: str | None
    post_position: int
    author_username: str | None
    author_id_on_site: str | None
    posted_at_raw: str | None
    raw_content_html: str
    raw_content_text: str
