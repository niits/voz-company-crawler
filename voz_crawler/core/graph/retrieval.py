_TRAVERSAL_AQL = """
FOR v, e, p IN 0..@max_depth ANY CONCAT("Posts/", @post_id)
  GRAPH "reply_graph"
  OPTIONS {uniqueVertices: "global", bfs: true}
  SORT v.posted_at ASC
  RETURN DISTINCT v
"""

_TRAVERSAL_QUOTES_ONLY_AQL = """
FOR v, e, p IN 0..@max_depth ANY CONCAT("Posts/", @post_id)
  quotes
  OPTIONS {uniqueVertices: "global", bfs: true}
  SORT v.posted_at ASC
  RETURN DISTINCT v
"""


def get_thread_context(
    post_id: int,
    arango_db,
    max_depth: int = 10,
    include_implicit: bool = True,
) -> list[dict]:
    """Return all posts in the conversation thread containing post_id."""
    aql = _TRAVERSAL_AQL if include_implicit else _TRAVERSAL_QUOTES_ONLY_AQL
    cursor = arango_db.aql.execute(
        aql,
        bind_vars={"max_depth": max_depth, "post_id": str(post_id)},
    )
    posts = list(cursor)
    return sorted(posts, key=lambda p: p.get("posted_at") or "")
