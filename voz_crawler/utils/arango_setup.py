_VECTOR_N_LISTS = 100


def _ensure_vector_index(db) -> None:
    """Create the vector index on Posts.embedding if enough documents exist.

    FAISS IVF index requires at least nLists training points. Skips silently
    on empty or small collections — the index will be created on a later run
    once enough embedded posts are present.
    """
    posts_col = db.collection("Posts")
    existing_types = {idx["type"] for idx in posts_col.indexes()}
    if "vector" in existing_types:
        return

    count = posts_col.count()
    if count < _VECTOR_N_LISTS:
        return  # Not enough data yet — will retry on next asset run

    posts_col.add_index({
        "type": "vector",
        "fields": ["embedding"],
        "params": {
            "metric": "cosine",
            "dimension": 1536,
            "nLists": _VECTOR_N_LISTS,
        },
    })


def ensure_schema(db) -> None:
    """Idempotently create ArangoDB collections, named graph, and vector index.

    Safe to call on every asset run — all operations check existence first.
    """
    if not db.has_collection("Posts"):
        db.create_collection("Posts")

    if not db.has_collection("quotes"):
        db.create_collection("quotes", edge=True)

    if not db.has_collection("implicit_replies"):
        db.create_collection("implicit_replies", edge=True)

    if not db.has_graph("reply_graph"):
        db.create_graph(
            "reply_graph",
            edge_definitions=[
                {
                    "edge_collection": "quotes",
                    "from_vertex_collections": ["Posts"],
                    "to_vertex_collections": ["Posts"],
                },
                {
                    "edge_collection": "implicit_replies",
                    "from_vertex_collections": ["Posts"],
                    "to_vertex_collections": ["Posts"],
                },
            ],
        )

    _ensure_vector_index(db)
