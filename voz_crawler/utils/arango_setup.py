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

    posts_col = db.collection("Posts")
    existing_types = {idx["type"] for idx in posts_col.indexes()}
    if "vector" not in existing_types:
        posts_col.add_index({
            "type": "vector",
            "fields": ["embedding"],
            "params": {
                "metric": "cosine",
                "dimension": 1536,
                "nLists": 100,
            },
        })
