def ensure_collections_and_graph(db) -> None:
    """Idempotently create posts vertex, quotes edge collection, reply_graph, and indexes.

    Safe to call on every asset run — all operations are no-ops if already present.
    `db` must be an authenticated python-arango Database handle.

    Indexes created:
      posts[partition_key]  — for efficient partition-scoped queries and cleanup
      quotes[partition_key] — for efficient AQL REMOVE by partition
    """
    if not db.has_collection("posts"):
        db.create_collection("posts")
    if not db.has_collection("quotes"):
        db.create_collection("quotes", edge=True)
    if not db.has_graph("reply_graph"):
        db.create_graph(
            "reply_graph",
            edge_definitions=[
                {
                    "edge_collection": "quotes",
                    "from_vertex_collections": ["posts"],
                    "to_vertex_collections": ["posts"],
                }
            ],
        )

    # Persistent indexes on partition_key — ArangoDB deduplicates by fields,
    # so calling add_persistent_index on an existing index is a no-op.
    posts_col = db.collection("posts")
    posts_col.add_persistent_index(fields=["partition_key"], unique=False)

    quotes_col = db.collection("quotes")
    quotes_col.add_persistent_index(fields=["partition_key"], unique=False)
