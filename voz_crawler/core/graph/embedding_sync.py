EMBEDDING_MODEL = "text-embedding-3-small"
EMBED_BATCH_SIZE = 100  # posts per OpenAI API call


def fetch_posts_needing_embedding(db, partition_key: str) -> list[dict]:
    """Return [{key, text}] for posts in this partition that are missing embeddings."""
    cursor = db.aql.execute(
        "FOR p IN posts"
        " FILTER p.partition_key == @pk"
        "   AND p.embedding == null"
        "   AND p.content_text != null"
        "   AND LENGTH(p.content_text) >= 20"
        " RETURN {key: p._key, text: p.content_text}",
        bind_vars={"pk": partition_key},
    )
    return list(cursor)


def embed_and_update(
    posts_col,
    client,
    to_embed: list[dict],
    model: str = EMBEDDING_MODEL,
    batch_size: int = EMBED_BATCH_SIZE,
) -> int:
    """Embed posts in batches and patch their embedding field in ArangoDB.

    `client` must be an OpenAI client (e.g. from openai.get_client(context)).
    Returns total number of posts embedded.
    """
    total = 0
    for batch_start in range(0, len(to_embed), batch_size):
        batch = to_embed[batch_start : batch_start + batch_size]
        response = client.embeddings.create(model=model, input=[b["text"] for b in batch])
        updates = [
            {
                "_key": batch[i]["key"],
                "embedding": response.data[i].embedding,
                "embedding_model": model,
            }
            for i in range(len(batch))
        ]
        posts_col.update_many(updates)
        total += len(batch)
    return total
