from voz_crawler.core.entities.arango import EmbedItem, EmbedPatch

EMBEDDING_MODEL = "text-embedding-3-small"
EMBED_BATCH_SIZE = 100  # posts per OpenAI API call


def embed_and_update(
    repo,
    client,
    to_embed: list[EmbedItem],
    model: str = EMBEDDING_MODEL,
    batch_size: int = EMBED_BATCH_SIZE,
) -> int:
    """Embed posts in batches and patch their embedding field in ArangoDB.

    `client` must be an OpenAI client (e.g. from openai.get_client(context)).
    `repo` must be a GraphRepository instance.
    Embeds normalized_own_text (EmbedItem.text is populated from that field by the repository).
    Returns total number of posts embedded.
    """
    total = 0
    for batch_start in range(0, len(to_embed), batch_size):
        batch = to_embed[batch_start : batch_start + batch_size]
        response = client.embeddings.create(model=model, input=[b.text for b in batch])
        patches = [
            EmbedPatch(
                key=batch[i].key,
                embedding=response.data[i].embedding,
                embedding_model=model,
            )
            for i in range(len(batch))
        ]
        repo.update_post_embeddings(patches)
        total += len(batch)
    return total
