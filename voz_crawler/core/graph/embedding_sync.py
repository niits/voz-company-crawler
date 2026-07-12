from langfuse import Langfuse

from voz_crawler.core.entities.arango import EmbedItem, EmbedPatch

EMBEDDING_MODEL = "text-embedding-3-small"
EMBED_BATCH_SIZE = 100  # posts per OpenAI API call


def embed_batch(
    client,
    items: list[EmbedItem],
    model: str = EMBEDDING_MODEL,
    batch_size: int = EMBED_BATCH_SIZE,
    partition_key: str | None = None,
    langfuse: Langfuse | None = None,
) -> list[EmbedPatch]:
    """Pure: embed items in batches, return patches. Caller writes to DB.

    No DB access — designed for in-memory op chains where the write
    happens once at the end of the group. `langfuse` is an already-constructed
    client (from the optional LangfuseResource) — this module never calls
    get_client() itself, so tracing stays off when the caller doesn't inject it.
    """
    patches: list[EmbedPatch] = []
    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]
        if langfuse is not None:
            with langfuse.start_as_current_observation(
                name="compute_embeddings",
                as_type="embedding",
                model=model,
                metadata={"partition_key": partition_key, "post_keys": [b.key for b in batch]},
            ) as obs:
                response = client.embeddings.create(model=model, input=[b.text for b in batch])
                obs.update(usage_details={"input": response.usage.prompt_tokens})
        else:
            response = client.embeddings.create(model=model, input=[b.text for b in batch])
        patches.extend(
            EmbedPatch(
                key=batch[j].key,
                embedding=response.data[j].embedding,
                embedding_model=model,
            )
            for j in range(len(batch))
        )
    return patches
