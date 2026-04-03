import openai

_DEFAULT_MODEL = "text-embedding-3-small"
_DEFAULT_BATCH = 20


class EmbeddingService:
    """Batch-embeds ArangoDB posts that are missing embeddings."""

    def __init__(
        self,
        client: openai.OpenAI,
        model: str = _DEFAULT_MODEL,
        batch_size: int = _DEFAULT_BATCH,
    ) -> None:
        self._client = client
        self._model = model
        self._batch_size = batch_size

    def compute_missing(self, arango_db) -> dict:
        """Fetch posts with embedding==null, compute embeddings, update collection."""
        cursor = arango_db.aql.execute(
            "FOR p IN Posts FILTER p.embedding == null "
            "RETURN {_key: p._key, text: p.content_text}"
        )
        pending = list(cursor)
        if not pending:
            return {"records_processed": 0}

        posts_col = arango_db.collection("Posts")
        total = 0

        for i in range(0, len(pending), self._batch_size):
            batch = pending[i : i + self._batch_size]
            texts = [p["text"] or "" for p in batch]
            response = self._client.embeddings.create(model=self._model, input=texts)
            for post, emb_obj in zip(batch, response.data):
                posts_col.update({
                    "_key": post["_key"],
                    "embedding": emb_obj.embedding,
                    "embedding_model": self._model,
                })
            total += len(batch)

        return {"records_processed": total}
