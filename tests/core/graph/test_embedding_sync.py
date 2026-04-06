"""Unit tests for voz_crawler.core.graph.embedding_sync.embed_and_update.

All external dependencies (OpenAI client, GraphRepository) are mocked.
Tests verify:
- batch slicing and API call count
- EmbedPatch construction (key, embedding, model)
- repo.update_post_embeddings is called with correct payloads
- empty input short-circuits without calling the API
"""

from unittest.mock import MagicMock, call

from voz_crawler.core.entities.arango import EmbedItem, EmbedPatch
from voz_crawler.core.graph.embedding_sync import EMBEDDING_MODEL, embed_and_update


# ── helpers ───────────────────────────────────────────────────────────────────


def _items(n: int) -> list[EmbedItem]:
    return [EmbedItem(key=str(i), text=f"text for post {i}") for i in range(n)]


def _openai_client(embeddings: list[list[float]]) -> MagicMock:
    """Return a mock OpenAI client whose embeddings.create() returns ``embeddings``."""
    client = MagicMock()
    response = MagicMock()
    response.data = [MagicMock(embedding=e) for e in embeddings]
    client.embeddings.create.return_value = response
    return client


# ── empty input ───────────────────────────────────────────────────────────────


def test_empty_input_returns_zero():
    repo = MagicMock()
    client = MagicMock()
    assert embed_and_update(repo, client, []) == 0


def test_empty_input_never_calls_api():
    repo = MagicMock()
    client = MagicMock()
    embed_and_update(repo, client, [])
    client.embeddings.create.assert_not_called()
    repo.update_post_embeddings.assert_not_called()


# ── single batch ─────────────────────────────────────────────────────────────


def test_single_batch_returns_correct_count():
    items = _items(3)
    client = _openai_client([[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]])
    repo = MagicMock()
    assert embed_and_update(repo, client, items) == 3


def test_single_batch_calls_api_once():
    items = _items(3)
    client = _openai_client([[0.1], [0.2], [0.3]])
    repo = MagicMock()
    embed_and_update(repo, client, items)
    assert client.embeddings.create.call_count == 1


def test_single_batch_passes_texts_to_api():
    items = _items(2)
    client = _openai_client([[0.1], [0.2]])
    repo = MagicMock()
    embed_and_update(repo, client, items)
    _, kwargs = client.embeddings.create.call_args
    assert kwargs["input"] == ["text for post 0", "text for post 1"]


def test_single_batch_uses_default_model():
    items = _items(1)
    client = _openai_client([[0.1]])
    repo = MagicMock()
    embed_and_update(repo, client, items)
    _, kwargs = client.embeddings.create.call_args
    assert kwargs["model"] == EMBEDDING_MODEL


def test_single_batch_patches_have_correct_keys():
    items = _items(2)
    client = _openai_client([[0.1, 0.2], [0.3, 0.4]])
    repo = MagicMock()
    embed_and_update(repo, client, items)
    patches: list[EmbedPatch] = repo.update_post_embeddings.call_args[0][0]
    assert patches[0].key == "0"
    assert patches[1].key == "1"


def test_single_batch_patches_have_correct_embeddings():
    items = _items(2)
    client = _openai_client([[1.0, 2.0], [3.0, 4.0]])
    repo = MagicMock()
    embed_and_update(repo, client, items)
    patches: list[EmbedPatch] = repo.update_post_embeddings.call_args[0][0]
    assert patches[0].embedding == [1.0, 2.0]
    assert patches[1].embedding == [3.0, 4.0]


def test_single_batch_patches_have_correct_model():
    items = _items(1)
    client = _openai_client([[0.5]])
    repo = MagicMock()
    embed_and_update(repo, client, items)
    patches: list[EmbedPatch] = repo.update_post_embeddings.call_args[0][0]
    assert patches[0].embedding_model == EMBEDDING_MODEL


# ── custom model ─────────────────────────────────────────────────────────────


def test_custom_model_is_forwarded_to_api():
    items = _items(1)
    client = _openai_client([[0.1]])
    repo = MagicMock()
    embed_and_update(repo, client, items, model="text-embedding-ada-002")
    _, kwargs = client.embeddings.create.call_args
    assert kwargs["model"] == "text-embedding-ada-002"


def test_custom_model_is_stored_in_patch():
    items = _items(1)
    client = _openai_client([[0.1]])
    repo = MagicMock()
    embed_and_update(repo, client, items, model="my-custom-model")
    patches = repo.update_post_embeddings.call_args[0][0]
    assert patches[0].embedding_model == "my-custom-model"


# ── batching ─────────────────────────────────────────────────────────────────


def test_batching_calls_api_multiple_times():
    """5 items with batch_size=2 → 3 API calls (2+2+1)."""
    items = _items(5)
    # client must handle 3 separate calls with different sizes
    client = MagicMock()
    responses = [
        MagicMock(data=[MagicMock(embedding=[float(i)]) for i in range(2)]),
        MagicMock(data=[MagicMock(embedding=[float(i)]) for i in range(2)]),
        MagicMock(data=[MagicMock(embedding=[float(i)]) for i in range(1)]),
    ]
    client.embeddings.create.side_effect = responses
    repo = MagicMock()
    total = embed_and_update(repo, client, items, batch_size=2)
    assert client.embeddings.create.call_count == 3
    assert total == 5


def test_batching_calls_repo_once_per_batch():
    items = _items(5)
    client = MagicMock()
    responses = [
        MagicMock(data=[MagicMock(embedding=[0.1, 0.2]) for _ in range(2)]),
        MagicMock(data=[MagicMock(embedding=[0.3, 0.4]) for _ in range(2)]),
        MagicMock(data=[MagicMock(embedding=[0.5, 0.6]) for _ in range(1)]),
    ]
    client.embeddings.create.side_effect = responses
    repo = MagicMock()
    embed_and_update(repo, client, items, batch_size=2)
    assert repo.update_post_embeddings.call_count == 3


def test_batching_correct_texts_per_batch():
    """Each API call must receive only the texts for its own batch slice."""
    items = _items(3)
    client = MagicMock()
    responses = [
        MagicMock(data=[MagicMock(embedding=[0.1]) for _ in range(2)]),
        MagicMock(data=[MagicMock(embedding=[0.2]) for _ in range(1)]),
    ]
    client.embeddings.create.side_effect = responses
    repo = MagicMock()
    embed_and_update(repo, client, items, batch_size=2)

    first_call_texts = client.embeddings.create.call_args_list[0][1]["input"]
    second_call_texts = client.embeddings.create.call_args_list[1][1]["input"]
    assert first_call_texts == ["text for post 0", "text for post 1"]
    assert second_call_texts == ["text for post 2"]
