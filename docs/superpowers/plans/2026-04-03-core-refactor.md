# Core Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract all non-Dagster business logic into a pure-Python `core/` domain layer, leaving `defs/` as thin orchestration, with resources exposing `get_instance()` factory methods backed by `PrivateAttr` caching.

**Architecture:** `core/` contains four sub-packages: `crawl` (HTTP + parsing), `graph` (ArangoDB), `llm` (OpenAI), `db` (PostgreSQL). Dagster resources act as factories — `resource.get_instance()` returns a cached domain object (one per Dagster run). Assets call `service.method()` and yield events.

**Tech Stack:** Python, Dagster, dlt, SQLAlchemy, python-arango, openai, requests, pytest, ruff

---

## File Map

**Created:**
- `voz_crawler/core/__init__.py`
- `voz_crawler/core/crawl/__init__.py`
- `voz_crawler/core/crawl/parser.py` ← from `utils/html_parser.py`
- `voz_crawler/core/crawl/pagination.py` ← from `utils/pagination.py`
- `voz_crawler/core/crawl/fetcher.py` ← new class extracted from `sources/voz_thread.py`
- `voz_crawler/core/crawl/source.py` ← from `sources/voz_thread.py`, uses `Fetcher`
- `voz_crawler/core/graph/__init__.py`
- `voz_crawler/core/graph/schema.py` ← from `utils/arango_setup.py`
- `voz_crawler/core/graph/retrieval.py` ← from `utils/graph_retrieval.py`
- `voz_crawler/core/graph/client.py` ← new `ArangoGraphClient`
- `voz_crawler/core/graph/sync.py` ← new `GraphSyncService`
- `voz_crawler/core/llm/__init__.py`
- `voz_crawler/core/llm/embeddings.py` ← new `EmbeddingService`
- `voz_crawler/core/llm/implicit_edges.py` ← new `ImplicitEdgeDetector`
- `voz_crawler/core/db/__init__.py`
- `voz_crawler/core/db/postgres.py` ← new `PostgresClient`
- `voz_crawler/core/db/posts.py` ← new `PostsRepository`
- `voz_crawler/defs/resources/openai.py` ← new `OpenAIResource`
- `tests/core/__init__.py`, `tests/core/crawl/__init__.py`, `tests/core/graph/__init__.py`
- `tests/core/llm/__init__.py`, `tests/core/db/__init__.py`
- `tests/core/crawl/test_fetcher.py`
- `tests/core/graph/test_client.py`
- `tests/core/graph/test_sync.py`
- `tests/core/db/test_postgres.py`
- `tests/core/llm/test_embeddings.py`
- `tests/core/llm/test_implicit_edges.py`

**Modified:**
- `tests/utils/test_html_parser.py` → update import path
- `tests/utils/test_arango_setup.py` → update import path
- `tests/utils/test_graph_retrieval.py` → update import path
- `tests/resources/test_arango_resource.py` → update to test `get_instance()`
- `voz_crawler/defs/resources/arango.py` → add `get_instance()` + `PrivateAttr`
- `voz_crawler/defs/resources/postgres.py` → add `get_instance()` + `PrivateAttr`
- `voz_crawler/defs/resources/crawler.py` → add `get_instance()` + `PrivateAttr`
- `voz_crawler/defs/__init__.py` → add `OpenAIResource` binding
- `voz_crawler/defs/jobs/crawl.py` → use `Fetcher` via `crawler.get_instance()`
- `voz_crawler/defs/assets/ingestion.py` → update import paths
- `voz_crawler/defs/assets/reply_graph.py` → thin assets using core services

**Deleted:**
- `voz_crawler/utils/` (entire directory)
- `voz_crawler/sources/` (entire directory)

---

## Task 1: Core package skeleton

**Files:**
- Create: `voz_crawler/core/__init__.py`
- Create: `voz_crawler/core/crawl/__init__.py`
- Create: `voz_crawler/core/graph/__init__.py`
- Create: `voz_crawler/core/llm/__init__.py`
- Create: `voz_crawler/core/db/__init__.py`

- [ ] **Step 1: Create all `__init__.py` files**

```bash
touch voz_crawler/core/__init__.py \
      voz_crawler/core/crawl/__init__.py \
      voz_crawler/core/graph/__init__.py \
      voz_crawler/core/llm/__init__.py \
      voz_crawler/core/db/__init__.py
```

- [ ] **Step 2: Verify package is importable**

```bash
uv run python -c "import voz_crawler.core.crawl; import voz_crawler.core.graph; import voz_crawler.core.llm; import voz_crawler.core.db; print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add voz_crawler/core/
git commit -m "chore: create core/ package skeleton"
```

---

## Task 2: Move parser.py and pagination.py

**Files:**
- Create: `voz_crawler/core/crawl/parser.py`
- Create: `voz_crawler/core/crawl/pagination.py`
- Modify: `tests/utils/test_html_parser.py`
- Modify: `tests/utils/test_arango_setup.py` (no change yet — arango_setup moves in Task 5)

- [ ] **Step 1: Update test import to new path (will fail)**

In `tests/utils/test_html_parser.py`, replace:
```python
from voz_crawler.utils.html_parser import extract_quote_edges
```
with:
```python
from voz_crawler.core.crawl.parser import extract_quote_edges
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/utils/test_html_parser.py -v
```
Expected: `ModuleNotFoundError: No module named 'voz_crawler.core.crawl.parser'`

- [ ] **Step 3: Create `core/crawl/parser.py`**

Copy `voz_crawler/utils/html_parser.py` content verbatim into `voz_crawler/core/crawl/parser.py`:

```python
from bs4 import BeautifulSoup


def extract_posts(html: str) -> list[dict]:
    """Parse XenForo thread page HTML and extract raw post records."""
    soup = BeautifulSoup(html, "lxml")
    posts = []
    for position, article in enumerate(soup.select("article.message[data-author]"), start=1):
        post_id = _extract_post_id(article)
        author_username = article.get("data-author", "").strip() or None
        author_id = _extract_author_id(article)
        posted_at_raw = _extract_posted_at(article)
        raw_content_html, raw_content_text = _extract_body(article)
        posts.append({
            "post_id_on_site": post_id,
            "post_position": position,
            "author_username": author_username,
            "author_id_on_site": author_id,
            "posted_at_raw": posted_at_raw,
            "raw_content_html": raw_content_html,
            "raw_content_text": raw_content_text,
        })
    return posts


def _extract_post_id(article) -> str | None:
    elem_id = article.get("id", "")
    if elem_id.startswith("js-post-"):
        return elem_id[len("js-post-"):]
    return None


def _extract_author_id(article) -> str | None:
    link = article.select_one("a.username[data-user-id]")
    if link:
        return link.get("data-user-id")
    return None


def _extract_posted_at(article) -> str | None:
    time_tag = article.select_one("time.u-dt")
    if time_tag:
        return time_tag.get("datetime") or time_tag.get_text(strip=True) or None
    return None


def _extract_body(article) -> tuple[str, str]:
    body = article.select_one("div.bbWrapper")
    if not body:
        return "", ""
    return str(body), body.get_text(separator="\n", strip=True)


def extract_quote_edges(html: str) -> list[dict]:
    """Extract explicit quote edges from XenForo HTML."""
    soup = BeautifulSoup(html, "lxml")
    edges = []
    for article in soup.select("article.message[data-author]"):
        from_post_id = _extract_post_id(article)
        if not from_post_id:
            continue
        source_author = article.get("data-author", "").strip() or None
        for ordinal, quote in enumerate(article.select("blockquote[data-source]"), start=1):
            raw_source = quote.get("data-source", "")
            if not raw_source.startswith("post: "):
                continue
            to_post_id = raw_source[len("post: "):].strip()
            target_author = quote.get("data-quote", "").strip() or None
            edges.append({
                "from_post_id": from_post_id,
                "to_post_id": to_post_id,
                "quote_ordinal": ordinal,
                "source_author": source_author,
                "target_author": target_author,
            })
    return edges
```

- [ ] **Step 4: Create `core/crawl/pagination.py`**

```python
from bs4 import BeautifulSoup


def build_page_url(base_thread_url: str, page_number: int) -> str:
    """Build a XenForo thread page URL."""
    base = base_thread_url.rstrip("/")
    if page_number == 1:
        return base + "/"
    return f"{base}/page-{page_number}"


def discover_total_pages(html: str) -> int:
    """Parse XenForo pagination nav to find total page count."""
    soup = BeautifulSoup(html, "lxml")
    nav = soup.select_one(".pageNav")
    if not nav:
        return 1
    page_numbers = [
        int(a.get_text(strip=True))
        for a in nav.select("li.pageNav-page a")
        if a.get_text(strip=True).isdigit()
    ]
    return max(page_numbers) if page_numbers else 1
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/utils/test_html_parser.py -v
```
Expected: all 4 tests PASS

- [ ] **Step 6: Commit**

```bash
git add voz_crawler/core/crawl/parser.py voz_crawler/core/crawl/pagination.py tests/utils/test_html_parser.py
git commit -m "feat(core): move parser and pagination to core/crawl"
```

---

## Task 3: Fetcher class

**Files:**
- Create: `tests/core/crawl/test_fetcher.py`
- Create: `voz_crawler/core/crawl/fetcher.py`

- [ ] **Step 1: Create test directory and failing test**

```bash
mkdir -p tests/core/crawl && touch tests/core/__init__.py tests/core/crawl/__init__.py
```

Create `tests/core/crawl/test_fetcher.py`:

```python
from unittest.mock import MagicMock, patch

from voz_crawler.core.crawl.fetcher import Fetcher


def _make_fetcher():
    return Fetcher(flaresolverr_url="http://localhost:8191", timeout=30)


def test_fetch_returns_status_and_html():
    fetcher = _make_fetcher()
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "status": "ok",
        "solution": {"status": 200, "response": "<html>ok</html>"},
    }
    with patch.object(fetcher._session, "post", return_value=mock_response) as mock_post:
        status, html = fetcher.fetch("http://example.com")
    assert status == 200
    assert html == "<html>ok</html>"
    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert call_kwargs[0][0] == "http://localhost:8191/v1"


def test_fetch_raises_on_flaresolverr_error():
    fetcher = _make_fetcher()
    mock_response = MagicMock()
    mock_response.json.return_value = {"status": "error", "message": "timeout"}
    with patch.object(fetcher._session, "post", return_value=mock_response):
        try:
            fetcher.fetch("http://example.com")
            assert False, "Expected RuntimeError"
        except RuntimeError as e:
            assert "FlareSolverr error" in str(e)


def test_is_cf_block_by_status_code_and_marker():
    fetcher = _make_fetcher()
    assert fetcher.is_cf_block(403, "Just a moment...") is True
    assert fetcher.is_cf_block(503, "cf-browser-verification") is True


def test_is_cf_block_false_for_200():
    fetcher = _make_fetcher()
    assert fetcher.is_cf_block(200, "<html>normal page</html>") is False


def test_is_cf_block_false_for_403_without_marker():
    fetcher = _make_fetcher()
    assert fetcher.is_cf_block(403, "<html>not cloudflare</html>") is False


def test_session_reused_across_calls():
    fetcher = _make_fetcher()
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "status": "ok",
        "solution": {"status": 200, "response": "<html/>"},
    }
    with patch.object(fetcher._session, "post", return_value=mock_response):
        fetcher.fetch("http://a.com")
        fetcher.fetch("http://b.com")
    # Both calls go through the same session object
    assert fetcher._session.post.call_count == 2
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/core/crawl/test_fetcher.py -v
```
Expected: `ModuleNotFoundError: No module named 'voz_crawler.core.crawl.fetcher'`

- [ ] **Step 3: Implement `core/crawl/fetcher.py`**

```python
import requests

_CF_BLOCK_MARKERS = ("Just a moment", "cf-browser-verification")
_CF_BLOCK_STATUSES = (403, 429, 503)


class Fetcher:
    """HTTP client for FlareSolverr. Reuses a requests.Session for keep-alive."""

    def __init__(self, flaresolverr_url: str, timeout: int = 60) -> None:
        self._flaresolverr_url = flaresolverr_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()

    def fetch(self, url: str) -> tuple[int, str]:
        """Fetch a URL through FlareSolverr. Returns (status_code, html)."""
        payload = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": self._timeout * 1000,
        }
        r = self._session.post(
            f"{self._flaresolverr_url}/v1",
            json=payload,
            timeout=self._timeout + 10,
        )
        r.raise_for_status()
        data = r.json()
        if data["status"] != "ok":
            raise RuntimeError(f"FlareSolverr error: {data.get('message', data)}")
        solution = data["solution"]
        return solution["status"], solution["response"]

    def is_cf_block(self, status_code: int, html: str) -> bool:
        if status_code in _CF_BLOCK_STATUSES:
            snippet = html[:2000]
            return any(m in snippet for m in _CF_BLOCK_MARKERS)
        return False
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/core/crawl/test_fetcher.py -v
```
Expected: all 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add voz_crawler/core/crawl/fetcher.py tests/core/ 
git commit -m "feat(core): add Fetcher class with requests.Session reuse"
```

---

## Task 4: Move dlt source to core/crawl/source.py

**Files:**
- Create: `voz_crawler/core/crawl/source.py`

The source is moved from `sources/voz_thread.py`. Uses `Fetcher` internally instead of bare `requests.post`. No separate test file — dlt source internals are already covered by `test_fetcher.py` and `test_html_parser.py`.

- [ ] **Step 1: Create `core/crawl/source.py`**

```python
"""dlt source for a single Voz.vn thread page.

Data flow:
  voz_page_source(page_url)
      └─► voz_page_posts (resource)
              yields one dict per forum post (global merge by post_id_on_site)

No cursor or incremental logic — idempotency is handled by:
  write_disposition="merge" + primary_key="post_id_on_site"
"""

import dlt

from voz_crawler.core.crawl.fetcher import Fetcher
from voz_crawler.core.crawl.parser import extract_posts


@dlt.resource(
    name="posts",
    write_disposition="merge",
    primary_key="post_id_on_site",
)
def voz_page_posts(
    page_url: str,
    flaresolverr_url: str,
    http_timeout_seconds: int = 60,
) -> dlt.sources.TDataItems:
    """Fetch a single Voz thread page via FlareSolverr and yield all post records."""
    fetcher = Fetcher(flaresolverr_url=flaresolverr_url, timeout=http_timeout_seconds)
    status_code, html = fetcher.fetch(page_url)
    if fetcher.is_cf_block(status_code, html):
        raise RuntimeError(
            f"Cloudflare blocked {page_url} (HTTP {status_code}). Try again later."
        )
    for post in extract_posts(html):
        if not post["post_id_on_site"]:
            continue
        try:
            post_id_int = int(post["post_id_on_site"])
        except (ValueError, TypeError):
            continue
        yield {
            "post_id_on_site": post_id_int,
            "page_url": page_url,
            "author_username": post["author_username"],
            "author_id_on_site": post["author_id_on_site"],
            "posted_at_raw": post["posted_at_raw"],
            "raw_content_html": post["raw_content_html"],
            "raw_content_text": post["raw_content_text"],
        }


@dlt.source(name="voz")
def voz_page_source(
    page_url: str,
    flaresolverr_url: str,
    http_timeout_seconds: int = 60,
) -> dlt.sources.TDataItems:
    """dlt source for a single Voz.vn thread page."""
    return voz_page_posts(
        page_url=page_url,
        flaresolverr_url=flaresolverr_url,
        http_timeout_seconds=http_timeout_seconds,
    )
```

- [ ] **Step 2: Verify import works**

```bash
uv run python -c "from voz_crawler.core.crawl.source import voz_page_source; print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add voz_crawler/core/crawl/source.py
git commit -m "feat(core): move dlt source to core/crawl/source.py"
```

---

## Task 5: Move arango_setup.py and graph_retrieval.py

**Files:**
- Create: `voz_crawler/core/graph/schema.py`
- Create: `voz_crawler/core/graph/retrieval.py`
- Modify: `tests/utils/test_arango_setup.py`
- Modify: `tests/utils/test_graph_retrieval.py`

- [ ] **Step 1: Update test imports (will fail)**

In `tests/utils/test_arango_setup.py`, replace:
```python
from voz_crawler.utils.arango_setup import ensure_schema
```
with:
```python
from voz_crawler.core.graph.schema import ensure_schema
```

In `tests/utils/test_graph_retrieval.py`, replace:
```python
from voz_crawler.utils.graph_retrieval import get_thread_context
```
with:
```python
from voz_crawler.core.graph.retrieval import get_thread_context
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/utils/test_arango_setup.py tests/utils/test_graph_retrieval.py -v
```
Expected: `ModuleNotFoundError` for both

- [ ] **Step 3: Create `core/graph/schema.py`**

```python
_VECTOR_N_LISTS = 100


def _ensure_vector_index(db) -> None:
    """Create the vector index on Posts.embedding if enough documents exist."""
    posts_col = db.collection("Posts")
    existing_types = {idx["type"] for idx in posts_col.indexes()}
    if "vector" in existing_types:
        return
    count = posts_col.count()
    if count < _VECTOR_N_LISTS:
        return
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
    """Idempotently create ArangoDB collections, named graph, and vector index."""
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
```

- [ ] **Step 4: Create `core/graph/retrieval.py`**

```python
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
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/utils/test_arango_setup.py tests/utils/test_graph_retrieval.py -v
```
Expected: all 9 tests PASS

- [ ] **Step 6: Commit**

```bash
git add voz_crawler/core/graph/schema.py voz_crawler/core/graph/retrieval.py \
        tests/utils/test_arango_setup.py tests/utils/test_graph_retrieval.py
git commit -m "feat(core): move schema and retrieval to core/graph"
```

---

## Task 6: ArangoGraphClient

**Files:**
- Create: `tests/core/graph/__init__.py` + `tests/core/graph/test_client.py`
- Create: `voz_crawler/core/graph/client.py`

- [ ] **Step 1: Create failing test**

```bash
mkdir -p tests/core/graph && touch tests/core/graph/__init__.py
```

Create `tests/core/graph/test_client.py`:

```python
from unittest.mock import MagicMock, patch, call

from voz_crawler.core.graph.client import ArangoGraphClient


def _make_client(mock_arango_client):
    """Patch ArangoClient and return an ArangoGraphClient instance."""
    mock_db = MagicMock()
    mock_arango_client.return_value.db.return_value = mock_db
    client = ArangoGraphClient(
        host="localhost", port=8529,
        username="root", password="pw",
        db_name="test_db",
    )
    return client, mock_db


def test_db_returns_authenticated_database():
    with patch("voz_crawler.core.graph.client.ArangoClient") as mock_arango:
        client, mock_db = _make_client(mock_arango)
        result = client.db()
    assert result is mock_db


def test_db_calls_ensure_schema_on_first_access():
    with patch("voz_crawler.core.graph.client.ArangoClient") as mock_arango, \
         patch("voz_crawler.core.graph.client.ensure_schema") as mock_schema:
        client, _ = _make_client(mock_arango)
        client.db()
    mock_schema.assert_called_once()


def test_ensure_schema_called_only_once():
    with patch("voz_crawler.core.graph.client.ArangoClient") as mock_arango, \
         patch("voz_crawler.core.graph.client.ensure_schema") as mock_schema:
        client, _ = _make_client(mock_arango)
        client.db()
        client.db()
        client.db()
    mock_schema.assert_called_once()


def test_db_returns_same_instance_on_repeated_calls():
    with patch("voz_crawler.core.graph.client.ArangoClient") as mock_arango:
        client, mock_db = _make_client(mock_arango)
        first = client.db()
        second = client.db()
    assert first is second
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/core/graph/test_client.py -v
```
Expected: `ModuleNotFoundError: No module named 'voz_crawler.core.graph.client'`

- [ ] **Step 3: Implement `core/graph/client.py`**

```python
from arango import ArangoClient

from voz_crawler.core.graph.schema import ensure_schema


class ArangoGraphClient:
    """Authenticated ArangoDB connection with lazy schema initialization.

    Calls ensure_schema() exactly once on first db() access. Subsequent
    calls return the same Database object — no new TCP handshakes.
    """

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        db_name: str,
    ) -> None:
        client = ArangoClient(hosts=f"http://{host}:{port}")
        self._db = client.db(db_name, username=username, password=password)
        self._schema_initialized = False

    def db(self):
        """Return the authenticated Database, initializing schema on first call."""
        if not self._schema_initialized:
            ensure_schema(self._db)
            self._schema_initialized = True
        return self._db
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/core/graph/test_client.py -v
```
Expected: all 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add voz_crawler/core/graph/client.py tests/core/graph/
git commit -m "feat(core): add ArangoGraphClient with lazy schema init"
```

---

## Task 7: PostgresClient and PostsRepository

**Files:**
- Create: `tests/core/db/__init__.py` + `tests/core/db/test_postgres.py`
- Create: `voz_crawler/core/db/postgres.py`
- Create: `voz_crawler/core/db/posts.py`

- [ ] **Step 1: Create failing tests**

```bash
mkdir -p tests/core/db && touch tests/core/db/__init__.py
```

Create `tests/core/db/test_postgres.py`:

```python
from unittest.mock import MagicMock, patch

from voz_crawler.core.db.postgres import PostgresClient
from voz_crawler.core.db.posts import PostsRepository


# ── PostgresClient ─────────────────────────────────────────────────────────────

def test_engine_created_once():
    with patch("voz_crawler.core.db.postgres.create_engine") as mock_create:
        mock_create.return_value = MagicMock()
        client = PostgresClient(url="postgresql://user:pw@host/db")
        _ = client.engine
        _ = client.engine
    mock_create.assert_called_once_with(
        "postgresql://user:pw@host/db",
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )


def test_dispose_delegates_to_engine():
    with patch("voz_crawler.core.db.postgres.create_engine") as mock_create:
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine
        client = PostgresClient(url="postgresql://user:pw@host/db")
        client.dispose()
    mock_engine.dispose.assert_called_once()


# ── PostsRepository ────────────────────────────────────────────────────────────

def _make_repo(rows):
    """Return a PostsRepository backed by a mock engine that yields rows."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.fetchall.return_value = rows
    return PostsRepository(engine=mock_engine), mock_conn


def test_fetch_new_posts_queries_correct_table():
    repo, mock_conn = _make_repo([])
    repo.fetch_new_posts(after_post_id=0)
    sql_text = str(mock_conn.execute.call_args[0][0])
    assert "raw.voz__posts" in sql_text
    assert "post_id_on_site" in sql_text


def test_fetch_new_posts_passes_last_id():
    repo, mock_conn = _make_repo([])
    repo.fetch_new_posts(after_post_id=42)
    params = mock_conn.execute.call_args[0][1]
    assert params["last_id"] == 42


def test_fetch_html_for_posts_queries_correct_columns():
    repo, mock_conn = _make_repo([])
    repo.fetch_html_for_posts(after_post_id=0)
    sql_text = str(mock_conn.execute.call_args[0][0])
    assert "raw_content_html" in sql_text


def test_fetch_html_for_posts_passes_last_id():
    repo, mock_conn = _make_repo([])
    repo.fetch_html_for_posts(after_post_id=99)
    params = mock_conn.execute.call_args[0][1]
    assert params["last_id"] == 99
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/core/db/test_postgres.py -v
```
Expected: `ModuleNotFoundError`

- [ ] **Step 3: Implement `core/db/postgres.py`**

```python
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class PostgresClient:
    """SQLAlchemy engine wrapper with production-ready pool settings."""

    def __init__(self, url: str) -> None:
        self._engine = create_engine(
            url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )

    @property
    def engine(self) -> Engine:
        return self._engine

    def dispose(self) -> None:
        self._engine.dispose()
```

- [ ] **Step 4: Implement `core/db/posts.py`**

```python
from sqlalchemy import text
from sqlalchemy.engine import Engine


class PostsRepository:
    """SQL queries over the raw.voz__posts table."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def fetch_new_posts(self, after_post_id: int) -> list:
        with self._engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT post_id_on_site, author_username, author_id_on_site, "
                    "posted_at_raw, raw_content_html, raw_content_text "
                    "FROM raw.voz__posts "
                    "WHERE post_id_on_site > :last_id "
                    "ORDER BY post_id_on_site"
                ),
                {"last_id": after_post_id},
            ).fetchall()

    def fetch_html_for_posts(self, after_post_id: int) -> list:
        with self._engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT post_id_on_site, raw_content_html "
                    "FROM raw.voz__posts "
                    "WHERE post_id_on_site > :last_id "
                    "ORDER BY post_id_on_site"
                ),
                {"last_id": after_post_id},
            ).fetchall()
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/core/db/test_postgres.py -v
```
Expected: all 6 tests PASS

- [ ] **Step 6: Commit**

```bash
git add voz_crawler/core/db/postgres.py voz_crawler/core/db/posts.py tests/core/db/
git commit -m "feat(core): add PostgresClient and PostsRepository"
```

---

## Task 8: GraphSyncService

**Files:**
- Create: `tests/core/graph/test_sync.py`
- Create: `voz_crawler/core/graph/sync.py`

- [ ] **Step 1: Create failing test**

Create `tests/core/graph/test_sync.py`:

```python
from types import SimpleNamespace
from unittest.mock import MagicMock, call

from voz_crawler.core.graph.sync import GraphSyncService


def _make_row(**kwargs):
    return SimpleNamespace(**kwargs)


def _make_service(aql_results=None, post_rows=None, html_rows=None):
    """Helper: build a GraphSyncService with fully mocked dependencies."""
    arango_db = MagicMock()
    posts_repo = MagicMock()

    # Default AQL cursor returns 0 (no existing data)
    aql_cursor = MagicMock()
    aql_cursor.__iter__ = MagicMock(return_value=iter(aql_results or [0]))
    arango_db.aql.execute.return_value = aql_cursor

    posts_repo.fetch_new_posts.return_value = post_rows or []
    posts_repo.fetch_html_for_posts.return_value = html_rows or []

    return GraphSyncService(arango_db=arango_db, posts_repo=posts_repo), arango_db, posts_repo


def test_sync_posts_returns_zero_stats_when_no_new_posts():
    svc, _, _ = _make_service(post_rows=[])
    stats = svc.sync_posts()
    assert stats["records_processed"] == 0


def test_sync_posts_inserts_each_row():
    rows = [
        _make_row(
            post_id_on_site=101, author_username="alice", author_id_on_site="1",
            posted_at_raw="2024-01-01T10:00:00", raw_content_text="hello",
        ),
        _make_row(
            post_id_on_site=102, author_username="bob", author_id_on_site="2",
            posted_at_raw="2024-01-01T11:00:00", raw_content_text="world",
        ),
    ]
    svc, arango_db, _ = _make_service(post_rows=rows)
    stats = svc.sync_posts()
    assert stats["records_processed"] == 2
    assert stats["last_processed_post_id"] == 102
    posts_col = arango_db.collection("Posts")
    assert posts_col.insert.call_count == 2


def test_sync_posts_uses_max_id_as_cursor():
    svc, arango_db, posts_repo = _make_service(aql_results=[55], post_rows=[])
    svc.sync_posts()
    posts_repo.fetch_new_posts.assert_called_once_with(after_post_id=55)


def test_extract_explicit_edges_upserts_quotes():
    html = (
        '<article class="message" data-author="alice" id="js-post-200">'
        '<div class="message-body"><div class="bbWrapper">'
        '<blockquote data-source="post: 100" data-quote="bob">q</blockquote>'
        '</div></div></article>'
    )
    rows = [_make_row(post_id_on_site=200, raw_content_html=html)]
    svc, arango_db, _ = _make_service(html_rows=rows)
    stats = svc.extract_explicit_edges()
    assert stats["edges_processed"] == 1
    quotes_col = arango_db.collection("quotes")
    quotes_col.insert.assert_called_once()
    doc = quotes_col.insert.call_args[0][0]
    assert doc["_from"] == "Posts/200"
    assert doc["_to"] == "Posts/100"
    assert doc["method"] == "html_metadata"
    assert doc["confidence"] == 1.0


def test_extract_explicit_edges_returns_zero_for_no_quotes():
    rows = [_make_row(post_id_on_site=300, raw_content_html="<p>no quotes</p>")]
    svc, _, _ = _make_service(html_rows=rows)
    stats = svc.extract_explicit_edges()
    assert stats["edges_processed"] == 0
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/core/graph/test_sync.py -v
```
Expected: `ModuleNotFoundError: No module named 'voz_crawler.core.graph.sync'`

- [ ] **Step 3: Implement `core/graph/sync.py`**

```python
from voz_crawler.core.crawl.parser import extract_quote_edges
from voz_crawler.core.db.posts import PostsRepository


class GraphSyncService:
    """Syncs PostgreSQL posts into ArangoDB and extracts explicit quote edges."""

    def __init__(self, arango_db, posts_repo: PostsRepository) -> None:
        self._db = arango_db
        self._repo = posts_repo

    def sync_posts(self) -> dict:
        """Incremental sync: copy new posts from PostgreSQL into ArangoDB Posts."""
        cursor = self._db.aql.execute(
            "RETURN MAX(FOR p IN Posts RETURN p.post_id)"
        )
        last_id = int(next(iter(cursor)) or 0)

        rows = self._repo.fetch_new_posts(after_post_id=last_id)
        if not rows:
            return {"records_processed": 0}

        posts_col = self._db.collection("Posts")
        for row in rows:
            posts_col.insert(
                {
                    "_key": str(row.post_id_on_site),
                    "post_id": row.post_id_on_site,
                    "author_username": row.author_username,
                    "author_id": row.author_id_on_site,
                    "posted_at": row.posted_at_raw,
                    "content_text": row.raw_content_text,
                    "embedding": None,
                    "embedding_model": None,
                },
                overwrite=True,
            )

        return {
            "records_processed": len(rows),
            "last_processed_post_id": rows[-1].post_id_on_site,
        }

    def extract_explicit_edges(self) -> dict:
        """Parse HTML quotes and upsert explicit edges into the quotes collection."""
        cursor = self._db.aql.execute(
            "RETURN MAX(FOR e IN quotes RETURN TO_NUMBER(PARSE_IDENTIFIER(e._from).key))"
        )
        last_from_id = int(next(iter(cursor)) or 0)

        rows = self._repo.fetch_html_for_posts(after_post_id=last_from_id)
        quotes_col = self._db.collection("quotes")
        count = 0
        for row in rows:
            edges = extract_quote_edges(row.raw_content_html or "")
            for edge in edges:
                quotes_col.insert(
                    {
                        "_from": f"Posts/{edge['from_post_id']}",
                        "_to": f"Posts/{edge['to_post_id']}",
                        "source_author": edge["source_author"],
                        "target_author": edge["target_author"],
                        "quote_ordinal": edge["quote_ordinal"],
                        "confidence": 1.0,
                        "method": "html_metadata",
                    },
                    overwrite=True,
                )
                count += 1

        return {"edges_processed": count}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/core/graph/test_sync.py -v
```
Expected: all 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add voz_crawler/core/graph/sync.py tests/core/graph/test_sync.py
git commit -m "feat(core): add GraphSyncService"
```

---

## Task 9: EmbeddingService

**Files:**
- Create: `tests/core/llm/__init__.py` + `tests/core/llm/test_embeddings.py`
- Create: `voz_crawler/core/llm/embeddings.py`

- [ ] **Step 1: Create failing test**

```bash
mkdir -p tests/core/llm && touch tests/core/llm/__init__.py
```

Create `tests/core/llm/test_embeddings.py`:

```python
from unittest.mock import MagicMock, call

from voz_crawler.core.llm.embeddings import EmbeddingService


def _make_service(pending_posts=None):
    """Build EmbeddingService with mocked OpenAI client and ArangoDB."""
    client = MagicMock()
    arango_db = MagicMock()

    cursor = MagicMock()
    cursor.__iter__ = MagicMock(return_value=iter(pending_posts or []))
    arango_db.aql.execute.return_value = cursor

    return EmbeddingService(client=client), client, arango_db


def test_returns_zero_when_no_pending_posts():
    svc, _, arango_db = _make_service(pending_posts=[])
    stats = svc.compute_missing(arango_db)
    assert stats["records_processed"] == 0


def test_does_not_call_openai_when_no_pending():
    svc, mock_client, arango_db = _make_service(pending_posts=[])
    svc.compute_missing(arango_db)
    mock_client.embeddings.create.assert_not_called()


def test_batches_calls_correctly():
    """25 posts with batch_size=20 should produce 2 OpenAI API calls."""
    posts = [{"_key": str(i), "text": f"post {i}"} for i in range(25)]
    svc, mock_client, arango_db = _make_service(pending_posts=posts)
    svc = EmbeddingService(client=mock_client, batch_size=20)

    mock_emb = MagicMock()
    mock_emb.embedding = [0.1] * 1536
    mock_client.embeddings.create.return_value.data = [mock_emb] * 20

    svc.compute_missing(arango_db)
    assert mock_client.embeddings.create.call_count == 2


def test_updates_posts_with_embedding():
    posts = [{"_key": "42", "text": "some text"}]
    svc, mock_client, arango_db = _make_service(pending_posts=posts)
    svc = EmbeddingService(client=mock_client, batch_size=20)

    mock_emb = MagicMock()
    mock_emb.embedding = [0.5] * 1536
    mock_client.embeddings.create.return_value.data = [mock_emb]

    svc.compute_missing(arango_db)

    posts_col = arango_db.collection("Posts")
    posts_col.update.assert_called_once()
    update_doc = posts_col.update.call_args[0][0]
    assert update_doc["_key"] == "42"
    assert update_doc["embedding"] == [0.5] * 1536
    assert update_doc["embedding_model"] == "text-embedding-3-small"


def test_returns_correct_count():
    posts = [{"_key": str(i), "text": f"t{i}"} for i in range(5)]
    svc, mock_client, arango_db = _make_service(pending_posts=posts)
    svc = EmbeddingService(client=mock_client, batch_size=20)
    mock_emb = MagicMock()
    mock_emb.embedding = [0.1] * 1536
    mock_client.embeddings.create.return_value.data = [mock_emb] * 5
    stats = svc.compute_missing(arango_db)
    assert stats["records_processed"] == 5
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/core/llm/test_embeddings.py -v
```
Expected: `ModuleNotFoundError: No module named 'voz_crawler.core.llm.embeddings'`

- [ ] **Step 3: Implement `core/llm/embeddings.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/core/llm/test_embeddings.py -v
```
Expected: all 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add voz_crawler/core/llm/embeddings.py tests/core/llm/
git commit -m "feat(core): add EmbeddingService"
```

---

## Task 10: ImplicitEdgeDetector

**Files:**
- Create: `tests/core/llm/test_implicit_edges.py`
- Create: `voz_crawler/core/llm/implicit_edges.py`

- [ ] **Step 1: Create failing test**

Create `tests/core/llm/test_implicit_edges.py`:

```python
import json
from unittest.mock import MagicMock

from voz_crawler.core.llm.implicit_edges import ImplicitEdgeDetector


def _make_detector(embedded_posts=None, existing_quotes=None, ann_results=None, llm_pairs=None):
    client = MagicMock()
    arango_db = MagicMock()

    def _aql_side_effect(aql, **kwargs):
        aql_stripped = aql.strip()
        cursor = MagicMock()
        if "APPROX_NEAR_COSINE" in aql_stripped:
            cursor.__iter__ = MagicMock(return_value=iter(ann_results or []))
        elif "implicit_replies" not in aql_stripped and "FOR e IN quotes" in aql_stripped:
            cursor.__iter__ = MagicMock(return_value=iter(existing_quotes or []))
        elif "Posts" in aql_stripped and "embedding != null" in aql_stripped:
            cursor.__iter__ = MagicMock(return_value=iter(embedded_posts or []))
        else:
            cursor.__iter__ = MagicMock(return_value=iter([]))
        return cursor

    arango_db.aql.execute.side_effect = _aql_side_effect

    if llm_pairs is not None:
        mock_resp = MagicMock()
        mock_resp.choices[0].message.content = json.dumps(llm_pairs)
        client.chat.completions.create.return_value = mock_resp

    return ImplicitEdgeDetector(client=client, arango_db=arango_db), arango_db


def test_returns_zero_when_no_embedded_posts():
    detector, _ = _make_detector(embedded_posts=[])
    stats = detector.detect()
    assert stats["edges_created"] == 0


def test_skips_llm_when_no_ann_candidates():
    posts = [{"_key": "1", "post_id": 1, "author_username": "alice",
              "posted_at": "2024-01-01T10:00:00", "content_text": "hello world",
              "embedding": [0.1] * 1536}]
    detector, _ = _make_detector(embedded_posts=posts, ann_results=[])
    stats = detector.detect()
    assert stats["edges_created"] == 0
    # LLM should not be called when no ANN candidates found
    assert detector._client.chat.completions.create.call_count == 0


def test_filters_pairs_below_confidence_threshold():
    posts = [{"_key": "1", "post_id": 1, "author_username": "a",
              "posted_at": "2024-01-01T10:00:00", "content_text": "x" * 25,
              "embedding": [0.1] * 1536}]
    low_conf_pairs = [{"from": 2, "to": 1, "confidence": 0.5, "reason": "weak"}]
    detector, arango_db = _make_detector(
        embedded_posts=posts, ann_results=[{"_key": "2"}], llm_pairs=low_conf_pairs
    )
    stats = detector.detect()
    assert stats["edges_created"] == 0
    arango_db.collection("implicit_replies").insert.assert_not_called()


def test_inserts_high_confidence_pair():
    posts = [{"_key": "1", "post_id": 1, "author_username": "a",
              "posted_at": "2024-01-01T10:00:00", "content_text": "x" * 25,
              "embedding": [0.1] * 1536}]
    high_conf_pairs = [{"from": 2, "to": 1, "confidence": 0.9, "reason": "direct reply"}]
    detector, arango_db = _make_detector(
        embedded_posts=posts, ann_results=[{"_key": "2"}], llm_pairs=high_conf_pairs
    )
    stats = detector.detect()
    assert stats["edges_created"] == 1
    col = arango_db.collection("implicit_replies")
    col.insert.assert_called_once()
    doc = col.insert.call_args[0][0]
    assert doc["_from"] == "Posts/2"
    assert doc["_to"] == "Posts/1"
    assert doc["method"] == "semantic"
    assert doc["llm_confidence"] == 0.9


def test_deduplicates_against_existing_quotes():
    posts = [{"_key": "1", "post_id": 1, "author_username": "a",
              "posted_at": "2024-01-01T10:00:00", "content_text": "x" * 25,
              "embedding": [0.1] * 1536}]
    # This pair already exists in quotes
    existing_quotes = [{"from": "Posts/2", "to": "Posts/1"}]
    pairs = [{"from": 2, "to": 1, "confidence": 0.95, "reason": "already quoted"}]
    detector, arango_db = _make_detector(
        embedded_posts=posts, existing_quotes=existing_quotes,
        ann_results=[{"_key": "2"}], llm_pairs=pairs,
    )
    stats = detector.detect()
    assert stats["edges_created"] == 0
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/core/llm/test_implicit_edges.py -v
```
Expected: `ModuleNotFoundError: No module named 'voz_crawler.core.llm.implicit_edges'`

- [ ] **Step 3: Implement `core/llm/implicit_edges.py`**

```python
import json

import openai

_IMPLICIT_AQL = """
FOR doc IN Posts
  FILTER doc.posted_at >= DATE_SUBTRACT(@post_time, 10, "hour")
  FILTER doc.posted_at < @post_time
  FILTER doc.author_username != @author
  FILTER doc.embedding != null
  SORT APPROX_NEAR_COSINE(doc.embedding, @query_embedding) DESC
  LIMIT 10
  LET sim = COSINE_SIMILARITY(doc.embedding, @query_embedding)
  FILTER sim >= 0.65
  LET delta_h = DATE_DIFF(doc.posted_at, @post_time, "h")
  RETURN MERGE(doc, {
    similarity: sim,
    temporal_score: EXP(-delta_h / 1.36),
    combined_score: 0.6 * sim + 0.4 * EXP(-delta_h / 1.36)
  })
"""

_LLM_PROMPT = """\
Below are {n} consecutive posts from a Vietnamese IT company review thread.

{posts}

Identify pairs (A, B) where post B is an implicit reply to post A (the user did not \
press the quote button but is directly responding to the content of post A).

Only return pairs with confidence >= 0.7. Do not create an edge if an HTML quote already exists.

Output JSON only:
[{{"from": <post_id>, "to": <post_id>, "confidence": 0.85, "reason": "..."}}]
"""

_PAGE_SIZE = 20
_MIN_TEXT_LEN = 20
_MIN_CONFIDENCE = 0.75


class ImplicitEdgeDetector:
    """Detects implicit reply relationships using ANN + LLM (3-stage pipeline)."""

    def __init__(self, client: openai.OpenAI, arango_db) -> None:
        self._client = client
        self._db = arango_db

    def detect(self) -> dict:
        """Run ANN → dedup → LLM pipeline. Returns {"edges_created": N}."""
        all_cursor = self._db.aql.execute(
            "FOR p IN Posts FILTER p.embedding != null "
            "SORT p.posted_at ASC "
            "RETURN {_key: p._key, post_id: p.post_id, author_username: p.author_username, "
            "posted_at: p.posted_at, content_text: p.content_text, embedding: p.embedding}"
        )
        posts = list(all_cursor)
        if not posts:
            return {"edges_created": 0}

        existing_quotes: set[tuple[str, str]] = set()
        qcursor = self._db.aql.execute(
            "FOR e IN quotes RETURN {from: e._from, to: e._to}"
        )
        for edge in qcursor:
            existing_quotes.add((edge["from"], edge["to"]))

        implicit_col = self._db.collection("implicit_replies")
        total_edges = 0

        for page_start in range(0, len(posts), _PAGE_SIZE):
            page = posts[page_start : page_start + _PAGE_SIZE]
            page = [p for p in page if len(p.get("content_text") or "") >= _MIN_TEXT_LEN]
            if not page:
                continue

            # Stage 1: ANN — check if any candidates exist for this page
            candidates_exist = False
            for post in page:
                cands = list(self._db.aql.execute(
                    _IMPLICIT_AQL,
                    bind_vars={
                        "post_time": post["posted_at"],
                        "author": post["author_username"],
                        "query_embedding": post["embedding"],
                    },
                ))
                if cands:
                    candidates_exist = True
                    break

            if not candidates_exist:
                continue

            # Stage 3: LLM
            posts_text = "\n\n".join(
                f"[{p['post_id']}] {p['author_username']}: "
                f"{(p['content_text'] or '')[:300]}"
                for p in page
            )
            prompt = _LLM_PROMPT.format(n=len(page), posts=posts_text)

            pairs: list[dict] = []
            for attempt in range(2):
                try:
                    resp = self._client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[{"role": "user", "content": prompt}],
                        temperature=0,
                    )
                    pairs = json.loads(resp.choices[0].message.content.strip())
                    break
                except Exception:
                    if attempt == 1:
                        pairs = []

            for pair in pairs:
                if pair.get("confidence", 0) < _MIN_CONFIDENCE:
                    continue
                from_key = f"Posts/{pair['from']}"
                to_key = f"Posts/{pair['to']}"
                if (from_key, to_key) in existing_quotes:
                    continue
                implicit_col.insert(
                    {
                        "_from": from_key,
                        "_to": to_key,
                        "llm_confidence": pair["confidence"],
                        "llm_reasoning": pair.get("reason", ""),
                        "method": "semantic",
                    },
                    overwrite=True,
                )
                total_edges += 1

        return {"edges_created": total_edges}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/core/llm/test_implicit_edges.py -v
```
Expected: all 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add voz_crawler/core/llm/implicit_edges.py tests/core/llm/test_implicit_edges.py
git commit -m "feat(core): add ImplicitEdgeDetector"
```

---

## Task 11: Update Dagster resources with get_instance() + add OpenAIResource

**Files:**
- Modify: `voz_crawler/defs/resources/arango.py`
- Modify: `voz_crawler/defs/resources/postgres.py`
- Modify: `voz_crawler/defs/resources/crawler.py`
- Create: `voz_crawler/defs/resources/openai.py`
- Modify: `tests/resources/test_arango_resource.py`

- [ ] **Step 1: Update `tests/resources/test_arango_resource.py`**

Replace the entire file:

```python
from unittest.mock import patch

from voz_crawler.defs.resources.arango import ArangoResource
from voz_crawler.core.graph.client import ArangoGraphClient


def test_arango_resource_fields():
    resource = ArangoResource(
        host="localhost",
        port=8529,
        username="admin",
        password="secret",
        db="test_db",
    )
    assert resource.host == "localhost"
    assert resource.port == 8529
    assert resource.username == "admin"
    assert resource.db == "test_db"


def test_arango_resource_defaults():
    resource = ArangoResource(host="arango", password="pw", db="mydb")
    assert resource.port == 8529
    assert resource.username == "root"


def test_get_instance_returns_arango_graph_client():
    with patch("voz_crawler.core.graph.client.ArangoClient"), \
         patch("voz_crawler.core.graph.client.ensure_schema"):
        resource = ArangoResource(host="localhost", password="pw", db="mydb")
        instance = resource.get_instance()
    assert isinstance(instance, ArangoGraphClient)


def test_get_instance_returns_same_cached_object():
    with patch("voz_crawler.core.graph.client.ArangoClient"), \
         patch("voz_crawler.core.graph.client.ensure_schema"):
        resource = ArangoResource(host="localhost", password="pw", db="mydb")
        first = resource.get_instance()
        second = resource.get_instance()
    assert first is second
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/resources/test_arango_resource.py -v
```
Expected: last 2 tests FAIL (no `get_instance` yet)

- [ ] **Step 3: Rewrite `defs/resources/arango.py`**

```python
from typing import Optional

from dagster import ConfigurableResource
from pydantic import PrivateAttr

from voz_crawler.core.graph.client import ArangoGraphClient


class ArangoResource(ConfigurableResource):
    """ArangoDB connection parameters. Use get_instance() to obtain a pooled client."""

    host: str
    port: int = 8529
    username: str = "root"
    password: str
    db: str = "voz_graph"

    _instance: Optional[ArangoGraphClient] = PrivateAttr(default=None)

    def get_instance(self) -> ArangoGraphClient:
        if self._instance is None:
            self._instance = ArangoGraphClient(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                db_name=self.db,
            )
        return self._instance
```

- [ ] **Step 4: Rewrite `defs/resources/postgres.py`**

```python
from typing import Optional

from dagster import ConfigurableResource
from pydantic import PrivateAttr

from voz_crawler.core.db.postgres import PostgresClient


class PostgresResource(ConfigurableResource):
    """PostgreSQL connection parameters. Use get_instance() for a pooled engine."""

    user: str
    password: str
    host: str
    port: int = 5432
    db: str

    _instance: Optional[PostgresClient] = PrivateAttr(default=None)

    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

    def get_instance(self) -> PostgresClient:
        if self._instance is None:
            self._instance = PostgresClient(url=self.url)
        return self._instance
```

- [ ] **Step 5: Rewrite `defs/resources/crawler.py`**

```python
from typing import Optional

from dagster import ConfigurableResource
from pydantic import PrivateAttr

from voz_crawler.core.crawl.fetcher import Fetcher


class CrawlerResource(ConfigurableResource):
    """Crawler configuration. Use get_instance() for a Fetcher with HTTP keep-alive."""

    thread_url: str
    flaresolverr_url: str
    http_delay_seconds: str = "2"
    http_timeout_seconds: int = 60

    _instance: Optional[Fetcher] = PrivateAttr(default=None)

    @property
    def delay(self) -> float:
        return float(self.http_delay_seconds)

    def get_instance(self) -> Fetcher:
        if self._instance is None:
            self._instance = Fetcher(
                flaresolverr_url=self.flaresolverr_url,
                timeout=self.http_timeout_seconds,
            )
        return self._instance
```

- [ ] **Step 6: Create `defs/resources/openai.py`**

```python
from typing import Optional

import openai
from dagster import ConfigurableResource
from pydantic import PrivateAttr


class OpenAIResource(ConfigurableResource):
    """OpenAI API configuration. Use get_instance() for a cached client."""

    api_key: str

    _instance: Optional[openai.OpenAI] = PrivateAttr(default=None)

    def get_instance(self) -> openai.OpenAI:
        if self._instance is None:
            self._instance = openai.OpenAI(api_key=self.api_key)
        return self._instance
```

- [ ] **Step 7: Run all resource tests to verify they pass**

```bash
uv run pytest tests/resources/ -v
```
Expected: all 4 tests PASS

- [ ] **Step 8: Commit**

```bash
git add voz_crawler/defs/resources/ tests/resources/test_arango_resource.py
git commit -m "feat(resources): add get_instance() factory with PrivateAttr caching, add OpenAIResource"
```

---

## Task 12: Update definitions.py to bind OpenAIResource

**Files:**
- Modify: `voz_crawler/defs/__init__.py`

- [ ] **Step 1: Rewrite `voz_crawler/defs/__init__.py`**

```python
from dagster import Definitions, EnvVar
from dagster_dlt import DagsterDltResource

from .assets.ingestion import voz_page_posts_assets
from .assets.reply_graph import (
    compute_embeddings,
    detect_implicit_edges,
    extract_explicit_edges,
    sync_posts_to_arango,
)
from .jobs.crawl import crawl_page_job, discover_pages_job
from .jobs.graph import reply_graph_job
from .resources.arango import ArangoResource
from .resources.crawler import CrawlerResource
from .resources.openai import OpenAIResource
from .resources.postgres import PostgresResource
from .sensors.crawl import voz_crawl_sensor, voz_discover_sensor
from .sensors.graph import reply_graph_sensor

postgres_resource = PostgresResource(
    user=EnvVar("POSTGRES_USER"),
    password=EnvVar("POSTGRES_PASSWORD"),
    host=EnvVar("POSTGRES_HOST"),
    port=EnvVar.int("POSTGRES_PORT"),
    db=EnvVar("POSTGRES_DB"),
)

crawler_resource = CrawlerResource(
    thread_url=EnvVar("VOZ_THREAD_URL"),
    flaresolverr_url=EnvVar("FLARESOLVERR_URL"),
    http_delay_seconds=EnvVar("HTTP_DELAY_SECONDS"),
    http_timeout_seconds=EnvVar.int("HTTP_TIMEOUT_SECONDS"),
)

arango_resource = ArangoResource(
    host=EnvVar("ARANGO_HOST"),
    port=EnvVar.int("ARANGO_PORT"),
    username=EnvVar("ARANGO_USER"),
    password=EnvVar("ARANGO_ROOT_PASSWORD"),
    db=EnvVar("ARANGO_DB"),
)

openai_resource = OpenAIResource(
    api_key=EnvVar("OPENAI_API_KEY"),
)

defs = Definitions(
    assets=[
        voz_page_posts_assets,
        sync_posts_to_arango,
        extract_explicit_edges,
        compute_embeddings,
        detect_implicit_edges,
    ],
    jobs=[crawl_page_job, discover_pages_job, reply_graph_job],
    sensors=[voz_discover_sensor, voz_crawl_sensor, reply_graph_sensor],
    resources={
        "dagster_dlt": DagsterDltResource(),
        "postgres": postgres_resource,
        "crawler": crawler_resource,
        "arango": arango_resource,
        "openai": openai_resource,
    },
)
```

- [ ] **Step 2: Commit**

```bash
git add voz_crawler/defs/__init__.py
git commit -m "feat(defs): bind OpenAIResource in Definitions"
```

---

## Task 13: Update defs/jobs/crawl.py and slim down assets

**Files:**
- Modify: `voz_crawler/defs/jobs/crawl.py`
- Modify: `voz_crawler/defs/assets/ingestion.py`
- Modify: `voz_crawler/defs/assets/reply_graph.py`

`★ Insight ─────────────────────────────────────`
The `discover_pages_op` currently calls the bare `fetch_via_flaresolverr` function from sources/. After refactor it will use `crawler.get_instance()` (a Fetcher), which means the op participates in the same connection-reuse pool as the rest of the Dagster run. This is a meaningful improvement — `requests.Session` keeps the TLS handshake alive across the op and any subsequent uses in the same process.
`─────────────────────────────────────────────────`

- [ ] **Step 1: Rewrite `defs/jobs/crawl.py`**

```python
from dagster import AssetSelection, OpExecutionContext, define_asset_job, job, op

from voz_crawler.core.crawl.pagination import build_page_url, discover_total_pages

from ..partitions import voz_pages_partitions
from ..resources.crawler import CrawlerResource

crawl_page_job = define_asset_job(
    name="crawl_page_job",
    selection=AssetSelection.groups("voz"),
    partitions_def=voz_pages_partitions,
    description="Crawl one Voz.vn thread page partition and load posts to PostgreSQL.",
)


@op
def discover_pages_op(context: OpExecutionContext, crawler: CrawlerResource) -> int:
    """Fetch page 1 to discover total pages, then register new partition keys."""
    fetcher = crawler.get_instance()
    page1_url = build_page_url(crawler.thread_url, 1)
    status_code, html = fetcher.fetch(page1_url)
    if fetcher.is_cf_block(status_code, html):
        raise RuntimeError(f"Cloudflare blocked page 1 discovery (HTTP {status_code}).")

    total_pages = discover_total_pages(html)
    context.log.info(f"Discovered {total_pages} total pages.")

    existing_keys: set[str] = set(context.instance.get_dynamic_partitions("voz_pages"))
    new_keys = [str(p) for p in range(1, total_pages + 1) if str(p) not in existing_keys]
    if new_keys:
        context.instance.add_dynamic_partitions("voz_pages", new_keys)
        preview = new_keys[:5] + (["..."] if len(new_keys) > 5 else [])
        context.log.info(f"Added {len(new_keys)} new partition(s): {preview}")
    else:
        context.log.info("No new pages to register.")

    return total_pages


@job(
    description=(
        "Discover new Voz thread pages and register them as partitions. "
        "Run manually or before the first crawl."
    )
)
def discover_pages_job():
    discover_pages_op()
```

- [ ] **Step 2: Update `defs/assets/ingestion.py`**

Only the import lines change — update both:

Replace:
```python
from voz_crawler.sources.voz_thread import voz_page_source
from voz_crawler.utils.pagination import build_page_url
```
with:
```python
from voz_crawler.core.crawl.source import voz_page_source
from voz_crawler.core.crawl.pagination import build_page_url
```

- [ ] **Step 3: Rewrite `defs/assets/reply_graph.py`**

```python
from dagster import AssetExecutionContext, MetadataValue, asset

from voz_crawler.core.db.posts import PostsRepository
from voz_crawler.core.graph.sync import GraphSyncService
from voz_crawler.core.llm.embeddings import EmbeddingService
from voz_crawler.core.llm.implicit_edges import ImplicitEdgeDetector

from ..resources.arango import ArangoResource
from ..resources.openai import OpenAIResource
from ..resources.postgres import PostgresResource


@asset(
    group_name="reply_graph",
    description="Sync new posts from PostgreSQL into ArangoDB Posts collection (incremental).",
)
def sync_posts_to_arango(
    context: AssetExecutionContext,
    arango: ArangoResource,
    postgres: PostgresResource,
) -> None:
    repo = PostsRepository(engine=postgres.get_instance().engine)
    svc = GraphSyncService(arango_db=arango.get_instance().db(), posts_repo=repo)
    stats = svc.sync_posts()
    context.log.info(f"Synced {stats['records_processed']} posts.")
    context.add_output_metadata(
        {k: MetadataValue.int(v) for k, v in stats.items() if isinstance(v, int)}
    )


@asset(
    deps=["sync_posts_to_arango"],
    group_name="reply_graph",
    description="Parse HTML quote blocks and upsert explicit edges into the quotes collection.",
)
def extract_explicit_edges(
    context: AssetExecutionContext,
    arango: ArangoResource,
    postgres: PostgresResource,
) -> None:
    repo = PostsRepository(engine=postgres.get_instance().engine)
    svc = GraphSyncService(arango_db=arango.get_instance().db(), posts_repo=repo)
    stats = svc.extract_explicit_edges()
    context.log.info(f"Upserted {stats['edges_processed']} quote edges.")
    context.add_output_metadata({"edges_processed": MetadataValue.int(stats["edges_processed"])})


@asset(
    deps=["sync_posts_to_arango"],
    group_name="reply_graph",
    description="Batch-embed posts without embeddings using text-embedding-3-small.",
)
def compute_embeddings(
    context: AssetExecutionContext,
    arango: ArangoResource,
    openai: OpenAIResource,
) -> None:
    svc = EmbeddingService(client=openai.get_instance())
    stats = svc.compute_missing(arango.get_instance().db())
    context.log.info(f"Embedded {stats['records_processed']} posts.")
    context.add_output_metadata({"records_processed": MetadataValue.int(stats["records_processed"])})


@asset(
    deps=["compute_embeddings"],
    group_name="reply_graph",
    description="Detect implicit replies via temporal+ANN+LLM and upsert into implicit_replies.",
)
def detect_implicit_edges(
    context: AssetExecutionContext,
    arango: ArangoResource,
    openai: OpenAIResource,
) -> None:
    detector = ImplicitEdgeDetector(
        client=openai.get_instance(),
        arango_db=arango.get_instance().db(),
    )
    stats = detector.detect()
    context.log.info(f"Upserted {stats['edges_created']} implicit reply edges.")
    context.add_output_metadata({"edges_upserted": MetadataValue.int(stats["edges_created"])})
```

- [ ] **Step 4: Run all tests**

```bash
uv run pytest -v
```
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
git add voz_crawler/defs/jobs/crawl.py \
        voz_crawler/defs/assets/ingestion.py \
        voz_crawler/defs/assets/reply_graph.py
git commit -m "refactor(defs): slim assets to thin orchestration, update imports to core/"
```

---

## Task 14: Delete old utils/ and sources/ directories

**Files:**
- Delete: `voz_crawler/utils/` (entire directory)
- Delete: `voz_crawler/sources/` (entire directory)

- [ ] **Step 1: Verify no remaining imports from old paths**

```bash
grep -r "voz_crawler.utils\|voz_crawler.sources" voz_crawler/ tests/
```
Expected: no output

- [ ] **Step 2: Delete old directories**

```bash
git rm -r voz_crawler/utils/ voz_crawler/sources/
```

- [ ] **Step 3: Run all tests to confirm nothing broke**

```bash
uv run pytest -v
```
Expected: all tests PASS

- [ ] **Step 4: Commit**

```bash
git commit -m "chore: remove voz_crawler/utils/ and voz_crawler/sources/ (migrated to core/)"
```

---

## Task 15: Final verification

- [ ] **Step 1: Lint and format**

```bash
uv run ruff check . && uv run ruff format .
```
Expected: no errors (ruff format may adjust whitespace — commit if it does)

- [ ] **Step 2: Run full test suite**

```bash
uv run pytest -v
```
Expected: all tests PASS

- [ ] **Step 3: Confirm core has zero Dagster imports**

```bash
uv run python -c "
import sys
import voz_crawler.core.crawl.parser
import voz_crawler.core.crawl.fetcher
import voz_crawler.core.crawl.pagination
import voz_crawler.core.graph.client
import voz_crawler.core.graph.sync
import voz_crawler.core.llm.embeddings
import voz_crawler.core.llm.implicit_edges
import voz_crawler.core.db.postgres
import voz_crawler.core.db.posts
dagster_imported = any('dagster' in m for m in sys.modules)
assert not dagster_imported, f'dagster leaked into core: {[m for m in sys.modules if \"dagster\" in m]}'
print('OK: core has no Dagster imports')
"
```
Expected: `OK: core has no Dagster imports`

- [ ] **Step 4: Confirm Dagster loads definitions**

```bash
uv run dagster asset list 2>&1 | head -20
```
Expected: list of asset keys including `voz/voz__posts`, `reply_graph/sync_posts_to_arango`, etc.

- [ ] **Step 5: Commit any ruff fixes**

```bash
git add -u
git diff --cached --quiet || git commit -m "style: apply ruff format after refactor"
```
