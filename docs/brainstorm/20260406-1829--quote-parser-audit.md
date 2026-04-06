# Quote Parser Audit — `extract_explicit_edges`

**Date**: 2026-04-06

**Date**: 2026-04-06

**Status**: Audit complete — minor gap, no structural fix needed
**Scope**: `voz_crawler/core/graph/quote_parser.py` + `extract_explicit_edges` asset

---

## Findings

### Blockquote types in the dataset (3,000-post sample)

| Type | Count | Handled |
|---|---|---|
| `data-source="post: {id}"` — standard quote button | 3,104 | ✅ Edge created |
| `data-source=""`, `data-quote=""` — unattributed manual quote | 3 | ⚠️ Silently skipped |

All real, resolvable quotes use `data-source="post: {id}"`. The current CSS selector `blockquote[data-source]` matches both types (attribute presence, not value), but the `startswith("post: ")` guard correctly discards the empty-attribute case.

**No edge can be created for unattributed quotes** — there is no source post ID to link to. The current behavior is logically correct.

### What an unattributed quote looks like

```html
<blockquote class="bbCodeBlock bbCodeBlock--expandable bbCodeBlock--quote js-expandWatch"
            data-attributes=""
            data-quote=""
            data-source="">
  <div class="bbCodeBlock-content">
    <div class="bbCodeBlock-expandContent js-expandContent">
      m apply vai trò tech lead back-end, deal 3k
    </div>
  </div>
</blockquote>
```

This happens when a user writes `[QUOTE]...[/QUOTE]` manually in the BBCode editor instead of clicking the forum's quote button. The forum renders it as a visual quote block but stores no attribution metadata.

---

## Gaps to fix

### Gap 1 — Unattributed quotes are invisible to `extract_explicit_edges`

The asset logs `edges_inserted` and `edges_dropped` but has no counter for unresolvable quotes. These are silently dropped.

**Fix**: Return a count of skipped unattributed quotes from `extract_quote_edges` and surface it in `MaterializeResult` metadata.

```python
# quote_parser.py — updated return signature
def extract_quote_edges(
    html: str, source_post_id: int, partition_key: str
) -> tuple[list[ArangoEdge], int]:   # (edges, unresolvable_count)
```

### Gap 2 — Normalization step does not extract unattributed quote content

`normalize_post_html` (content normalization for LLM input) strips blockquotes and extracts their text into `quoted_blocks`. For unattributed quotes, the quoted text should still appear in `quoted_blocks` — just with `source_post_id=None` and `author=None`. This is already handled by the normalization function since it uses `data-quote` and `data-source` independently from the CSS selector.

No change needed to `normalize_post_html` for this case — it already handles `data-source=""` gracefully (sets `source_post_id=None`).

---

## No other quote types found

After auditing all 3,000 posts with blockquote HTML:
- No `data-source` values other than `"post: {id}"` or `""` were found.
- No nested blockquotes (blockquote inside blockquote) — XenForo strips inner nesting before rendering.
- No `data-quote`-only blockquotes (data-quote always co-occurs with data-source).

The XenForo quote format is uniform. The parser does not need structural changes.

---

## Required changes

### `voz_crawler/core/graph/quote_parser.py`

Change `extract_quote_edges` to return `(edges, unresolvable_count)`:

```python
def extract_quote_edges(
    html: str, source_post_id: int, partition_key: str
) -> tuple[list[ArangoEdge], int]:
    ...
    unresolvable = 0
    for ordinal, blockquote in enumerate(soup.select("blockquote[data-source]"), start=1):
        raw = blockquote.get("data-source", "")
        if not raw.startswith("post: "):
            unresolvable += 1   # count but cannot create edge
            continue
        ...
    return edges, unresolvable
```

### `voz_crawler/core/graph/edge_sync.py`

Update `build_edges` to propagate the count:

```python
def build_edges(rows, partition_key) -> tuple[list[ArangoEdge], int]:
    edges, total_unresolvable = [], 0
    for r in rows:
        new_edges, unresolvable = extract_quote_edges(...)
        edges.extend(new_edges)
        total_unresolvable += unresolvable
    return edges, total_unresolvable
```

### `voz_crawler/defs/assets/reply_graph.py` — `extract_explicit_edges`

Add `unresolvable_quotes` to `MaterializeResult` metadata.