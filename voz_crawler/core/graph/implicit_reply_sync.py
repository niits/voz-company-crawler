from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np
from openai import AsyncOpenAI
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider

from voz_crawler.core.entities.arango import ArangoEdge
from voz_crawler.core.entities.enrichment import (
    ENRICHMENT_VERSION,
    ImplicitReplyDecision,
)

# ── Adaptive window parameters ────────────────────────────────────────────────
RATE_WINDOW_HOURS = 2
BASE_N = 5
BASE_RATE = 10.0        # posts/hour reference rate
MIN_WINDOW = 2
MAX_WINDOW = 20
BASE_LOOKBACK = 24.0    # hours at reference rate
MIN_RATE = 1.0
MIN_LOOKBACK_H = 2.0
MAX_LOOKBACK_H = 168.0  # 7 days

# ── Scoring parameters ────────────────────────────────────────────────────────
MIN_EMB_SIM = 0.65
CONFIDENCE_THRESHOLD = 0.6
TOP_N_CANDIDATES = 5

_SYSTEM_PROMPT = (
    "Bạn đang phân tích một thread diễn đàn IT Việt Nam.\n"
    "Cho một bài viết nguồn và danh sách candidates, xác định bài nào "
    "mà source post đang ngầm reply tới (không dùng quote tag tường minh).\n"
    "Chỉ chấp nhận confidence >= 0.60. Trả về replies_to=[] nếu không có.\n"
    "Ghi reasoning ngắn gọn cho mỗi candidate được chọn."
)


@dataclass
class ImplicitReplyStats:
    posts_processed: int = 0
    edges_inserted: int = 0
    errors: int = 0
    error_keys: list[str] = field(default_factory=list)


def compute_adaptive_window(reply_rate: float) -> tuple[int, float]:
    """Compute (adaptive_n, max_lookback_hours) from local reply rate (posts/hour)."""
    adaptive_n = round(BASE_N * (reply_rate / BASE_RATE))
    adaptive_n = max(MIN_WINDOW, min(MAX_WINDOW, adaptive_n))
    effective_rate = max(reply_rate, MIN_RATE)
    max_lookback = BASE_LOOKBACK * (BASE_RATE / effective_rate)
    max_lookback = max(MIN_LOOKBACK_H, min(MAX_LOOKBACK_H, max_lookback))
    return adaptive_n, max_lookback


def get_window_keys(posts_sorted: list[dict], target_idx: int) -> list[str]:
    """Return the adaptive window of candidate post keys for the post at target_idx."""
    target = posts_sorted[target_idx]
    t_p = datetime.fromisoformat(target["posted_at"])
    win_start = t_p - timedelta(hours=RATE_WINDOW_HOURS)

    recent = [
        p for p in posts_sorted[:target_idx]
        if datetime.fromisoformat(p["posted_at"]) >= win_start
    ]
    reply_rate = len(recent) / RATE_WINDOW_HOURS
    adaptive_n, max_lookback = compute_adaptive_window(reply_rate)

    lb_start = t_p - timedelta(hours=max_lookback)
    candidates = [
        p for p in posts_sorted[:target_idx]
        if datetime.fromisoformat(p["posted_at"]) >= lb_start
    ]
    return [p["key"] for p in candidates[-adaptive_n:]]


def _cosine_sim(a: list[float], b: list[float]) -> float:
    va, vb = np.array(a), np.array(b)
    denom = np.linalg.norm(va) * np.linalg.norm(vb)
    return float(np.dot(va, vb) / denom) if denom > 0 else 0.0


def _rerank_with_embedding(
    candidates: list[dict], source_embedding: list[float]
) -> list[dict]:
    """Add emb_sim to candidates and re-sort by combined score."""
    for c in candidates:
        emb = c.get("embedding")
        c["emb_sim"] = _cosine_sim(source_embedding, emb) if emb else 0.0
        # Re-weight: 0.5*emb + 0.3*bm25_norm + window_boost already in c["score"]
        bm25_norm = min(c.get("bm25", 0.0) / 5.0, 1.0)
        window_boost = 0.2 if c.get("score", 0) > 0.2 else 0.0  # preserved from AQL
        c["score"] = 0.5 * c["emb_sim"] + 0.3 * bm25_norm + window_boost

    # Filter: must pass embedding similarity floor (unless in window)
    filtered = [
        c for c in candidates
        if c["emb_sim"] >= MIN_EMB_SIM or c.get("score", 0) > 0.2
    ]
    return sorted(filtered, key=lambda x: x["score"], reverse=True)[:TOP_N_CANDIDATES]


def process_partition_implicit_replies(
    repo,
    api_key: str,
    partition_key: str,
) -> ImplicitReplyStats:
    """Detect implicit reply edges for all posts in a partition.

    Stage 1: AQL query (BM25 + window boost) → re-rank with cosine similarity.
    Stage 2: LLM agent decides which candidates are genuine implicit replies.
    Inserts ArangoEdge(method="implicit_llm") and patches ExtractionResultDoc.
    """
    async_client = AsyncOpenAI(api_key=api_key)
    model = OpenAIModel("gpt-4o-mini", provider=OpenAIProvider(openai_client=async_client))
    agent = Agent(
        model=model,
        output_type=ImplicitReplyDecision,
        system_prompt=_SYSTEM_PROMPT,
        model_settings={"temperature": 0},
    )

    posts = repo.fetch_partition_posts_for_implicit_reply(partition_key)
    stats = ImplicitReplyStats()

    if len(posts) < 2:
        return stats

    edges_to_insert: list[ArangoEdge] = []

    for idx in range(1, len(posts)):
        src = posts[idx]
        try:
            window_keys = get_window_keys(posts, idx)
            raw_candidates = repo.fetch_implicit_reply_candidates(
                partition_key=partition_key,
                source_key=src["key"],
                source_text=src["text"],
                window_keys=window_keys,
                top_n=TOP_N_CANDIDATES * 2,  # over-fetch before embedding re-rank
            )

            if not raw_candidates:
                stats.posts_processed += 1
                continue

            src_embedding = src.get("embedding")
            if src_embedding:
                top = _rerank_with_embedding(raw_candidates, src_embedding)
            else:
                top = raw_candidates[:TOP_N_CANDIDATES]

            if not top:
                stats.posts_processed += 1
                continue

            candidate_text = "\n\n".join(
                f"[candidate {c['key']}] {(c.get('text') or '')[:300]}"
                for c in top
            )
            prompt = (
                f"SOURCE POST [{src['key']}]:\n{(src.get('text') or '')[:500]}\n\n"
                f"CANDIDATES:\n{candidate_text}"
            )

            result = agent.run_sync(prompt)
            decision: ImplicitReplyDecision = result.output

            new_implicit: list[dict] = []
            for reply in decision.replies_to:
                if reply.confidence < CONFIDENCE_THRESHOLD:
                    continue
                edge_key = f"{src['key']}_impl_{reply.candidate_key}"
                edges_to_insert.append(
                    ArangoEdge(
                        from_vertex=f"posts/{src['key']}",
                        to_vertex=f"posts/{reply.candidate_key}",
                        key=edge_key,
                        quote_ordinal=0,
                        confidence=reply.confidence,
                        method="implicit_llm",
                        partition_key=partition_key,
                    )
                )
                new_implicit.append(
                    {
                        "candidate_key": reply.candidate_key,
                        "confidence": reply.confidence,
                        "reasoning": reply.reasoning,
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                    }
                )

            if new_implicit:
                repo.patch_extraction_implicit_replies(
                    src["key"], ENRICHMENT_VERSION, new_implicit
                )
            stats.posts_processed += 1

        except Exception:
            stats.errors += 1
            stats.error_keys.append(src["key"])

    if edges_to_insert:
        repo.insert_implicit_edges(edges_to_insert)
        stats.edges_inserted = len(edges_to_insert)

    return stats
