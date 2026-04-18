from dataclasses import dataclass, field
from datetime import datetime, timezone

from openai import AsyncOpenAI
from pydantic_ai import Agent
from pydantic_ai.messages import ModelMessagesTypeAdapter
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider

from voz_crawler.core.entities.arango import EmbedItem, ExtractionResultDoc, NormalizedPostDoc
from voz_crawler.core.entities.enrichment import (
    ENRICHMENT_VERSION,
    PostEnrichmentResult,
)

_SYSTEM_PROMPT = (
    "Bạn là hệ thống phân tích bài viết trên diễn đàn IT Việt Nam (Voz.vn).\n\n"
    "NHIỆM VỤ:\n"
    "1. Phân loại bài viết theo content_class:\n"
    "   - review: đánh giá trải nghiệm làm việc tại công ty (môi trường, văn hóa, quản lý)\n"
    "   - rating: chỉ cho điểm / xếp hạng công ty, ít mô tả\n"
    "   - event: thông báo tuyển dụng, layoff, tin tức công ty\n"
    "   - question: hỏi về công ty, lương, quy trình\n"
    "   - noise: chào hỏi, reaction ngắn (+1, hihi), dưới 10 từ, off-topic\n\n"
    "2. Trích xuất TẤT CẢ công ty được đề cập bởi TÁC GIẢ bài viết:\n"
    "   - identification_clues: biệt danh / alias NGUYÊN VĂN trong bài\n"
    '     (ví dụ: "nhà F", "ép sọt", "cty cá", "big4") — KHÔNG giải thích\n'
    '   - company_name: tên chuẩn hóa (bỏ "công ty", "cty TNHH", "JSC"; viết hoa đúng)\n'
    "   - company_type: it | outsourcing | product | startup | bank | telco | unknown\n"
    "   - mention_type: positive_review | negative_review | salary_info"
    " | layoff | hiring | event | general\n"
    "   - sentiment: positive | negative | neutral\n"
    "   - summary: một câu tóm tắt điều tác giả nói về công ty này\n\n"
    "3. Ghi lại alias definitions nếu tác giả GIẢI THÍCH hoặc ĐỊnh nghĩa một alias:\n"
    "   - alias: biệt danh nguyên văn\n"
    "   - company_name: tên công ty mà alias này trỏ đến\n"
    "   - evidence_type: explicit_definition | disambiguation | co_occurrence\n"
    "   - confidence: 0.0-1.0\n\n"
    "QUY TẮC:\n"
    "- Phần trích dẫn đã được loại bỏ — chỉ phân tích lời của tác giả.\n"
    "- Nếu không có công ty nào được nhắc, trả về company_mentions = [].\n"
    "- Đặt post_key chính xác theo giá trị được cung cấp trong đầu vào.\n"
)


@dataclass
class EnrichmentStats:
    enriched: int = 0
    skipped: int = 0
    errors: int = 0
    request_tokens: int = 0
    response_tokens: int = 0
    error_keys: list[str] = field(default_factory=list)


def run_llm_enrichment(
    items: list[EmbedItem],
    api_key: str,
    partition_key: str,
) -> tuple[list[ExtractionResultDoc], list[NormalizedPostDoc], EnrichmentStats]:
    """Pure LLM enrichment: classify posts and build extraction docs.

    Takes pre-fetched items (key + normalized_own_text). No DB access — designed
    for in-memory op chains where the single write happens at the end of the group.

    Returns (extraction_docs, enrichment_patches, stats).
    """
    async_client = AsyncOpenAI(api_key=api_key)
    model = OpenAIModel("gpt-4o-mini", provider=OpenAIProvider(openai_client=async_client))
    agent = Agent(
        model=model,
        output_type=PostEnrichmentResult,
        system_prompt=_SYSTEM_PROMPT,
        model_settings={"temperature": 0},
    )

    stats = EnrichmentStats(skipped=0)
    extraction_docs: list[ExtractionResultDoc] = []
    enrichment_patches: list[NormalizedPostDoc] = []

    for item in items:
        try:
            result = agent.run_sync(f"post_key: {item.key}\n\n{item.text}")
            output: PostEnrichmentResult = result.output
            usage = result.usage()

            stats.request_tokens += usage.request_tokens or 0
            stats.response_tokens += usage.response_tokens or 0

            messages_json = ModelMessagesTypeAdapter.dump_python(result.all_messages(), mode="json")

            extraction_docs.append(
                ExtractionResultDoc(
                    key=f"{item.key}_{ENRICHMENT_VERSION}",
                    post_key=item.key,
                    partition_key=partition_key,
                    enrichment_version=ENRICHMENT_VERSION,
                    content_class=output.classification.content_class,
                    content_class_confidence=output.classification.confidence,
                    company_mentions=[m.model_dump() for m in output.company_mentions],
                    alias_definitions=[a.model_dump() for a in output.alias_definitions],
                    implicit_replies=[],
                    messages_json=messages_json,
                    model_used="gpt-4o-mini",
                    extracted_at=datetime.now(timezone.utc).isoformat(),
                )
            )
            enrichment_patches.append(
                NormalizedPostDoc(
                    key=item.key,
                    content_class=output.classification.content_class,
                    content_class_confidence=output.classification.confidence,
                    has_company_mention=len(output.company_mentions) > 0,
                    enrichment_version=ENRICHMENT_VERSION,
                )
            )
            stats.enriched += 1

        except Exception:
            stats.errors += 1
            stats.error_keys.append(item.key)

    return extraction_docs, enrichment_patches, stats


def enrich_partition(
    repo,
    api_key: str,
    partition_key: str,
) -> EnrichmentStats:
    """Run LLM enrichment for all posts in a partition that need it.

    Uses AsyncOpenAI client via PydanticAI. Token totals are accumulated manually
    from result.usage() and reported in MaterializeResult metadata by the asset.
    Serial within partition; partition-level concurrency managed by Dagster concurrency key.
    """
    async_client = AsyncOpenAI(api_key=api_key)
    model = OpenAIModel("gpt-4o-mini", provider=OpenAIProvider(openai_client=async_client))
    agent = Agent(
        model=model,
        output_type=PostEnrichmentResult,
        system_prompt=_SYSTEM_PROMPT,
        model_settings={"temperature": 0},
    )

    posts = repo.fetch_posts_needing_enrichment(partition_key, ENRICHMENT_VERSION)
    stats = EnrichmentStats(skipped=0)

    extraction_docs: list[ExtractionResultDoc] = []
    enrichment_patches: list[NormalizedPostDoc] = []

    for item in posts:
        try:
            result = agent.run_sync(f"post_key: {item.key}\n\n{item.text}")
            output: PostEnrichmentResult = result.output
            usage = result.usage()

            stats.request_tokens += usage.request_tokens or 0
            stats.response_tokens += usage.response_tokens or 0

            messages_json = ModelMessagesTypeAdapter.dump_python(result.all_messages(), mode="json")

            extraction_docs.append(
                ExtractionResultDoc(
                    key=f"{item.key}_{ENRICHMENT_VERSION}",
                    post_key=item.key,
                    partition_key=partition_key,
                    enrichment_version=ENRICHMENT_VERSION,
                    content_class=output.classification.content_class,
                    content_class_confidence=output.classification.confidence,
                    company_mentions=[m.model_dump() for m in output.company_mentions],
                    alias_definitions=[a.model_dump() for a in output.alias_definitions],
                    implicit_replies=[],
                    messages_json=messages_json,
                    model_used="gpt-4o-mini",
                    extracted_at=datetime.now(timezone.utc).isoformat(),
                )
            )
            enrichment_patches.append(
                NormalizedPostDoc(
                    key=item.key,
                    content_class=output.classification.content_class,
                    content_class_confidence=output.classification.confidence,
                    has_company_mention=len(output.company_mentions) > 0,
                    enrichment_version=ENRICHMENT_VERSION,
                )
            )
            stats.enriched += 1

        except Exception:
            stats.errors += 1
            stats.error_keys.append(item.key)

    if extraction_docs:
        repo.upsert_extraction_results(extraction_docs)
    if enrichment_patches:
        repo.patch_post_enrichment_fields(enrichment_patches)

    return stats
