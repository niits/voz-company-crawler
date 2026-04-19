from dataclasses import dataclass
from datetime import datetime, timezone

from voz_crawler.core.entities.arango import ExtractionResultDoc
from voz_crawler.core.entities.company import AliasEvidenceDoc, CompanyMentionDoc, MentionEdge
from voz_crawler.core.entities.enrichment import ENRICHMENT_VERSION


@dataclass
class CompanyMentionStats:
    mention_docs: int = 0
    mention_edges: int = 0
    alias_evidence_docs: int = 0


def build_company_mention_docs(
    extraction_results: list[ExtractionResultDoc],
    partition_key: str,
) -> tuple[list[CompanyMentionDoc], list[MentionEdge], list[AliasEvidenceDoc]]:
    """Convert ExtractionResultDoc records into CompanyMentionDoc + MentionEdge + AliasEvidenceDoc."""
    mention_docs: list[CompanyMentionDoc] = []
    mention_edges: list[MentionEdge] = []
    alias_evidence_docs: list[AliasEvidenceDoc] = []
    now = datetime.now(timezone.utc).isoformat()

    for doc in extraction_results:
        post_key = doc.post_key

        for ordinal, mention in enumerate(doc.company_mentions, start=1):
            mention_key = f"{post_key}_{ordinal}"
            mention_docs.append(
                CompanyMentionDoc(
                    key=mention_key,
                    post_key=post_key,
                    partition_key=partition_key,
                    company_name_raw=mention["company_name_raw"],
                    identification_clues=mention.get("identification_clues", []),
                    company_name=mention["company_name"],
                    company_type=mention.get("company_type"),
                    mention_type=mention["mention_type"],
                    sentiment=mention.get("sentiment"),
                    summary=mention.get("summary"),
                )
            )
            mention_edges.append(
                MentionEdge(
                    from_vertex=f"posts/{post_key}",
                    to_vertex=f"company_mentions/{mention_key}",
                    key=f"{post_key}_mention_{ordinal}",
                    partition_key=partition_key,
                )
            )

        for alias_def in doc.alias_definitions:
            import re

            alias_slug = re.sub(r"[^a-z0-9]+", "-", alias_def["alias"].lower()).strip("-")
            company_slug = re.sub(r"[^a-z0-9]+", "-", alias_def["company_name"].lower()).strip("-")
            alias_evidence_docs.append(
                AliasEvidenceDoc(
                    key=f"{post_key}_{alias_slug}",
                    alias=alias_def["alias"],
                    alias_slug=alias_slug,
                    company_key=company_slug,
                    source_post_key=post_key,
                    partition_key=partition_key,
                    evidence_type=alias_def["evidence_type"],
                    confidence=alias_def["confidence"],
                    enrichment_version=ENRICHMENT_VERSION,
                    extracted_at=now,
                )
            )

    return mention_docs, mention_edges, alias_evidence_docs
