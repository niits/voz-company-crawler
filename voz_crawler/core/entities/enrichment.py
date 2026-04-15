from pydantic import BaseModel

# Bump NORMALIZATION_VERSION to reprocess all posts through normalize_posts.
# Bump ENRICHMENT_VERSION to reprocess all LLM enrichments (re-calls the LLM).
NORMALIZATION_VERSION = 1
ENRICHMENT_VERSION = 1


class ContentClassification(BaseModel):
    content_class: str      # review | rating | event | question | noise
    confidence: float       # 0.0–1.0


class CompanyMentionResult(BaseModel):
    company_name_raw: str
    identification_clues: list[str]     # aliases/slang as-found in post; never resolved here
    company_name: str                   # LLM best-effort normalized name
    company_type: str                   # it | outsourcing | product | startup | bank | telco | unknown
    mention_type: str                   # positive_review | negative_review | salary_info | layoff | hiring | event | general
    sentiment: str                      # positive | negative | neutral
    summary: str


class AliasDefinitionResult(BaseModel):
    alias: str
    company_name: str           # what the LLM believes this alias resolves to
    evidence_type: str          # explicit_definition | co_occurrence | disambiguation
    confidence: float


class PostEnrichmentResult(BaseModel):
    post_key: str
    classification: ContentClassification
    company_mentions: list[CompanyMentionResult]
    alias_definitions: list[AliasDefinitionResult]  # empty list if none found


class ImplicitReply(BaseModel):
    candidate_key: str
    confidence: float
    reasoning: str


class ImplicitReplyDecision(BaseModel):
    source_key: str
    replies_to: list[ImplicitReply]
