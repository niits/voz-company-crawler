"""Cross-thread company/alias resolution — pure transforms.

Aggregates AliasEvidenceDoc records (written per-post by extract_company_mentions)
into CompanyAliasDoc resolutions + CompanyDoc stubs, and computes the company_key
each CompanyMentionDoc should point to. No DB access — the resolve_companies asset
fetches inputs, calls resolve(), and writes the outputs.

This is the one place forum slang is unified: "nhà F", "ép sọt", "cty cá" → a
single canonical company, using human-provided alias definitions as evidence.
"""

import re
from collections import Counter, defaultdict

from voz_crawler.core.entities.company import CompanyAliasDoc, CompanyDoc
from voz_crawler.core.entities.enrichment import RESOLUTION_VERSION

# ── Tunables (domain knowledge lives here, not in the logic) ──────────────────
# How much each evidence kind counts toward an alias→company score.
EVIDENCE_TYPE_WEIGHTS = {
    "explicit_definition": 1.0,  # author literally said "nhà F = FPT"
    "disambiguation": 0.7,  # author clarified which company among several
    "co_occurrence": 0.3,  # alias and company name appeared together
    "contradiction": -0.5,  # author said the alias is NOT this company
}
# Raw weighted-sum at which a candidate is considered fully confident (score → 1.0).
SCORE_SATURATION = 2.0
# Top-2 score gap below which an alias is ambiguous.
AMBIGUITY_GAP = 0.15
# Top score below which an alias is considered unresolved.
UNRESOLVED_THRESHOLD = 0.4


def slugify(text: str) -> str:
    """Deterministic slug: lowercase, non-alphanumerics → single hyphen.

    Matches the slug used by company_sync so alias evidence and mention clues
    hash to the same key. (Vietnamese diacritics collapse to hyphens — crude but
    consistent; proper transliteration is a future improvement.)
    """
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")


def _score(raw: float) -> float:
    """Squash a raw weighted-sum into [0, 1] via linear saturation."""
    return max(0.0, min(1.0, raw / SCORE_SATURATION))


def resolve_aliases(evidence: list[dict]) -> list[CompanyAliasDoc]:
    """Aggregate alias_evidence into ranked CompanyAliasDoc resolutions."""
    by_alias: dict[str, list[dict]] = defaultdict(list)
    alias_text: dict[str, str] = {}
    for ev in evidence:
        slug = ev["alias_slug"]
        by_alias[slug].append(ev)
        alias_text.setdefault(slug, ev.get("alias", slug))

    docs: list[CompanyAliasDoc] = []
    for slug, evs in by_alias.items():
        agg: dict[str, dict] = {}
        for ev in evs:
            ck = ev["company_key"]
            weight = EVIDENCE_TYPE_WEIGHTS.get(ev["evidence_type"], 0.0)
            entry = agg.setdefault(
                ck, {"raw": 0.0, "count": 0, "last_post": None, "last_updated": ""}
            )
            entry["raw"] += float(ev.get("confidence", 0.0)) * weight
            entry["count"] += 1
            updated = ev.get("extracted_at", "")
            if updated >= entry["last_updated"]:
                entry["last_updated"] = updated
                entry["last_post"] = ev.get("source_post_key")

        resolutions = sorted(
            (
                {
                    "company_key": ck,
                    "confidence": round(_score(e["raw"]), 4),
                    "evidence_count": e["count"],
                    "last_evidence_post_key": e["last_post"],
                    "last_updated": e["last_updated"],
                }
                for ck, e in agg.items()
            ),
            key=lambda r: r["confidence"],
            reverse=True,
        )

        top = resolutions[0]
        second = resolutions[1] if len(resolutions) > 1 else None
        gap = top["confidence"] - (second["confidence"] if second else 0.0)
        is_unresolved = top["confidence"] < UNRESOLVED_THRESHOLD
        is_ambiguous = second is not None and gap < AMBIGUITY_GAP
        best = top["company_key"] if (gap > AMBIGUITY_GAP and not is_unresolved) else None

        docs.append(
            CompanyAliasDoc(
                key=slug,
                alias=alias_text[slug],
                alias_normalized=alias_text[slug].lower(),
                resolutions=resolutions,
                best_company_key=best,
                is_ambiguous=is_ambiguous,
                is_unresolved=is_unresolved,
                resolution_version=RESOLUTION_VERSION,
            )
        )
    return docs


def build_company_stubs(company_keys: set[str], mentions: list[dict], now: str) -> list[CompanyDoc]:
    """One CompanyDoc stub per referenced company_key.

    canonical_name / company_type are the most frequent values seen in mentions
    whose own company_name slugifies to that key.
    """
    names: dict[str, Counter] = defaultdict(Counter)
    types: dict[str, Counter] = defaultdict(Counter)
    for m in mentions:
        ck = slugify(m.get("company_name", ""))
        if ck:
            names[ck][m.get("company_name") or ck] += 1
            if m.get("company_type"):
                types[ck][m["company_type"]] += 1

    docs: list[CompanyDoc] = []
    for ck in sorted(company_keys):
        canonical = names[ck].most_common(1)[0][0] if names.get(ck) else ck
        ctype = types[ck].most_common(1)[0][0] if types.get(ck) else None
        docs.append(
            CompanyDoc(
                key=ck,
                canonical_name=canonical,
                company_type=ctype,
                is_stub=True,
                created_at=now,
                updated_at=now,
            )
        )
    return docs


def assign_mention_resolutions(mentions: list[dict], aliases: list[CompanyAliasDoc]) -> list[dict]:
    """Compute the company_key patch for each mention.

    Alias evidence wins over the LLM's per-post company_name guess: a confident
    alias resolution is corpus-wide canonical, whereas company_name varies per post
    ("FPT" vs "FPT Software"). Falls back to the mention's own name slug.

    Returns patch dicts: {_key, company_key, is_ambiguous, resolution_version}.
    """
    alias_index = {a.key: a for a in aliases}
    patches: list[dict] = []
    for m in mentions:
        company_key: str | None = None
        is_ambiguous = False

        for clue in m.get("identification_clues") or []:
            entry = alias_index.get(slugify(clue))
            if entry and entry.best_company_key:
                company_key = entry.best_company_key
                is_ambiguous = entry.is_ambiguous
                break

        if company_key is None:
            company_key = slugify(m.get("company_name", "")) or None

        patches.append(
            {
                "_key": m["_key"],
                "company_key": company_key,
                "is_ambiguous": is_ambiguous,
                "resolution_version": RESOLUTION_VERSION,
            }
        )
    return patches


def resolve(
    evidence: list[dict], mentions: list[dict], now: str
) -> tuple[list[CompanyAliasDoc], list[CompanyDoc], list[dict]]:
    """Full resolution pass. Returns (aliases, company_stubs, mention_patches)."""
    aliases = resolve_aliases(evidence)
    mention_patches = assign_mention_resolutions(mentions, aliases)

    referenced = {p["company_key"] for p in mention_patches if p["company_key"]}
    referenced.update(ev["company_key"] for ev in evidence if ev.get("company_key"))
    companies = build_company_stubs(referenced, mentions, now)

    return aliases, companies, mention_patches
