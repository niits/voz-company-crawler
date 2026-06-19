"""Unit tests for voz_crawler.core.graph.company_resolution (pure transforms)."""

from voz_crawler.core.graph.company_resolution import (
    RESOLUTION_VERSION,
    assign_mention_resolutions,
    build_company_stubs,
    resolve,
    resolve_aliases,
    slugify,
)


def _evidence(alias, company_key, evidence_type, confidence, post="p1", at="2024-01-01T00:00:00"):
    return {
        "alias": alias,
        "alias_slug": slugify(alias),
        "company_key": company_key,
        "evidence_type": evidence_type,
        "confidence": confidence,
        "source_post_key": post,
        "extracted_at": at,
    }


# ── slugify ───────────────────────────────────────────────────────────────────


def test_slugify_basic():
    assert slugify("FPT Software") == "fpt-software"


def test_slugify_handles_empty():
    assert slugify("") == ""
    assert slugify(None) == ""


# ── resolve_aliases ───────────────────────────────────────────────────────────


def test_single_explicit_definition_is_confident():
    aliases = resolve_aliases([_evidence("nhà F", "fpt", "explicit_definition", 1.0)])
    assert len(aliases) == 1
    a = aliases[0]
    assert a.key == slugify("nhà F")
    assert a.best_company_key == "fpt"
    assert a.is_ambiguous is False
    assert a.is_unresolved is False
    assert a.resolutions[0]["confidence"] == 0.5  # 1.0 weight * 1.0 conf / saturation 2.0


def test_two_definitions_saturate_to_one():
    aliases = resolve_aliases(
        [
            _evidence("nhà F", "fpt", "explicit_definition", 1.0, post="p1"),
            _evidence("nhà F", "fpt", "explicit_definition", 1.0, post="p2"),
        ]
    )
    assert aliases[0].resolutions[0]["confidence"] == 1.0
    assert aliases[0].resolutions[0]["evidence_count"] == 2


def test_close_competitors_are_ambiguous():
    aliases = resolve_aliases(
        [
            _evidence("cty cá", "company-a", "explicit_definition", 1.0),
            _evidence("cty cá", "company-b", "explicit_definition", 0.9),
        ]
    )
    a = aliases[0]
    assert a.is_ambiguous is True
    assert a.best_company_key is None  # gap 0.05 < AMBIGUITY_GAP


def test_weak_evidence_is_unresolved():
    aliases = resolve_aliases([_evidence("x", "company-a", "co_occurrence", 0.5)])
    a = aliases[0]
    assert a.is_unresolved is True
    assert a.best_company_key is None


def test_contradiction_lowers_score():
    aliases = resolve_aliases(
        [
            _evidence("y", "company-a", "explicit_definition", 1.0),
            _evidence("y", "company-a", "contradiction", 1.0),
        ]
    )
    # raw = 1.0 - 0.5 = 0.5 → score 0.25 → unresolved
    assert aliases[0].resolutions[0]["confidence"] == 0.25
    assert aliases[0].is_unresolved is True


# ── assign_mention_resolutions ────────────────────────────────────────────────


def test_clue_resolves_via_alias_over_self_name():
    aliases = resolve_aliases([_evidence("nhà F", "fpt", "explicit_definition", 1.0)])
    mention = {
        "_key": "m1",
        "company_name": "FPT Software",  # would slug to fpt-software
        "identification_clues": ["nhà F"],
    }
    [patch] = assign_mention_resolutions([mention], aliases)
    assert patch["company_key"] == "fpt"  # alias wins over self-name slug
    assert patch["resolution_version"] == RESOLUTION_VERSION


def test_falls_back_to_self_name_when_no_alias_match():
    mention = {
        "_key": "m2",
        "company_name": "Kyber Network",
        "identification_clues": ["unknown slang"],
    }
    [patch] = assign_mention_resolutions([mention], [])
    assert patch["company_key"] == "kyber-network"
    assert patch["is_ambiguous"] is False


def test_ambiguous_alias_is_not_used_as_best():
    aliases = resolve_aliases(
        [
            _evidence("cty cá", "company-a", "explicit_definition", 1.0),
            _evidence("cty cá", "company-b", "explicit_definition", 0.9),
        ]
    )
    mention = {"_key": "m3", "company_name": "Fallback Co", "identification_clues": ["cty cá"]}
    [patch] = assign_mention_resolutions([mention], aliases)
    # ambiguous alias has no best_company_key → fall back to self name
    assert patch["company_key"] == "fallback-co"


# ── build_company_stubs ───────────────────────────────────────────────────────


def test_canonical_name_is_most_frequent():
    mentions = [
        {"_key": "1", "company_name": "FPT", "company_type": "it"},
        {"_key": "2", "company_name": "FPT", "company_type": "it"},
        {"_key": "3", "company_name": "Fpt", "company_type": "outsourcing"},
    ]
    stubs = build_company_stubs({"fpt"}, mentions, now="2024-01-01T00:00:00")
    assert len(stubs) == 1
    assert stubs[0].canonical_name == "FPT"
    assert stubs[0].company_type == "it"
    assert stubs[0].is_stub is True


# ── resolve (end-to-end) ──────────────────────────────────────────────────────


def test_resolve_returns_all_three_outputs():
    evidence = [_evidence("nhà F", "fpt", "explicit_definition", 1.0)]
    mentions = [
        {"_key": "m1", "company_name": "FPT Software", "identification_clues": ["nhà F"]},
    ]
    aliases, companies, patches = resolve(evidence, mentions, now="2024-01-01T00:00:00")
    assert len(aliases) == 1
    assert {c.key for c in companies} == {"fpt"}  # referenced by the resolved mention
    assert patches[0]["company_key"] == "fpt"
