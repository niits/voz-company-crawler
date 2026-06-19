"""Global company-resolution tier (cross-thread, unpartitioned).

Unlike the per-thread reply-graph assets, resolution must see every thread's
alias evidence at once — an alias defined in one thread resolves mentions in all
others. So this is a single asset, built once (not by the per-thread factory),
and triggered on a schedule rather than by AutomationCondition.eager().
"""

from datetime import datetime, timezone

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    DefaultScheduleStatus,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from voz_crawler.core.entities.enrichment import RESOLUTION_VERSION
from voz_crawler.core.graph.company_resolution import resolve
from voz_crawler.defs.resources.arango_resource import ArangoDBResource

RESOLUTION_GROUP = "company_resolution"


@asset(
    name="resolve_companies",
    group_name=RESOLUTION_GROUP,
    compute_kind="ArangoDB",
    description=(
        "Cross-thread alias resolution: aggregate alias_evidence into company_aliases,"
        " stub companies, and back-patch company_mention.company_key."
    ),
)
def resolve_companies(
    context: AssetExecutionContext,
    arango: ArangoDBResource,
) -> MaterializeResult:
    repo = arango.get_repository()

    evidence = repo.fetch_all_alias_evidence()
    mentions = repo.fetch_all_company_mentions()
    context.log.info(f"[resolve_companies] evidence={len(evidence)} mentions={len(mentions)}")

    now = datetime.now(timezone.utc).isoformat()
    aliases, companies, mention_patches = resolve(evidence, mentions, now)

    repo.upsert_company_aliases(aliases)
    repo.upsert_companies(companies)
    repo.patch_mention_resolutions(mention_patches)

    resolved = sum(1 for p in mention_patches if p["company_key"])
    ambiguous_aliases = sum(1 for a in aliases if a.is_ambiguous)
    context.log.info(
        f"[resolve_companies] aliases={len(aliases)} companies={len(companies)}"
        f" mentions_resolved={resolved}/{len(mention_patches)}"
    )
    return MaterializeResult(
        metadata={
            "alias_docs": MetadataValue.int(len(aliases)),
            "ambiguous_aliases": MetadataValue.int(ambiguous_aliases),
            "company_stubs": MetadataValue.int(len(companies)),
            "mentions_total": MetadataValue.int(len(mention_patches)),
            "mentions_resolved": MetadataValue.int(resolved),
            "resolution_version": MetadataValue.int(RESOLUTION_VERSION),
        }
    )


def build_resolution_pipeline() -> tuple:
    """Returns (asset, job, schedule) for the global resolution tier.

    Built once and registered outside the per-thread factory loop.
    """
    resolve_job = define_asset_job(
        name="resolve_companies_job",
        selection=AssetSelection.assets(resolve_companies),
        description="Aggregate alias evidence into canonical companies across all threads.",
    )
    resolve_schedule = ScheduleDefinition(
        name="resolve_companies_schedule",
        job=resolve_job,
        cron_schedule="0 * * * *",  # hourly
        default_status=DefaultScheduleStatus.RUNNING,
    )
    return resolve_companies, resolve_job, resolve_schedule
