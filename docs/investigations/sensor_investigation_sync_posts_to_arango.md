# Investigation: `sync_posts_to_arango` automation not running

Date: 2026-04-03

Summary
-------
- Symptom: `sync_posts_to_arango` (partitioned by `voz_pages`) was not auto-run by Dagster's default automation sensor despite upstream partitions being present and materialized.
- Root cause: the automation evaluation used a `SINCE` window (`reset_timestamp`) later than the timestamps of the upstream materializations. Automation only considers *new* materializations after that `reset_timestamp`, so earlier upstream materializations were ignored.

This document is an expanded investigation log: it contains the step-by-step chronology, commands run, returned outputs (key excerpts), and detours/failed attempts encountered while debugging.

Environment
-----------
- Repo: voz-company-crawler
- Dagster instance DB: PostgreSQL (container `postgres` in `docker/dev/docker-compose.yml`)
- Assets inspected: `dlt_voz_posts` (upstream), `sync_posts_to_arango` (downstream)
- Partitions definition: `voz_pages` (DynamicPartitionsDefinition)

Files inspected
---------------
- `voz_crawler/defs/assets/ingestion.py` — defines `voz_page_posts_assets` and `voz_pages_partitions` (DLT asset)
- `voz_crawler/defs/assets/reply_graph.py` — defines `sync_posts_to_arango`, `extract_explicit_edges`, `compute_embeddings` with `AutomationCondition.eager()` and `AssetDep` using `IdentityPartitionMapping()`
- `voz_crawler/defs/sensors/ingestion.py` — run_status_sensor that submits crawl runs

Actions performed (commands and queries)
--------------------------------------
All commands run inside project (using the `docker/dev/docker-compose.yml` compose file). Replace paths/flags if you run locally.

1) Listed DB tables and inspected dynamic partitions:

```bash
docker compose -f docker/dev/docker-compose.yml exec -T postgres bash -lc \
  "psql -U <PG_USER> -d <PG_DB> -c \"\d+ dynamic_partitions\""

docker compose -f docker/dev/docker-compose.yml exec -T postgres bash -lc \
  "psql -U <PG_USER> -d <PG_DB> -c \"SELECT count(*) FROM dynamic_partitions WHERE partitions_def_name='voz_pages';\" -A -F '|||'"
```

2) Queried partitions (examples shown):

```bash
psql -c "SELECT partition FROM dynamic_partitions WHERE partitions_def_name='voz_pages' ORDER BY partition LIMIT 50;"
psql -c "SELECT partition FROM dynamic_partitions WHERE partitions_def_name='voz_pages' ORDER BY partition DESC LIMIT 50;"
```

Result: 521 partitions registered, example range `<PARTITION_KEY_START>` → `<PARTITION_KEY_END>`.

Chronology & detours (detailed)
-------------------------------

1) Inspect dynamic partitions

- Command:

```bash
psql -U <PG_USER> -d <PG_DB> -c "SELECT count(*) FROM dynamic_partitions WHERE partitions_def_name='voz_pages';"
```

- Output (excerpt):

```
count
521
(1 row)
```

Detour: none.

2) Confirm partition keys exist and sample them

- Commands:

```bash
psql -U <PG_USER> -d <PG_DB> -c "SELECT partition FROM dynamic_partitions WHERE partitions_def_name='voz_pages' ORDER BY partition LIMIT 50;"
psql -U <PG_USER> -d <PG_DB> -c "SELECT partition FROM dynamic_partitions WHERE partitions_def_name='voz_pages' ORDER BY partition DESC LIMIT 50;"
```

# Output (head / tail excerpts):

```
<PARTITION_KEY>
<PARTITION_KEY>
... (many keys) ...
<PARTITION_KEY_SAMPLE>

<PARTITION_KEY_END>
<PARTITION_KEY_END-1>
<PARTITION_KEY_END-2>
... (many keys) ...
<PARTITION_KEY_SAMPLE_2>
```

Detour: none.

3) Inspect registered asset keys

- Command:

```bash
psql -U <PG_USER> -d <PG_DB> -c "SELECT asset_key, last_materialization_timestamp FROM asset_keys WHERE asset_key ILIKE '%voz%' ORDER BY asset_key;"
```

- Output (excerpt):

```
asset_key|||last_materialization_timestamp
["dlt_voz_posts"]|||<LAST_MATERIALIZATION_TIMESTAMP>
["voz_posts"]|||
```

Interpretation: `dlt_voz_posts` is registered and shows recent materialization timestamps; downstream `sync_posts_to_arango` is also present elsewhere in `asset_keys`.

Detour: I attempted to import Python object `voz_page_posts_assets` inside the `user_code` container to print `.keys`, but the container lacked `dlt` and the import failed:

```
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/app/voz_crawler/defs/assets/ingestion.py", line 1, in <module>
    import dlt
ModuleNotFoundError: No module named 'dlt'
```

This prevented using the in-memory decorated object to observe its `.keys` programmatically; instead we relied on DB `asset_keys` and `event_logs`.

4) Inspect recent materialization events for upstream asset (`dlt_voz_posts`)

- Commands:

```bash
psql -U <PG_USER> -d <PG_DB> -c \
  "SELECT asset_key, key, value, event_timestamp FROM asset_event_tags WHERE asset_key ILIKE '%dlt_voz_posts%' ORDER BY event_timestamp DESC LIMIT 50;"

psql -U <PG_USER> -d <PG_DB> -c \
  "SELECT asset_key, partition, dagster_event_type, timestamp FROM event_logs WHERE asset_key ILIKE '%dlt_voz_posts%' AND partition IS NOT NULL ORDER BY id DESC LIMIT 50;"
```

- Output (excerpt from `event_logs`):

```
["dlt_voz_posts"]|||<PARTITION_KEY>|||ASSET_MATERIALIZATION|||<TIMESTAMP>
["dlt_voz_posts"]|||<PARTITION_KEY>|||ASSET_MATERIALIZATION|||<TIMESTAMP>
["dlt_voz_posts"]|||<PARTITION_KEY>|||ASSET_MATERIALIZATION|||<TIMESTAMP>
... (many materializations between <TIME_RANGE>) ...
```

Interpretation: upstream partitions were materialized across ~13:17–13:43.

Detour: attempted to query `asset_event_tags` for a `partition_key` column — error because column doesn't exist. I therefore used `event_logs.partition` for per-partition materialization info instead.

5) Inspect auto-materialize evaluation body rows

- Action: extracted `asset_daemon_asset_evaluations` rows that mention `sync_posts_to_arango` (saved into a temporary file earlier). The evaluation bodies are serialized JSON containing the evaluated condition tree.

- Key extracted metadata from the evaluation body:
  - `reset_evaluation_id`: <EVALUATION_ID>
  - `reset_timestamp`: 1775223885.249286 → converted to human time `2026-04-03 13:44:45 UTC`
  - `trigger_evaluation_id`: <EVALUATION_ID> and `trigger_timestamp` equal to reset timestamp
  - Top-level condition label `eager` (AndAutomationCondition)
  - Final `run_ids` set is empty (no runs were chosen by evaluation)

Detour: none (JSON inspected via raw DB extraction and read_file).

6) Cross-compare times

- Upstream materializations: latest observed ~`2026-04-03 13:43:57` for a partition.
- Evaluation reset/trigger time: `2026-04-03 13:44:45 UTC` (later)

Inference: because evaluation reset time is later than upstream materializations, the SINCE clause inside `eager()` did not treat those materializations as "new" and therefore did not trigger downstream runs.

Other small detours and notes
---------------------------
- I attempted to run a Python snippet inside `user_code` to inspect `AutomationCondition.eager()` repr; the environment had Python but lacked `dlt`, causing import failure (see detour above). I therefore relied on the serialized evaluation JSON in the DB and the daemon logs previously collected.
- I checked `asset_keys` and `event_logs` to confirm the asset names and materialization timestamps. Asset keys present: `['dlt_voz_posts']`, `['voz_posts']`, and `['sync_posts_to_arango']`.
- I inspected `asset_daemon_asset_evaluations` and confirmed `run_ids` was empty for the evaluated tick.

3) Checked registered asset keys in `asset_keys`:

```bash
psql -U <PG_USER> -d <PG_DB> -c "SELECT asset_key, last_materialization_timestamp FROM asset_keys WHERE asset_key ILIKE '%voz%' ORDER BY asset_key;"
```

Result: `["dlt_voz_posts"]` present, and `["sync_posts_to_arango"]` present. Asset key names are aligned with code.

4) Looked at recent materialization events for the upstream asset:

```bash
psql -U <PG_USER> -d <PG_DB> -c \
  "SELECT asset_key, key, value, event_timestamp FROM asset_event_tags WHERE asset_key ILIKE '%dlt_voz_posts%' ORDER BY event_timestamp DESC LIMIT 50;"

psql -U <PG_USER> -d <PG_DB> -c \
  "SELECT asset_key, partition, dagster_event_type, timestamp FROM event_logs WHERE asset_key ILIKE '%dlt_voz_posts%' AND partition IS NOT NULL ORDER BY id DESC LIMIT 50;"
```

Result sample: many `ASSET_MATERIALIZATION` events for `['dlt_voz_posts']` with partitions like `<PARTITION_KEY>` at timestamp `<TIMESTAMP>` and similar.

5) Inspect auto-materialize evaluation rows (we extracted JSON evaluations earlier): the evaluation for the tick shows a `reset_timestamp`/`trigger_timestamp` equivalent to `2026-04-03 13:44:45 UTC`.

What the evaluation showed
-------------------------
- Top-level `AutomationCondition` used is `AutomationCondition.eager()` — an AND wrapper with these notable parts:
  - `in_latest_time_window` — matched (all 521 partitions present)
  - `SINCE` (a window operator) wrapping an OR of `(newly_missing) OR (any_deps_updated)` SINCE `handled` — this is the core trigger test
  - Guard checks: `NOT any_deps_missing`, `NOT any_deps_in_progress`, `NOT in_progress` — all evaluated true (no blockers)
- The trigger branches (`newly_missing`, `any_deps_updated`, `handled`) returned `0` partitions because the materializations they would consider as *new* occurred *before* the `reset_timestamp`.

Root cause (concise)
--------------------
- Automation evaluation was reset at ~`2026-04-03 13:44:45 UTC`.
- Upstream materializations for `dlt_voz_posts` occurred earlier (e.g. `2026-04-03 13:29:45`), therefore they are not considered *new* by the `SINCE` clause inside `eager()`.
- Because no partition appears as newly updated/missing/handled since the reset, the automation submitted 0 runs.

Why this can happen
-------------------
- Common causes:
  - Dagster daemon (or automation evaluation) restarted or reset after upstream materializations completed, updating `reset_timestamp` to now.
  - A manual reset of auto-materialization state.
  - A long-running maintenance window where materializations happened earlier than the last sensor tick reset.

Evidence
--------
- `event_logs` shows upstream `ASSET_MATERIALIZATION` timestamps at ~13:29–13:43 for many partitions.
- Evaluation metadata shows `reset_timestamp` ≈ `1775223885` → `2026-04-03 13:44:45 UTC` (later than those materializations).

Remediation options
-------------------
1. Re-run upstream partition(s) (recommended for small scope):
  - Re-run the DLT / ingestion for a single partition (e.g. `<PARTITION_KEY>`) so the new materialization has a timestamp > reset. Sensor will then detect and submit downstream for that partition.
   - How: use Dagit UI to run the crawl job for that partition or use `dg` / `uv run dg launch` (if configured). Example (Dagit): Jobs → `crawl_page_job` → launch with partition key.

2. Materialize downstream manually:
  - In Dagit: Assets → select `sync_posts_to_arango` → Materialize partition `<PARTITION_KEY>` (or batch list).

3. Adjust automation behavior (if you need the sensor to act on historic materializations):
  - Change automation logic (remove or alter the `SINCE` window) or reset the evaluation state so sensor re-considers older materializations. This requires code or DB-level changes and careful review.

4. Bulk re-trigger: if you want to re-run all 521 partitions, be cautious — this will queue many runs. You can script launches but consider rate limits/concurrency tags.

Next steps I can take for you
---------------------------
- A: Re-run a sample upstream partition now (tell me the partition key, e.g. `<PARTITION_KEY>`).
- B: Materialize `sync_posts_to_arango` for 1 or more partition keys (supply list or say `sample`).
- C: Provide commands and a short run-book to make the chosen remediation safely.

Detailed suggested run-book (safe steps)
--------------------------------------
1) Re-run a single upstream partition (safe, recommended for testing)

  - Use Dagit UI: Jobs → `crawl_page_job` or the specific job that materializes `voz_page_posts` → Launch with `partition_key=<PARTITION_KEY>`.

  - Or via `dg` CLI (if available/configured):

  ```bash
  uv run dg launch --job crawl_page_job --partition-key <PARTITION_KEY>
  ```

  - Expected result: new `ASSET_MATERIALIZATION` event for `dlt_voz_posts` with partition `<PARTITION_KEY>` and timestamp > current evaluation reset; on the next auto-materialize tick the sensor will detect this and should submit `sync_posts_to_arango` for the same partition.

2) Materialize downstream partition manually (if you need immediate result)

  - In Dagit: Assets → `sync_posts_to_arango` → Materialize → enter partition key `<PARTITION_KEY>`.

3) If you want to re-process all historical partitions:

  - Scripted bulk re-run is possible but will queue 521 runs — consider batching and using `dagster/concurrency_key` tags or increasing concurrency limits.


Appendix: notable queries used
----------------------------
- List dynamic partitions count:

```bash
psql -U <PG_USER> -d <PG_DB> -c "SELECT count(*) FROM dynamic_partitions WHERE partitions_def_name='voz_pages';"
```

- Show event_logs materializations for an asset:

```bash
psql -U <PG_USER> -d <PG_DB> -c "SELECT asset_key, partition, dagster_event_type, timestamp FROM event_logs WHERE asset_key ILIKE '%dlt_voz_posts%' AND partition IS NOT NULL ORDER BY id DESC LIMIT 50;"
```

---
