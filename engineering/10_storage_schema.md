# Storage and Schema Design

## Storage overview

```text
app_data/
  cache/
    domain_cache.sqlite
  runs/
    run_<timestamp>_<slug>/
      run_state.sqlite
      artifacts/
        link_observations/*.parquet
        graph_candidates/*.parquet
        exports/*
      logs/
        events.jsonl.zst
```

## Operational truth

SQLite is the operational source of truth for:

- Run state
- Task state
- Task leases
- Candidate domain table
- Domain availability cache
- Artifact manifests
- Observation batch idempotency markers
- CLI/TUI progress counters

Parquet is for immutable bulk artifacts and exports only.

## SQLite settings

At connection initialization:

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=30000;
```

SQLite writes are serialized through short, explicit write sections and batched transactions where the workflow ingests many records. This preserves SQLite's single-writer behavior while network and parsing work remains concurrent.

## Run DB

See `schemas/run_state_schema.sql`.

Key ideas:

- `runs` stores immutable run config and current run status.
- Task tables store explicit state, attempts, and lease timestamps.
- `cdx_discovery_pages` stores per-source/crawl/pattern/page discovery state so CDX pagination resumes without refetching completed pages.
- `candidate_domains` stores current candidate summary for CLI/TUI output.
- `candidate_source_pages` records unique WAT/WARC source pages so `source_page_count` is unique source-page evidence, while `outbound_url_count` remains total outbound observations.
- `artifact_files` records sealed Parquet/log/export files.
- `observation_batches` records stable task batch keys for idempotent link-observation ingestion.
- `availability_tasks.next_attempt_at` stores provider retry windows for deferred registrar checks; task leasing excludes rows whose retry time is still in the future.
- `events` stores compact operational events with enough severity/component context for CLI/TUI display.

## Domain cache DB

See `schemas/domain_cache_schema.sql`.

Key ideas:

- `domains` has one row per registrable domain.
- `availability_checks` stores historical check attempts.
- `dns_cache`, `rdap_cache`, and `registrar_cache` store method-specific details.
- `tld_support` stores support metadata, policy category, provider behavior, and last policy refresh.
- `ignore_rules` stores user/system exclusions.
- Broad TLD coverage is the default; blocked and restricted policies are stored as data, not hardcoded only in the classifier.

## Artifact safety

Write bulk artifacts as:

```text
part-000001.parquet.tmp
```

Then:

1. Finish writing.
2. Flush and close.
3. Compute hash and row count.
4. Atomic rename to `.parquet`.
5. Commit the artifact manifest, observation batch marker, candidate updates, progress increments, and availability task seeding in one SQLite transaction.

Observation batches use a stable key derived from the run/task identity. If a task is retried after an artifact was produced or after candidate rows were updated, the batch marker makes the retry a no-op for candidate counts and progress counters instead of duplicating observations.

On restart:

- Delete `.tmp` files.
- Keep manifested completed files.
- Ignore unmanifested completed files unless repair mode is run.
- Retry tasks whose batch marker is missing so evidence artifacts and candidate summaries can be made consistent again.

## Export safety

Exports must include enough context to prevent weak signals from being treated as a purchase recommendation:

- availability status
- confidence
- TLD policy category
- evidence level/modes
- registrar provider and price/premium summary where available
- manual verification required note

## Event Logs

SQLite `events` rows are the operational source for recent activity. When a run reaches `completed`, `cancelled`, or `failed`, the event stream is sealed to:

```text
runs/<run-id>/logs/events.jsonl.zst
```

The sealed log is recorded in `artifact_files` with `artifact_type='event_log'`, `format='jsonl'`, and `compression='zstd'`.

## Migrations

The embedded migration runner applies versioned schema payloads and records them in `schema_migrations`. Version 1 maps to:

```text
migrations/run/0001_initial.sql
migrations/cache/0001_initial.sql
```

Schema version is stored in `schema_migrations`.

## Event display requirements

Events are the durable source for recent activity shown by the TUI and status commands. Each event should identify:

- event type
- severity
- component
- short message
- optional structured payload
- creation time

Status and TUI queries should page recent events by `(run_id, created_at, id)` and avoid loading the full event history into memory.
