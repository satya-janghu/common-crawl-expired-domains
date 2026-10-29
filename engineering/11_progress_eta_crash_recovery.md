# Progress, ETA, and Crash Recovery

## Progress stages

CLI/TUI output shows four high-level stages:

```text
Discovery
Extraction
Domain Checks
Export
```

Internally these are composed of many task types.

## Progress counters

Each stage tracks:

- completed_units
- total_known_units
- projected_remaining_units
- failed_units
- last_update_at
- recent_rate_units_per_second
- eta_seconds

## ETA recalculation

ETA recalculates every 10 seconds.

Use an exponential moving average:

```text
ema_rate = alpha * current_rate + (1 - alpha) * previous_ema_rate
```

Recommended alpha:

```text
0.35
```

Stage ETA:

```text
stage_eta = remaining_units / ema_rate
```

Overall ETA:

```text
overall_eta = sum(stage_etas)
```

The sum is a conservative estimate, because later stages are fed by earlier ones (code: `aggregateETA` in `internal/storage/storage.go`).

## Dynamic queue projection

Extraction discovers new domains, which creates new availability tasks.

Estimate future availability work:

```text
projected_new_domains =
  remaining_extraction_units
  × recent_new_domain_yield_per_unit
  × recent_cache_miss_rate
```

Then:

```text
availability_remaining =
  queued_unchecked_domains + projected_new_domains
```

Registrar-limited work is not considered runnable until its persisted `next_attempt_at` has passed. Status and inspection queries should show deferred availability tasks separately from immediately runnable work so multi-day runs can pause naturally while waiting for provider quota.

## ETA confidence

Low confidence when:

- Run has been active for under 3 minutes.
- Total known units are still changing quickly.
- Rate limits are active.
- More than 20% of recent tasks are retrying.

Medium confidence when:

- Rates are stable but queues still growing.

High confidence when:

- Queues are mostly fixed and rates stable.

## Crash recovery state machine

Task states:

```text
pending
leased
running
succeeded
failed_retryable
failed_final
cancelled
```

Task claiming:

```sql
UPDATE task_table
SET state = 'leased',
    lease_owner = ?,
    leased_until = ?,
    attempts = attempts + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = ?
  AND state IN ('pending', 'failed_retryable')
  AND attempts < max_attempts;
```

Startup recovery:

```text
leased/running tasks with leased_until < now → pending
failed_retryable with attempts < max_attempts → pending
.tmp artifacts → delete
active run statuses may be reported as recoverable until resumed or terminal
```

## Pause/resume

Pause:

- Set run status `pause_requested`.
- Stop claiming new work.
- Allow in-flight tasks to finish or checkpoint.
- CLI/TUI output shows paused when active workers reach zero.

Resume:

- Set run status `resume_requested`.
- Foreground `edf run resume <run-id>` changes a runnable created/resume-requested run to `running` before workers claim pending work.

Stop:

- Set run status `cancelling`.
- Stop claiming new work and cancel pending/leased/running tasks in the persisted task tables.
- Foreground execution exits after workers observe cancellation, then the run reaches `cancelled`.

Execution model:

- `edf run start` executes the selected workflow in the foreground by default.
- `edf run start --create-only` creates durable run state with status `created` and does not imply background workers.
- `edf run resume <run-id>` executes recoverable work in the foreground.
- If task-count smoke/pilot budgets are reached while work remains, foreground execution persists status `paused` and emits a budget-reached message. This is resumable progress, not successful completion.
- Unbounded production runs are uncapped inside the selected source/crawl/mode scope. CDX, task, availability, and runtime caps can be set to `0`; WARC/WAT/graph byte ceilings must remain positive and are set very high when no practical size limit is intended.
- There is no hidden in-memory supervisor after the command exits; status, watch, pause, resume, and stop operate through SQLite state.

## Event log

Events should be compact and queryable:

```text
run_started
crawl_catalog_loaded
cdx_query_completed
warc_task_completed
wat_task_completed
graph_chunk_completed
candidate_queued
cache_hit
availability_check_completed
availability_check_deferred
artifact_sealed
checkpoint_saved
run_completed
```

## Forensic diagnostics

Expected production failures are diagnosable from persisted state:

- `run_state.sqlite` stores run config, source/crawl/mode selections, task state, attempts, leases, and `last_error`.
- `errors` stores component, task table, task id, error message, and retryable flag for task failures.
- `events` stores compact lifecycle, recovery, ingestion, graph selection, export, and task-failure events.
- Terminal runs seal `events` into `logs/events.jsonl.zst` and record the log in `artifact_files`.
- `artifact_files` stores artifact path, format, compression, row count, byte count, and sha256.

Current diagnostic gaps:

- Panic stack traces are captured by the process manager (stdout/stderr) rather than stored in the run DB; persisting them to SQLite is a planned enhancement.
- Full HTTP attempt timelines are not persisted for every Common Crawl, RDAP, or registrar request.
- Full Common Crawl payloads and provider response bodies are intentionally not stored by default for size and privacy reasons.

Production process managers should capture stdout and stderr so panic stack traces and foreground command errors are retained outside the run DB.

## GUI progress model

The optional Fyne GUI does not own progress state. It reads:

- `progress.Snapshot` values recalculated from SQLite counters.
- Candidate rows from SQLite result queries.
- Completion blockers and terminal status from persisted run state.

`internal/guiapp.MonitorView` formats those persisted facts into headline, KPI, stage, candidate, alert, and action text for the desktop interface. This keeps the GUI consistent with CLI/watch output and preserves crash recovery semantics.
