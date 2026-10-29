# Technical Architecture

## Architecture summary

```text
CLI commands / optional TUI / optional Fyne GUI
  ↓
Go App Core
  ↓
Run Orchestrator
  ├── Crawl Catalog Service
  ├── CDX Discovery Service
  ├── Web Graph Processor
  ├── WAT Processor
  ├── WARC Range Fetcher
  ├── Link Extractor
  ├── Domain Normalizer
  ├── Availability Checker
  ├── Progress/ETA Service
  ├── SQLite Writers
  └── Artifact Writers
```

## Runtime model

- The CLI creates, starts, pauses, resumes, stops, watches, exports, and inspects runs.
- `run start` persists run configuration before workers begin and executes the selected workflow in the foreground by default.
- `run start --create-only` creates durable run state without starting workers; created runs must not be reported as active background work.
- `run resume <run-id>` executes recoverable work in the foreground.
- Foreground runs can be bounded with smoke/pilot caps for CDX records, WARC/WAT/graph tasks, availability checks, runtime, and object byte sizes.
- Unbounded production runs remove CDX/task/availability/runtime caps inside the selected source/crawl/mode scope. They do not mean global Common Crawl processing. Byte ceilings remain positive safety settings and are set very high when no practical size limit is intended.
- If a task-count cap stops a pass while runnable tasks remain, the run is persisted as `paused` and can be resumed; it must not be reported as `completed`.
- Long monitored production runs should use the watch/TUI view; `--no-tui` is reserved for deliberately scriptable or non-interactive runs.
- The optional Fyne GUI is a desktop monitoring console for one-source WARC+WAT runs. It uses the same Go backend and persisted SQLite run state, and it is tested through `cmd/edf-gui --run-once`.
- If stdout is not interactive, commands print line-oriented progress suitable for logs.
- Commands that expose status or results support text output by default and JSON output for automation.
- The Go backend owns all work while the foreground command is running; persisted SQLite state is the handoff point across process restarts.
- Workers use contexts, goroutines, bounded worker pools, and bounded channels.
- SQLite writes go through dedicated writer goroutines or batched transaction boundaries.
- Progress snapshots are computed every 10 seconds and persisted for CLI/TUI rendering.

## Modules

### `commoncrawl`

Responsibilities:

- Fetch crawl catalog.
- Map month selections to crawl IDs.
- Query CDXJ index.
- Create WARC/WAT tasks.

### `graph`

Responsibilities:

- Fetch graph release metadata.
- Stream domain graph vertices/edges.
- Filter edges from selected source domains.
- Produce graph candidate observations.

### `extraction`

Responsibilities:

- Fetch WARC ranges.
- Scan WAT records.
- Parse HTML links.
- Emit link observations.

### `normalization`

Responsibilities:

- URL normalization.
- IDNA/punycode conversion.
- Public Suffix List registrable-domain extraction.
- Ignore rules and broad TLD policy classification.

### `availability`

Responsibilities:

- DNS checks.
- RDAP lookup.
- Registrar API integrations.
- Signal classification.
- Cache TTL calculation.
- TLD policy category lookup for restricted, reserved, unsupported, and open retail suffixes.
- Registrar-call gating from pre-registrar DNS/RDAP status.
- Provider retry-window extraction and deferred availability scheduling.

### `storage`

Responsibilities:

- Run DB connection and migrations.
- Cache DB connection and migrations.
- Task leasing.
- Batched writes.
- Artifact manifest.

### `progress`

Responsibilities:

- Stage counters.
- ETA calculation.
- Queue projection.
- CLI/TUI progress snapshots.

### `cli`

Responsibilities:

- Parse commands and flags.
- Validate user input before run creation.
- Print non-interactive status, results, and exports in stable text or JSON shapes.
- Redact secrets in config output.

### `tui`

Responsibilities:

- Render live run summary, progress, events, activity, and candidate preview.
- Provide pause/resume/stop/quit controls.
- Poll persisted state rather than owning task state.

### `guiapp`

Responsibilities:

- Provide the optional GUI engine for start, resume, cancel, snapshot, results, and export workflows.
- Resolve year/month crawl selections for GUI users.
- Build testable monitor view models from persisted progress snapshots and candidate rows.
- Keep GUI exports in user-facing locations such as `~/Documents` while preserving run state in SQLite.

### `cmd/edf-gui`

Responsibilities:

- Render the Fyne run-control rail, operations monitor, stage monitor, candidate intelligence panel, and alerts.
- Provide `--run-once` for automated validation of the GUI execution path.
- Keep the GUI optional; CLI commands remain the complete automation surface.

## CLI contracts

Global flags:

```text
--config <path>
--data-dir <path>
--output text|json
```

Rules:

- Text is the default for humans.
- JSON is stable enough for scripts and tests.
- Non-interactive output must not contain terminal control codes.
- `run start` must persist and print the run ID before starting workers.
- No CLI command may leave a run in `running` status after exiting unless another foreground worker process is actually still executing that run.
- `run status`, `results`, and `export` must work after process restart by reading SQLite state.

## Data flow

```text
Source domains + crawls + modes
  ↓
Run DB initialized
  ↓
Discovery tasks seeded
  ↓
CDX / Graph / WAT / WARC pipelines emit observations
  ↓
URL/domain normalization
  ↓
Candidate domain dedupe
  ↓
Global cache lookup
  ↓
Availability tasks for stale/new domains
  ↓
Candidate classification
  ↓
CLI/TUI results + exports
```

## Concurrency design

Use separate bounded queues:

```text
cdx_discovery_queue
graph_processing_queue
warc_fetch_queue
wat_scan_queue
link_observation_queue
domain_candidate_queue
availability_queue
sqlite_write_queue
artifact_write_queue
```

Recommended defaults:

| Queue | Default concurrency |
|---|---:|
| CDX discovery | 4 |
| WARC fetch | 48 |
| WAT scan | 8 |
| Graph scan | CPU/disk adaptive |
| DNS | 300 |
| RDAP | 20 total, per-TLD limited |
| Registrar | provider-specific |
| SQLite writer | 1 per DB |

These settings must be configurable.

## Idempotency

Every task must have a stable dedupe key.

| Task | Dedupe key |
|---|---|
| CDX query | run_id + source_domain + crawl_id |
| WARC fetch | crawl_id + filename + offset + length |
| WAT scan | crawl_id + wat_filename |
| Graph edge scan | graph_release_id + edge_file + source_domain_set_hash |
| DNS check | domain + record_type + cache_epoch |
| RDAP check | domain + provider + cache_epoch |
| Registrar check | domain + registrar + cache_epoch |

## Failure handling

- Transient network failure: retry with exponential backoff and jitter.
- HTTP 429/503: respect Retry-After where present.
- Registrar rate limit: persist `availability_tasks.next_attempt_at`, leave candidate availability unset, and resume when the retry window is due.
- Permanent parse failure: mark failed_final with error summary.
- Corrupt artifact temp file: delete on restart.
- SQLite lock timeout: retry internal writer transaction; do not spawn more writers.
- Observation ingestion must be idempotent through stable batch keys so task retries do not duplicate candidate counts or progress counters.
