# Epics and User Stories

## Epic 1 — CLI/TUI App Shell and Local Project Structure

### Story 1.1 — Initialize Go CLI/TUI app

As a user, I want a local command-line app with optional live TUI monitoring and an optional desktop monitor so I can run the workflow without a cloud backend.

Acceptance criteria:

- `edf` binary builds and prints help.
- CLI exposes the primary `find` workflow plus init, config, run, results, and export command groups.
- TUI can watch a run and render progress/log snapshots from persisted state.
- Optional GUI can monitor supported single-domain WARC+WAT runs from the same persisted state.
- All core workflows remain usable in plain non-interactive CLI mode.

### Story 1.2 — App settings persistence

As a user, I want settings to persist between sessions.

Acceptance criteria:

- Settings file stores data directory, concurrency limits, environment-variable credential references, selected TLD policy, and display preferences.
- Default TLD policy checks all PSL-valid registrable domains except blocked or non-retail/restricted suffixes.
- Secrets are not printed in logs, config output, exports, or TUI views.
- Invalid config shows actionable error.

## Epic 2 — Source Domain and Crawl Selection

### Story 2.1 — Validate source domains

As a user, I want to pass source domains quickly.

Acceptance criteria:

- Input allows up to 10 `--source` values.
- URLs are normalized to source domains.
- Duplicates are removed.
- Invalid inputs are rejected before a run starts.
- Source domains are stored in run_state DB.

### Story 2.2 — Crawl catalog loader

As a user, I want to select crawl months without knowing Common Crawl IDs.

Acceptance criteria:

- App loads crawl index metadata from Common Crawl.
- Month/year selections map to crawl IDs.
- Graph releases are mapped separately when needed.
- App stores resolved crawl IDs/releases with the run.

## Epic 3 — Run Orchestration and Task Queue

### Story 3.1 — Create a run

As a user, I want to start a run with selected domains/crawls/modes.

Acceptance criteria:

- New per-run SQLite DB is created.
- Run config is persisted before processing begins.
- Tasks are seeded for selected modes.
- CLI prints run ID and initial status.
- `find` creates storage/config as needed, executes the selected workflow in the foreground, writes a default export, and prints a compact result preview.
- `run start` remains available for advanced/recovery workflows and executes the selected workflow in the foreground by default.
- `run start --create-only` creates durable run state with status `created` and does not imply background workers.
- Long monitored runs use the watch/TUI view; `--no-tui` is available for deliberately scriptable or non-interactive runs.

### Story 3.2 — Pause, resume, stop

As a user, I want control over long jobs.

Acceptance criteria:

- Pause stops claiming new work but lets in-flight tasks finish or checkpoint.
- Resume restarts pending work.
- Stop marks run stopping/cancelled safely.
- CLI/TUI status reflects state accurately.

### Story 3.3 — Crash recovery

As a user, I want to resume after a crash.

Acceptance criteria:

- Expired leased tasks return to pending on startup.
- Completed tasks remain completed.
- Unsealed temporary artifacts are ignored or deleted.
- Run can continue without manual repair.
- Retried observation tasks do not duplicate candidate counts or progress counters.

## Epic 4 — Common Crawl CDX Discovery

### Story 4.1 — Query CDXJ index

As the system, I need to discover page captures for source domains.

Acceptance criteria:

- Queries selected crawl IDs for each source domain.
- Stores CDX records with URL, timestamp, mime, status, digest, filename, offset, length.
- Filters to HTML-like successful responses where practical.
- Retries transient errors.

### Story 4.2 — CDX deduplication

As the system, I need to avoid redundant processing.

Acceptance criteria:

- Dedupes by crawl_id + urlkey + timestamp.
- Dedupes identical content by digest where safe.
- Dedupes byte-range fetch tasks by filename + offset + length.

## Epic 5 — WARC Range Extraction Mode

### Story 5.1 — Fetch WARC records by byte range

As the system, I need to fetch only relevant WARC records.

Acceptance criteria:

- Uses CDX filename/offset/length.
- Sends HTTP Range requests to Common Crawl data host.
- Handles gzip record decoding.
- Stores task result and retry state.

### Story 5.2 — Extract outbound links from HTML

As the system, I need outbound URLs from source pages.

Acceptance criteria:

- Parses links from `a[href]` primarily.
- Optionally records `link`, `script`, `img` only if configured.
- Resolves relative URLs against source URL.
- Captures anchor text, rel attributes, and source metadata.
- Writes large link observations to Parquet/ZSTD artifact files.

## Epic 6 — WAT Metadata Extraction Mode

### Story 6.1 — Resolve WAT sidecar files

As the system, I need to use WAT metadata where useful.

Acceptance criteria:

- Resolves WAT sidecar paths from selected crawl data.
- Streams WAT records without loading whole files.
- Emits normalized link observations.

## Epic 7 — Web Graph Mode

### Story 7.1 — Stream graph files

As the system, I need broad discovery from Common Crawl web graph data.

Acceptance criteria:

- Maps selected crawls/months to graph releases where available.
- Streams graph files and filters source-domain outlinks.
- Stores filtered graph candidate artifacts only by default.
- Marks graph-only observations as lower-confidence discovery evidence.

## Epic 8 — Normalization and Candidate Deduplication

### Story 8.1 — Normalize URLs and domains

As the system, I need reliable registrable-domain extraction.

Acceptance criteria:

- Uses the Public Suffix List.
- Handles IDNA/punycode.
- Does not split domains by last two labels.
- Applies ignore rules and TLD policy categories.

## Epic 9 — Availability Checker

### Story 9.1 — Check availability with multiple signals

As the system, I need robust availability classification.

Acceptance criteria:

- DNS checks are bounded and never alone mark registrar-confirmed availability.
- RDAP checks use bootstrap/provider data.
- Porkbun provider checks work when credentials are configured.
- Porkbun checks are gated by configured pre-registrar statuses and default to `available_likely` candidates only.
- Porkbun rate-limit responses defer the availability task using the provider retry window.
- Results are cached with status-specific TTLs.
- Restricted, reserved, premium, unsupported, ambiguous, and error states remain distinct.

## Epic 10 — Storage, Recovery, and Artifacts

### Story 10.1 — SQLite operational state

As the system, I need durable run and cache state.

Acceptance criteria:

- Uses per-run `run_state.sqlite`.
- Uses global `domain_cache.sqlite`.
- Applies WAL PRAGMAs on connection.
- Uses batched writes or dedicated writer goroutines.
- Stores migration versions.

### Story 10.2 — Artifact preservation

As the system, I need durable large evidence artifacts.

Acceptance criteria:

- Writes Parquet/ZSTD temp files before atomic rename.
- Manifests sealed artifacts in SQLite.
- Commits artifact manifest rows and observation batch markers atomically with candidate updates.
- Removes or ignores temp artifacts on recovery.

## Epic 11 — Progress, ETA, CLI, and TUI

### Story 11.1 — Non-interactive CLI status

As a user, I want scriptable status output.

Acceptance criteria:

- `edf run status <run-id>` prints status, elapsed time, ETA, cache hits, and stage rows.
- `run start` prints line-oriented progress every 10 seconds when not attached to an interactive terminal.
- Output can be consumed in logs without terminal control codes.

### Story 11.2 — Lightweight TUI

As a user, I want to watch live activity without a heavy GUI.

Acceptance criteria:

- `edf run watch <run-id>` opens the lightweight watch/TUI view.
- The watch/TUI view shows run summary, stage progress, live event/activity log, and compact candidate preview.
- The watch/TUI view supports pause/resume/stop/quit controls.
- Watch/TUI state is derived from SQLite and event streams, not independent in-memory assumptions.

### Story 11.3 — Optional GUI monitoring console

As a user testing a crawl locally, I want a desktop monitoring console that makes progress and candidate quality easier to understand.

Acceptance criteria:

- The GUI starts a supported one-source WARC+WAT run from a domain plus year/month selector.
- The GUI shows a run-control rail, operations monitor, stage monitor, candidate intelligence panel, alerts, and export path.
- GUI progress and candidate insights are derived from SQLite snapshots and result rows.
- The GUI builds as a standalone `edf-gui` binary and can be validated headlessly with `--run-once`.
- The GUI must not display credential values.

## Epic 12 — Results and Exports

### Story 12.1 — Candidate results

As a user, I want to inspect candidate domains.

Acceptance criteria:

- `edf results <run-id>` prints compact candidate rows.
- Details include status, confidence, source page count, modes, and evidence samples.
- Large evidence remains in artifacts.

### Story 12.2 — Exports

As a user, I want portable outputs.

Acceptance criteria:

- Exports CSV, JSON, and Parquet.
- Exports include evidence and status details.
- Exports include a manual-verification-required note.
