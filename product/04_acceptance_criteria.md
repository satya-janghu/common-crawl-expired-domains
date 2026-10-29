# Acceptance Criteria And Verification Status

The app is accepted when the product criteria below pass on a local developer machine and on the target workstation class. The status column records the current implementation state; it is not a substitute for running the validation suite.

Status values:

- `Implemented`: supported by code and local tests.
- `Partial`: implemented for local or fixture-scale use, with known gaps.
- `External validation required`: depends on live services, credentials, or current third-party behavior.
- `Future`: product goal not yet complete.

## Input and setup

| Criterion | Current status |
| --- | --- |
| User can provide 1-10 source domains. | Implemented |
| App normalizes URLs/domains and removes duplicates. | Implemented |
| App rejects invalid domains before starting. | Implemented |
| User can select multiple crawl months or crawl IDs. | Implemented |
| App resolves selected months to crawl IDs. | Implemented |
| App resolves graph releases automatically from selected months/crawl IDs. | Implemented for configured releases and Common Crawl `graphinfo.json` domain releases; live `CC-MAIN-2026-17` validation resolved `cc-main-2026-feb-mar-apr`. |
| User can select Web Graph, WAT, and/or WARC modes. | Implemented |
| At least one mode is required. | Implemented |

## Run execution

| Criterion | Current status |
| --- | --- |
| App creates per-run SQLite DB before processing begins. | Implemented |
| App creates or opens global domain cache SQLite DB. | Implemented |
| `edf find` runs the normal workflow as a single command with auto-initialization, foreground progress, default export, and result preview. | Implemented |
| `edf run start` executes in the foreground by default. | Implemented |
| A non-executing run creation path is explicit and not reported as active work. | Implemented through `--create-only`, which creates status `created`. |
| App can start, pause, resume, and stop a run. | Implemented for the foreground execution model. |
| App continues writing checkpoints during execution. | Implemented for task state, CDX page state, artifacts, candidates, observation batches, and events; WAT sidecars are batch-idempotent rather than byte-offset resumable. |
| App survives transient network errors through retries/backoff. | Implemented; timeouts, retryable HTTP failures, and `Retry-After` are covered by tests, and live Common Crawl connection refusals are documented as resumable failures. |
| App uses bounded workers and avoids unbounded in-memory task queues. | Implemented for current workflows. |

## Common Crawl processing

| Criterion | Current status |
| --- | --- |
| CDX discovery returns and stores relevant records. | Implemented with pagination, persisted page state, client-side filters, dedupe, and explicit budget failure. |
| WARC mode fetches byte ranges and extracts links. | Implemented with range-size and decoded-body limits plus final failures for malformed/truncated/oversized records. |
| WAT mode scans relevant metadata and extracts links where available. | Implemented with bounded streaming, malformed-line tolerance, stable batch commits, and live sidecar validation against `CC-MAIN-2026-17`. |
| Web Graph mode streams graph files and extracts domain-level outlinks. | Implemented for configured or `graphinfo.json` domain releases, with reverse-domain vertex parsing, malformed-line tolerance, object-cap safety, and retry idempotency; full live graph artifact creation is resource-blocked by current production graph size unless explicitly approved. |
| Results from all modes merge into one candidate domain pipeline. | Implemented |

## Domain processing

| Criterion | Current status |
| --- | --- |
| App uses PSL-based registrable domain extraction. | Implemented |
| App dedupes source domains and candidate domains within a run. | Implemented |
| App dedupes against global cache. | Implemented |
| App processes all PSL-valid registrable candidate domains by default. | Implemented, subject to policy and provider support classification. |
| App ignores configured blocked domains/TLDs/suffixes. | Implemented |
| App classifies non-retail or eligibility-restricted TLDs separately. | Implemented |
| App stores evidence for candidates. | Implemented, with artifact-backed observations and compact candidate summaries. |

## Availability checking

| Criterion | Current status |
| --- | --- |
| DNS checks run with bounded concurrency. | Implemented |
| RDAP checks use TLD bootstrap endpoints. | Implemented |
| Registrar provider checks work when API credentials are configured. | Implemented with live Porkbun validation and mock coverage. |
| Porkbun provider checks work when API credentials are configured. | Implemented with live validation for credential success, standard unavailable/available responses, and JSON rate limits; mock coverage includes success, premium, API error, malformed response, retry, typed retry windows, and rate-limit paths. |
| Registrar checks avoid unnecessary provider calls when DNS/RDAP evidence is sufficient. | Implemented; default `registrarCheckStatuses` is `["available_likely"]`, so registered, restricted, reserved, unsupported, pending-delete, redemption, error, and ambiguous candidates do not spend Porkbun quota. |
| Registrar rate limits are resumable across long runs. | Implemented; provider retry windows persist in `availability_tasks.next_attempt_at`, deferred tasks are excluded from leasing until due, and tests cover defer/resume behavior. |
| Classifier distinguishes available, likely available, registered, pending delete, reserved, restricted, premium, ambiguous, unsupported, rate-limited, ignored, and error states. | Implemented |
| Cache TTLs are applied by status. | Implemented |

## Progress and ETA

| Criterion | Current status |
| --- | --- |
| CLI/watch shows overall progress. | Implemented |
| CLI/watch shows Discovery, Extraction, Domain Checks, and Export rows. | Implemented |
| ETA is recalculated every 10 seconds. | Implemented |
| ETA uses recent measured throughput, not only a static pre-run estimate. | Implemented |
| Cache hit count updates during the run. | Implemented |
| Crash recovery status is visible. | Implemented |
| Rich interactive Bubble Tea-style TUI controls are available. | Future; current watch mode is lightweight and line-oriented. |
| Optional desktop GUI shows intuitive crawl progress and candidate insights. | Implemented for one-source WARC+WAT runs through the Fyne GUI. It displays a run-control rail, operations monitor, KPI cards, stage monitor, candidate intelligence, alerts, and export path. |
| Optional desktop GUI remains backed by persisted run state. | Implemented; `internal/guiapp` reads SQLite snapshots/results and uses `MonitorView` for display facts. |

## Crash recovery

| Criterion | Current status |
| --- | --- |
| Killing the app mid-run does not corrupt DBs. | Implemented for SQLite-backed state and artifact temp files. |
| Restarting app shows persisted runs. | Implemented |
| Expired task leases return to pending. | Implemented |
| Completed task observation batches are not counted twice on retry. | Implemented through stable `observation_batches` keys. |
| Temporary artifact files are cleaned or ignored. | Implemented |
| Final candidate results remain deduped. | Implemented |

## CLI and exports

| Criterion | Current status |
| --- | --- |
| `edf` builds as a local CLI binary. | Implemented |
| Optional `edf-gui` builds as a local GUI binary. | Implemented; builds as a standalone `edf-gui` binary on Linux with cgo. |
| `edf find`, `edf init`, `edf config show`, `edf run start`, `edf run status`, `edf run watch`, `edf run pause`, `edf run resume`, `edf run stop`, `edf results`, and `edf export` exist. | Implemented |
| Non-interactive output avoids terminal control codes. | Implemented |
| Status and results commands support machine-readable JSON output. | Implemented |
| Recoverable runs are discoverable after process restart. | Implemented |
| User can export CSV. | Implemented |
| User can export JSON. | Implemented |
| User can export Parquet. | Implemented |
| Exports include evidence and status details. | Implemented |
| Exports include a manual-verification-required note. | Implemented |

## Production readiness

| Criterion | Current status |
| --- | --- |
| A bounded live smoke-run procedure exists. | Implemented in `docs/smoke-run.md` |
| The app has a canonical final live test method. | Implemented in `docs/smoke-run.md` as the single-source `example.org` / `CC-MAIN-2024-10` WARC+WAT `edf find` command. |
| The app is declared ready for unattended unbounded production runs. | Partial with caveats: unbounded means uncapped within selected source/crawl/mode scope, not global Common Crawl processing. Operators must use TUI/watch monitoring, resume workflow, and completeness checks; byte ceilings must remain positive, so no-practical-size-limit runs use intentionally high byte values. |
| Live Common Crawl WARC/WAT/Web Graph validation is documented. | Implemented; WARC and WAT live runs plus graph metadata/object-size/object-cap behavior are documented in `docs/smoke-run.md`. |
| Live Porkbun registrar validation is documented. | Implemented in `docs/smoke-run.md`; live rate-limit responses were observed and deferred pacing is tested. |
| A multi-source (10-domain) pilot profile is documented. | Implemented as the pilot profile in `docs/smoke-run.md`; operators start there and use the resume workflow before scaling. |
| Optional GUI validation is documented. | Implemented in `docs/smoke-run.md` through the headless `edf-gui --run-once` path. |
