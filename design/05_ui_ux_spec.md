# CLI/TUI UX Specification

## Design direction

Expired Domain Finder is CLI-first. It must work well in scripts, terminals, remote sessions, and long-running jobs. The optional TUI is for live progress, logs, activity, and controls; it must remain lightweight and must not become the source of truth.

The optional Fyne GUI is a local monitoring console for users who prefer a desktop view during single-domain WARC+WAT validation and review. It follows the [GUI design system](07_gui_design_system.md): Precision & Density, cool neutral foundation, borders-only depth, compact spacing, and status-first copy. The GUI must not replace CLI automation or SQLite as source of truth.

## CLI command groups

```text
edf init
edf config show
edf run start
edf run status
edf run watch
edf run pause
edf run resume
edf run stop
edf results
edf export
```

## Run start

Required inputs:

- 1–10 `--source` values
- one or more `--crawl` values
- one or more `--mode` values: `web_graph`, `wat`, `warc`

Behavior:

- Validate inputs before creating work.
- Persist run config before workers start.
- Print run ID.
- Execute the selected workflow in the foreground by default.
- Use `--create-only` when the user wants to create durable run state without starting workers.
- Created runs must be shown as `created`, not `running`.
- Use the live watch/TUI view for monitored production runs; `--no-tui` is only for deliberately scriptable/non-interactive runs.
- Print line-oriented progress every 10 seconds in non-interactive output.

## Status output

Show only:

- Run ID
- Status
- Overall progress
- ETA
- Four stage rows:
  - Discovery
  - Extraction
  - Domain Checks
  - Export
- Elapsed time
- Cache hits
- Crash recovery state
- Deferred registrar checks, when present, with earliest retry time

## TUI layout

```text
Expired Domain Finder

Run summary
  Status | Elapsed | ETA | Cache hits | Recovery | Deferred checks

Stages
  Discovery      [########--] 80%
  Extraction     [#####-----] 50%
  Domain Checks  [###-------] 30%
  Export         [----------] 0%

Activity / Logs
  compact recent events, newest visible

Candidate Preview
  Domain | Status | Confidence | Source Pages

Controls
  p pause/resume | s stop | q quit
```

## TUI behavior

- Current implementation uses a lightweight line-oriented watch view as the supported production monitor. A richer Bubble Tea-style TUI with full-screen controls remains a future enhancement.
- Poll or subscribe to persisted progress/event state.
- Redraw without blocking workers.
- Keep logs compact; detailed logs remain in artifacts.
- Show actionable errors.
- Show registrar rate limits as deferred checks with the next retry time, not as fatal errors.
- Never show secrets.
- Quit exits the viewer without changing run state unless the user explicitly pauses/stops it.

## Results output

Default columns:

- Domain
- Status
- Confidence
- Source Pages
- Modes

Detailed output may include:

- TLD
- Score
- Evidence mode
- Last checked
- Registrar status
- Deferred retry time when availability is waiting on provider quota
- Compact evidence samples

## Settings

Required settings:

- Data directory
- Maximum WARC concurrency
- Maximum DNS concurrency
- Maximum RDAP concurrency
- Registrar provider and API key reference
- TLD/domain block lists
- Restricted/non-retail TLD policy display
- Cache TTL overrides
- Common Crawl mirror/source selection if later needed

## Error states

Errors should be actionable:

- `Common Crawl index returned 503. Retrying with backoff.`
- `RDAP provider for .ai is unavailable. Domain marked ambiguous.`
- `Registrar API key missing. Availability confidence will be lower.`
- `Registrar rate limit reached. Availability check deferred until <timestamp>.`

## Optional GUI layout

Current Fyne GUI structure:

```text
Run Control Rail        Operations Monitor
  Domain                  Headline, progress, ETA, elapsed
  Year / Month            KPI cards: progress, ETA, elapsed, cache hits,
  Start / Cancel / Resume candidates, export-ready count
  Run details
  Export path           Stage Monitor | Candidate Intelligence
                          Compact stage rows with state/rate/ETA
                          Top candidates with status/confidence/score

                        Alerts
```

GUI behavior:

- Accept one source domain and one year/month selector.
- Resolve the selected month to the closest older-or-exact crawl ID.
- Run WARC+WAT through the shared Go backend and SQLite run state.
- Poll persisted snapshots and candidate rows; do not infer progress from widget state.
- Show candidate mix, export-ready count, best confidence, best evidence mode, and top candidates during the run.
- Show alerts for failed units, recovered state, paused/recoverable state, and warnings.
- Write GUI exports to `~/Documents`.
- Keep `--run-once` available for automated validation of the GUI execution path.
- Never show secret values; only environment variable names may appear in setup guidance.
