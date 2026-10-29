# CLI/TUI Notes

`edf` is the primary interface. The TUI is an optional live viewer/controller for active runs and must render persisted state from SQLite rather than own task state.

Command surface:

```text
edf init --data-dir <path>
edf config show
edf find --source <domain> --crawl <id-or-month> --mode <web_graph|wat|warc> [--export-format csv|json|parquet] [--export-output <path>] [--no-export]
edf run start --source <domain> --crawl <id-or-month> --mode <web_graph|wat|warc> [--create-only] [--cdx-limit n] [--max-runtime 5m]
edf run status <run-id>
edf run status
edf run watch <run-id> [--ticks n] [--interval 10s]
edf run watch <run-id> --control <p|pause|resume|s|stop|q|quit>
edf run pause <run-id>
edf run resume <run-id> [--cdx-limit n] [--max-runtime 5m]
edf run stop <run-id>
edf results <run-id>
edf export <run-id> --format <csv|json|parquet> --output <path>
```

`edf find` is the primary command for normal use. It loads or creates local config/storage, creates a run, executes it in the foreground, prints progress every 10 seconds unless `--quiet` is set, writes a default CSV export to `runs/<run-id>/artifacts/exports/results.csv`, and prints the same compact candidate columns as `edf results`. `--export-format`, `--export-output`, and `--no-export` control the final export. When stdin is attached, `find` accepts the same line-oriented controls as watch mode: `p`, `s`, and `q`.

`edf run start` is retained for advanced recovery and inspection flows. It executes the selected workflow in the foreground by default and exits when the run pass completes, fails, pauses, or is cancelled. Use `--create-only` to persist run state without starting workers; created runs are marked `created`, not `running`, so status output does not imply background progress.

For smoke testing and controlled production, `find`, `run start`, and `run resume` accept safety caps: `--cdx-limit`, `--warc-task-limit`, `--wat-task-limit`, `--graph-task-limit`, `--availability-limit`, `--max-runtime`, `--max-warc-record-bytes`, `--max-warc-decoded-bytes`, `--max-wat-object-bytes`, and `--max-graph-object-bytes`. For unbounded production, set CDX/task/availability/runtime limits to `0` inside the selected source/crawl/mode scope. Byte ceilings must remain positive in the current implementation; set them very high when no practical size limit is intended. See `docs/smoke-run.md` for bounded and unbounded live-run procedures.

When a task-count budget is reached while work remains, foreground execution writes `execution=paused reason="...work budget reached"` and persists run status `paused` so the run can be resumed with the same or larger caps. Budget-paused runs are intentionally not marked `completed`.

Registrar rate limits are stored in SQLite as deferred availability tasks with `next_attempt_at`. If a production run appears idle with pending availability tasks, inspect the deferred count and earliest retry time from `docs/smoke-run.md` before increasing registrar concurrency.

`edf run status` without a run ID lists persisted runs from the configured data directory so interrupted runs are discoverable after process restart.

`edf run watch` is the supported current TUI/monitor. It renders state from SQLite snapshots, recent events, and candidate rows. The default refresh interval is 10 seconds to match the ETA recalculation cadence. The `--control` option applies the same persisted state transitions as the TUI controls: `p` toggles pause/resume based on current run status, `s` requests stop, and `q` exits without changing run state. When stdin is attached, watch mode also accepts line-oriented `p`, `s`, and `q` controls without terminal control codes.

When a run reaches `completed`, `cancelled`, or `failed`, the SQLite event stream is sealed to `runs/<run-id>/logs/events.jsonl.zst` and recorded in the run artifact manifest as `event_log`.

## Optional GUI Monitor

`cmd/edf-gui` provides an optional Fyne desktop monitor for single-source WARC+WAT runs. It uses the same SQLite-backed `internal/guiapp.Engine` and monitor view model as the automated `--run-once` path, but it is not the scriptable source of truth. The visual direction is documented in [design/07_gui_design_system.md](../design/07_gui_design_system.md).

Current GUI scope:

- one source domain
- one year/month crawl selection
- WARC+WAT modes
- start/cancel/resume controls
- operations monitor, KPI cards, stage monitor, candidate intelligence, alerts, and export path
- CSV export to `~/Documents`

Build it (Linux with cgo):

```bash
go build -buildvcs=false -o bin/edf-gui ./cmd/edf-gui
```

Use CLI/watch for multi-source, Web Graph, production profiles, JSON output, and advanced recovery workflows.

For Web Graph mode, configure graph release metadata in `config.json` when Common Crawl graph releases are known:

```json
{
  "commonCrawl": {
    "graphReleases": [
      {
        "id": "cc-domain-graph-2026-04",
        "months": ["2026-04"],
        "crawlIds": ["CC-MAIN-2026-18"],
        "vertexPath": "crawl-data/graph-release/vertices.txt.zst",
        "edgePath": "crawl-data/graph-release/edges.txt.zst"
      }
    ]
  }
}
```

If no configured graph release matches the selected crawl IDs, Web Graph mode attempts Common Crawl `graphinfo.json` and uses a matching domain-level release. For ad hoc local smoke tests, `edf run start --mode web_graph` also accepts `--graph-vertices` and `--graph-edges` object paths.
