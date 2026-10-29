# Lightweight Watch/TUI Reference

The current supported production terminal monitoring UI is a lightweight line-oriented watch view backed by SQLite state. The optional Fyne GUI is a separate desktop monitoring console for single-domain WARC+WAT validation and does not replace the CLI/watch workflow. A richer full-screen terminal TUI can use the same structure later:

```text
Expired Domain Finder

Run Summary:
  Status | Elapsed | ETA | Cache Hits | Recovery | Deferred Checks

Stages:
  Discovery
  Extraction
  Domain Checks
  Export

Activity:
  Recent persisted events and worker activity
  Deferred registrar checks show count and earliest next_attempt_at

Candidate Preview:
  Domain | Status | Confidence | Source Pages

Controls:
  p pause/resume
  s stop
  q quit
```

Implementation notes:

- The TUI/watch view is optional for scripts but preferred for long monitored production runs; plain CLI commands and SQLite remain authoritative.
- The TUI should render state derived from SQLite counters and events.
- `q` quits the viewer only; it does not stop a foreground or recoverable run.
- `p` and `s` map to the same persisted pause/resume/stop transitions as the CLI commands.
- Registrar rate limits should render as deferred work, not as terminal run failures.
- Avoid dense dashboard layouts and decorative terminal art.
