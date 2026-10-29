# ADR 0001 — Use Go, CLI/TUI, and SQLite

## Status

Accepted

## Context

The app needs to be local-first, cross-platform, long-running, network-heavy, and crash-recoverable. It needs efficient streaming file processing, high parallelism for network workloads, straightforward task orchestration, and low resource usage.

The primary interface must be scriptable and reliable in long-running terminal sessions. A lightweight terminal UI is useful for live progress, logs, activity, and pause/resume/stop controls, but it must remain optional and derive state from durable storage.

## Decision

Use:

- Go for application logic and command execution.
- CLI commands as the primary interface.
- An optional lightweight TUI for live progress/log views.
- Goroutines, contexts, bounded worker pools, and channels for concurrency.
- SQLite WAL for operational state.
- Parquet/ZSTD for immutable large artifacts and exports.

## Consequences

Benefits:

- Fast development velocity with a small, readable standard library.
- Efficient enough for high-concurrency network work and streaming processing.
- Simple cross-platform single-binary distribution.
- Scriptable command surface for automation.
- TUI remains optional and low overhead.

Tradeoffs:

- Terminal UX requires careful command and output design.
- TUI needs careful handling in non-interactive environments.
- Some libraries, such as SQLite and Parquet, require deliberate dependency choices for cross-platform builds.
