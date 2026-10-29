# ADR 0004 — Go Dependency Choices

## Status

Accepted

## Context

The project is now a Go CLI/TUI application. Core dependencies should support cross-platform local execution, high-concurrency network work, crash-recoverable SQLite state, PSL-correct domain handling, and low-overhead terminal rendering.

## Decision

Use these defaults:

- CLI parsing: Go standard `flag` package until command complexity requires `cobra`.
- TUI: Bubble Tea family (`bubbletea`, `bubbles`, `lipgloss`) when the placeholder TUI becomes interactive.
- SQLite: `modernc.org/sqlite` with `CGO_ENABLED=0` as the default driver strategy.
- Public Suffix List: `golang.org/x/net/publicsuffix`.
- Parquet/ZSTD: a Go Parquet library with native ZSTD support, selected when artifact writing is implemented.
- Config: explicit JSON config structs validated by application code and kept compatible with `schemas/app_config.schema.json`.

## Consequences

- Default builds avoid CGO and are easier to distribute as a single binary.
- CLI remains small while the command surface is still stabilizing.
- Future TUI work has a known library target without making the current scaffold depend on it prematurely.
- Dependency choices should be revisited only if a concrete implementation blocker appears.
