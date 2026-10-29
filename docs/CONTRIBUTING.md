# Contributing and Engineering Principles

Expired Domain Finder is a local-first Go CLI (`edf`) with a lightweight watch view and an optional Fyne desktop monitor. These principles keep it correct during runs that last days.

## Product principles

1. The interface is scriptable first. The watch view and the GUI show live progress and controls, but they never own state.
2. Crash recovery, task idempotency and correctness come before visual polish.
3. Long-running work is asynchronous, bounded and resumable.
4. Progress and ETA are recalculated every 10 seconds.
5. DNS alone never marks a domain as registerable.
6. Registrable domains come from the Public Suffix List, never from splitting on the last two labels.
7. Web Graph results are broad discovery evidence. WARC/WAT evidence can raise confidence.
8. SQLite is the operational source of truth. Parquet/ZSTD is used only for large immutable artifacts and exports.
9. All PSL-valid registrable domains are processed by default. Restricted, non-retail and unsupported TLDs are classified separately from buyable domains.
10. Every normalized link observation is kept in Parquet/ZSTD artifacts. CLI and GUI output show compact samples.

## Technical choices

Use:

- Go for the runtime, with the standard `flag` package for the CLI
- goroutines, contexts, bounded worker pools and channels
- SQLite in WAL mode (`modernc.org/sqlite`, no CGO needed for the CLI)
- Parquet with internal ZSTD compression for bulk artifacts

Avoid:

- long-lived, in-memory-only task progress
- naive domain parsing
- WHOIS-first availability checks for gTLDs
- importing full Common Crawl web graphs into SQLite
- unbounded queues or unbounded concurrency
- any GUI path that bypasses the shared SQLite-backed engine

## Development style

- Keep packages small and named by responsibility (`commoncrawl`, `graph`, `extraction`, `normalization`, `availability`, `storage`, `progress`, `config`, `cli`, `tui`, `guiapp`, `workflow`).
- Prefer explicit state machines for long-running tasks.
- Make every task idempotent so retries and recovery are safe.
- Cover Common Crawl, DNS, RDAP and registrar behaviour with integration tests against mocked servers.
- Derive CLI, watch and GUI state from backend events and database counters, not from terminal or widget assumptions.
- Before changing a schema, add a migration and update `engineering/10_storage_schema.md`.
- Write to SQLite with batched transactions or a dedicated writer.

## Definition of done

A change is complete when:

- `go vet ./...` and `go test ./...` pass, and `go test -race ./...` passes for concurrency-sensitive changes.
- `go build ./cmd/edf` succeeds. On Linux with cgo, `go build ./cmd/edf-gui` also succeeds.
- Docs in `docs/`, `engineering/` and `product/04_acceptance_criteria.md` reflect the new behaviour.
