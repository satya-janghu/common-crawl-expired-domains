# ADR 0002 — Use SQLite for Operational State

## Status

Accepted

## Context

Run progress, task leases, retries, candidate tables, cache TTLs, and CLI/TUI queries require mutable transactional state.

## Decision

Use SQLite WAL as the authoritative store for:

- Per-run state
- Task queues
- Domain availability cache
- Artifact manifests
- Candidate summaries

Use separate databases for run state and global cache.

## Consequences

Benefits:

- Strong local durability.
- Simple deployment.
- Easy status/results queries.
- Good crash recovery when tasks are idempotent.

Tradeoffs:

- Writes are serialized.
- Requires batched writes and dedicated writer pattern.
- Not ideal for massive immutable row stores, so Parquet is used for artifacts.
