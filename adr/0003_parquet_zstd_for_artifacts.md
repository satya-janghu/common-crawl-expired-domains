# ADR 0003 — Use Parquet with ZSTD for Large Artifacts

## Status

Accepted

## Context

Link observations and graph candidate observations can become large. They are mostly append-style and read analytically, not updated frequently.

## Decision

Use Parquet with internal ZSTD compression for large immutable artifacts.

Examples:

- `link_observations/part-*.parquet`
- `graph_candidates/part-*.parquet`
- `exports/*.parquet`

## Consequences

Benefits:

- Compact storage.
- Column-oriented reads.
- Better exports for analytical workflows.
- Avoids bloating SQLite with every raw observation.

Tradeoffs:

- Not suitable for mutable task state.
- Requires artifact manifest and crash-safe sealing.
- Slightly more complex writer pipeline.
