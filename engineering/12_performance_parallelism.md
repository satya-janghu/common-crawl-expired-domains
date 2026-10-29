# Performance and Parallelism

## Hardware target

Primary target workstation:

```text
Ryzen 5950X
64 GB RAM
Consumer broadband/fiber internet
Local SSD/NVMe preferred
```

## Design principles

- Most work is network-bound or disk-streaming-bound.
- Use concurrency, but keep queues bounded.
- Avoid full graph imports.
- Avoid storing full HTML.
- Use streaming parsers.
- Batch writes.
- Use cache to prevent repeated domain checks.

## Default concurrency settings

| Area | Default | Notes |
|---|---:|---|
| CDX requests | 4 | Avoid hammering index host |
| WARC range requests | 48 | Tune based on bandwidth/latency |
| WAT file scans | 8 | Disk/network heavy |
| Graph file scans | 4 | Disk/CPU heavy; chunked |
| DNS checks | 300 | Resolver-dependent |
| RDAP checks | 20 | Bounded separately from DNS |
| Registrar checks | 10 | Provider-sensitive and lower than DNS |
| SQLite batch rows | 1,000 | Used for bulk ingest transactions |
| Parquet row group | 50,000–250,000 rows | Tune later |

## Smoke and pilot safety caps

Foreground `run start` and `run resume` support explicit caps for first live runs:

- `--cdx-limit`
- `--warc-task-limit`
- `--wat-task-limit`
- `--graph-task-limit`
- `--availability-limit`
- `--max-runtime`
- `--max-warc-record-bytes`
- `--max-warc-decoded-bytes`
- `--max-wat-object-bytes`
- `--max-graph-object-bytes`

Task-count caps are intentionally completeness-breaking. If work remains after a cap is reached, the run is marked `paused` so an operator can inspect and resume with the same or larger caps. `--cdx-limit` is also a completeness budget: exceeding it fails explicitly instead of silently truncating.

Unbounded production runs set CDX, task, availability, and runtime caps to `0` inside selected source/crawl/mode scope. They still use bounded worker pools and do not imply unbounded queues or global crawling. WARC/WAT/graph byte ceilings must remain positive in the current implementation; set them very high when no practical size limit is intended.

## Adaptive rate limiting

External calls use bounded worker pools, retry/backoff, and Retry-After handling where available. The implementation keeps separate concurrency controls for:

```text
index.commoncrawl.org
data.commoncrawl.org
rdap
registrar:{provider}
dns_resolver:{resolver}
```

On 429/503/timeout spikes:

- Reduce concurrency.
- Apply Retry-After if provided.
- Retry with exponential backoff.

Registrar-specific behavior:

- Parse Porkbun `ttlRemaining`, HTTP `Retry-After`, and `X-RateLimit-Reset` into a retry window.
- Persist the retry window in `availability_tasks.next_attempt_at`.
- Leave candidate availability unset while deferred, so exports do not present a throttled candidate as final.
- Resume naturally when the deferred task becomes due.
- Keep the production default `registrarCheckStatuses` to `["available_likely"]` so registrar quota is spent only on candidates where DNS/RDAP evidence suggests a buyable domain.

## Memory controls

- Bound all channels.
- Do not import full graph releases into SQLite.
- Preserve normalized link observations in Parquet/ZSTD artifacts.
- Store candidate domain dedupe in SQLite unique indexes plus limited in-memory LRU/Bloom optimization if needed.
- Graph processing streams source edges and resolves only target node IDs needed for the selected source domains.

## Benchmark scenarios

Implemented benchmark-smoke scaffolding covers:

1. 100 decoded CDX records.
2. A 100k-edge graph fixture.
3. 10k candidate domain ingestion.
4. 10k availability cache writes.

## Metrics to record

- CDX records/sec
- WARC records/sec
- MB/sec downloaded
- Links/sec extracted
- Unique domains/sec normalized
- DNS checks/sec
- RDAP checks/sec
- Registrar checks/sec
- SQLite rows/sec
- Parquet rows/sec
- Cache hit ratio
- Error/retry rate
