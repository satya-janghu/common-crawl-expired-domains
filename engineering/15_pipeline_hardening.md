# Pipeline Hardening

These safeguards make live Common Crawl runs safe to leave unattended inside a selected source/crawl/mode scope.

## Discovery

- CDX discovery accepts both the plain-integer and JSON `showNumPages` response shapes, and it filters on exact `mime:text/html` because broad MIME regex filters can return unusable page fetches on current indexes.
- Discovery paginates with `showNumPages`, records each page's state in `cdx_discovery_pages`, skips succeeded pages on resume, and re-applies client-side validity filters (status 200, HTML MIME, valid filename, positive offset and length).
- `--cdx-limit` is an explicit failure budget. Exceeding it fails the run with a truncation error instead of silently returning partial results.
- When the CDX API fails transiently, discovery falls back to a local cache of previously discovered records, then to reading the raw `cluster.idx` index directly, before recording a retryable page failure.
- CLI and GUI runs build the Common Crawl client through one shared constructor, so every HTTP call inherits the default timeout, retry count, exponential backoff with jitter, and `Retry-After` handling.

## Extraction

- WARC processing rejects oversized ranges before fetching, verifies the exact range length, caps the decoded body size, and records malformed, truncated or oversized payloads as final parse failures. Network and retryable HTTP failures stay retryable.
- WAT processing streams sidecars line by line, skips malformed JSONL records, caps line and total bytes, and commits observations in stable, idempotent batches. Resume is idempotent at committed batch boundaries. It does not checkpoint byte offsets inside a single sidecar.
- Web Graph mode resolves configured releases first, then falls back to Common Crawl `graphinfo.json`. It skips releases without domain-level paths, parses reverse-domain vertex rows, tolerates malformed edge lines, and preserves duplicate edge counts. If an object exceeds its byte cap, the task fails visibly instead of producing incomplete graph results.

## Idempotency and evidence

- Candidate evidence keeps mode flags distinct. Unique WAT/WARC source pages are counted separately from total outbound observations, and graph edge counts stay graph-specific.
- WARC, WAT and graph ingestion use stable batch keys. Retrying an already-ingested task does not duplicate candidate counts, source-page counts, graph edge counts, artifact manifests or observation batch records.

## Availability

- Registrar checks are gated by the DNS/RDAP pre-classification. The default `registrarCheckStatuses: ["available_likely"]` keeps registered, restricted, reserved, unsupported, pending-delete, redemption, error and ambiguous candidates away from the Porkbun quota.
- Porkbun rate limits produce a typed retry window from `ttlRemaining`, `Retry-After` or `X-RateLimit-Reset`. Throttling defers the availability task with a persisted `next_attempt_at` instead of writing a final result or spending retry attempts.

## Safety caps

Foreground commands accept a CDX budget; WARC, WAT and graph task budgets; an availability budget; a runtime limit; and WARC, WAT and graph byte caps. Task-count budgets pause the run (status `paused`) so it can be resumed. They never mark it `completed`. Unbounded production runs set the count and runtime caps to `0` inside the selected scope, and keep byte ceilings positive and very high.

## Known limits

- Panic stack traces go to stderr, so process managers should capture them; persisting them to the run database is on the roadmap.
- Full live graph artifact creation means scanning roughly 19 GB of compressed graph files per release, so operators should approve that cost explicitly.
