# Common Crawl Pipelines

## CDXJ discovery

Purpose:

- Discover relevant captures for source domains.
- Obtain WARC `filename`, `offset`, and `length` for byte-range retrieval.

Query pattern:

```text
https://index.commoncrawl.org/{crawl_id}-index?url={source_domain}&matchType=domain&output=json
```

Notes:

- Use domain-wide matching for source domains so apex, `www`, and subdomains are discovered.
- For crawls or server behavior where wildcard domain matching is preferred, `url=*.{source_domain}` is acceptable without also requiring `matchType=domain`.
- CDX discovery uses `showNumPages=true`, then fetches each page with a fixed `pageSize`.
- Each CDX page is recorded in `cdx_discovery_pages`, so resume skips succeeded pages and retries only incomplete pages.
- `--cdx-limit` is a safety budget. If paginated discovery exceeds it, the run fails with an explicit truncation error instead of silently returning partial results.
- Client-side filtering still enforces `status=200`, HTML MIME, valid filename, positive offset, and positive length because index-side filters vary by server/release.

Request fields:

```text
url
urlkey
timestamp
mime
status
digest
length
offset
filename
```

Filters:

- Prefer `status=200`.
- Prefer `mime` containing HTML.
- Ignore obvious binary/document MIME types unless configured.
- Deduplicate by `digest` where safe.

## WARC range mode

Purpose:

- Fetch only the exact WARC response records for selected source pages.
- Extract precise source page evidence and anchor text.

Flow:

```text
CDX records
  ↓
WARC tasks
  ↓ HTTP Range request to data.commoncrawl.org
Record decode
  ↓
HTML parse
  ↓
Outbound link observations
```

Implementation notes:

- CDX ranges above the configured max record size are final parse failures before fetch.
- Range responses are limited to the expected byte length and fail if the server returns extra or truncated data.
- WARC decoded bodies have a configured max size. Malformed, truncated, and oversized records are final parse failures; network and retryable HTTP failures remain retryable.
- Do not store full HTML by default.
- Store WARC filename/offset/length as evidence pointer.
- Write link observations to Parquet/ZSTD artifacts before committing candidate updates, then record the artifact and stable observation batch in SQLite in one transaction.

## WAT mode

Purpose:

- Use Common Crawl metadata records to extract links without full HTML parsing when practical.

Challenge:

- CDXJ offsets point to WARC records, not necessarily directly to WAT records.
- WAT mode scans corresponding WAT sidecar files derived from WARC filenames.
- Do not assume CDX WARC byte offsets can be used as WAT byte offsets.

Flow:

```text
Relevant WARC filenames from CDX
  ↓
Map WARC paths to corresponding WAT paths
  ↓
Deduplicate WAT file tasks
  ↓
Stream WAT records
  ↓
Filter records to source domains
  ↓
Extract link metadata
```

Rules:

- WAT mode must be optional and bounded.
- WAT files are whole-file streaming/filtering tasks, not precise byte-range tasks.
- If WAT file cannot be mapped or fetched, mark task failed_final and continue.
- Evidence from WAT is not as strong as WARC with anchor context unless WAT contains enough source detail.
- WAT sidecars are streamed line by line with scanner and total-byte limits.
- Malformed JSONL records are skipped without aborting the sidecar.
- Observations flush in stable batches keyed as `wat:<task>:<batchIndex>:...`, making retry/resume idempotent for committed batches.
- WAT resume is batch-idempotent but not byte-offset checkpointed inside a single large sidecar.

## Web Graph mode

Purpose:

- Fast domain-level candidate discovery from Common Crawl host/domain graph releases.

Use domain-level graph first.

Flow:

```text
Graph release selection
  ↓
Stream vertices → find source domain node IDs
  ↓
Stream edges → keep outbound edges from source node IDs
  ↓
Collect target node IDs
  ↓
Stream vertices again → resolve target IDs to domains
  ↓
Emit graph candidate observations
```

Rules:

- Do not import the full graph into SQLite.
- Do not load all vertices into RAM.
- Stream graph files by default and cache filtered source-domain graph results only.
- Mark candidates as `graph_only` until confirmed by WAT/WARC.
- Treat graph releases as broad discovery datasets; they may span multiple crawl IDs and are not exact calendar-month equivalents.
- Configured graph releases are used first.
- If no configured release matches, the app fetches Common Crawl `graphinfo.json` and selects a domain-level graph release whose crawl list overlaps the selected crawl IDs.
- Explicit `--graph-vertices` and `--graph-edges` paths remain available for smoke tests.
- Host-only releases or releases without domain graph paths are skipped.

## Graph format assumptions

- Domain labels may use reverse-domain notation.
- Parser must support release-specific metadata.
- Graph releases often correspond to multiple crawl IDs, not exactly one calendar month.

## Artifact outputs

```text
artifacts/link_observations/part-*.parquet
artifacts/graph_candidates/part-*.parquet
```

Use internal ZSTD compression.

Observation ingestion is idempotent at the task batch level. A retry after artifact creation or candidate writes must use the same stable batch key so candidate counts, progress counters, and availability tasks are not duplicated.
