# Testing Strategy

## Unit tests

### Domain normalization

Cases:

- `https://www.TechCrunch.com/path#frag` → `techcrunch.com`
- `sub.example.co.uk` → `example.co.uk`
- `foo.github.io` → `foo.github.io` or policy-dependent registrable domain
- IDN/punycode conversion
- URL with default ports
- Invalid schemes

### Availability classifier

Cases:

- Registrar available + RDAP not found + DNS absent
- RDAP success + DNS present
- RDAP success + DNS absent
- RDAP pending delete
- Conflicting registrar/RDAP
- Unsupported TLD
- Restricted/non-retail TLD
- Rate-limited provider

### Cache TTL

Cases:

- Registered domain gets long TTL.
- Available domain gets short TTL.
- Rate-limited result gets retry TTL.
- Expired cache entry queues recheck.

### Progress/ETA

Cases:

- Stable rate ETA.
- Zero-rate handling.
- Dynamic queue growth.
- Concurrent stage ETA uses max active stage ETA.
- Low confidence early run.

## Integration tests

### Mock Common Crawl CDX

- Returns JSON records.
- Domain-wide query includes apex, `www`, and subdomain captures.
- Returns 503 then success.
- Returns 429/503 with `Retry-After`.
- Returns malformed line.
- Returns duplicate digest.

### Mock WARC server

- Supports Range requests.
- Returns valid WARC response record.
- Returns corrupt compressed record.
- Returns truncated record.
- Simulates timeout.

### Mock WAT server

- Provides small WAT-like JSON records.
- Includes records for selected and non-selected source domains.
- Includes link metadata.
- Validates WARC-to-WAT path mapping and whole-sidecar streaming.

### Mock graph files

- Vertex file with source and target nodes.
- Edge file with outbound and unrelated edges.
- Reverse-domain labels.
- Malformed vertex and edge records.
- Large file streaming test.

### Mock DNS/RDAP/registrar

- DNS records present/absent.
- RDAP not found.
- RDAP success.
- Registrar available/premium/unavailable.
- Rate limits and retry-after.
- Registrar gating by pre-registrar classification.
- Deferred registrar retry windows from `ttlRemaining`, `Retry-After`, and rate-limit headers.

## Recovery tests

- Kill process after tasks leased.
- Kill process during Parquet temp write.
- Kill process during SQLite batch write.
- Retry after link-observation artifact write and candidate update.
- Retry after availability check before candidate update.
- Registrar rate-limit defer/resume without finalizing candidate status.
- Resume and verify no duplicate final candidates.
- Terminal run statuses seal `events.jsonl.zst`.

## CLI/TUI tests

- Input validation.
- Mode selection.
- Progress snapshot rendering.
- Candidate preview rendering.
- Export command behavior.
- Pause/resume/stop controls.
- `run start` executes in the foreground by default.
- `run start --create-only` creates a non-running durable run.
- `run resume` executes recoverable work in the foreground.

## GUI tests

- `internal/guiapp.MonitorView` summarizes progress, ETA, cache hits, stage state, candidate mix, export-ready count, alerts, and actions from persisted snapshots and candidate rows.
- GUI display-formatting helpers are covered separately from Fyne widget handlers.
- `cmd/edf-gui --run-once` validates the GUI engine path without desktop interaction.
- `internal/guiapp` should stay at or above 80% statement coverage; the current GUI monitoring pass reports 81.2%.
- Full desktop screenshot regression is a future enhancement once a stable headless Fyne rendering path is available.

## Benchmark tests

Use small real or fixture datasets:

- 1 source domain, 1 crawl, 100 CDX records.
- 1 graph fixture with 100k edges.
- 10k candidate domains with mocked cache.
- 10k availability checks with mocked providers.
