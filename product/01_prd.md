# Product Requirements Document

## 1. Problem statement

Common Crawl contains historical and recent captures of pages from major publishers. Those pages contain outbound links to companies, projects, tools, events, and websites. Some linked domains later expire. Manually discovering these domains is slow because it requires filtering Common Crawl data, extracting links, reducing URLs to registrable domains, deduplicating candidates, and checking availability through several unreliable signals.

The product must automate this workflow locally with high throughput, crash recovery, evidence preservation, and domain-check caching.

Current implementation status is tracked separately in `product/04_acceptance_criteria.md`; this PRD describes the intended product contract.

## 2. Goals

### Functional goals

- Accept user-supplied source domains, maximum 10 per run.
- Allow selection of one or more crawl months/crawl IDs.
- Support Web Graph, WAT, and WARC extraction modes.
- Extract outbound URLs and reduce to registrable root domains.
- Deduplicate domains within a run and across prior runs.
- Check domain availability using DNS, RDAP, and registrar integrations.
- Check all PSL-valid registrable domains by default, except domains blocked by user/system policy or TLDs that cannot be bought as ordinary retail registrations.
- Maintain a global domain availability cache.
- Maintain a per-run crash-recoverable SQLite state DB.
- Show live progress with ETA recalculated every 10 seconds in CLI/TUI output and in the optional GUI monitor where supported.
- Present candidate domains with status, confidence, source page counts, evidence, and export options.

### Non-functional goals

- Local-first CLI/TUI operation, with `edf find` as the primary single-command workflow and separate run/results/export commands retained for recovery and inspection. The optional Fyne GUI is a monitoring console for supported one-source WARC+WAT runs.
- Capable of running for several days without losing progress.
- Efficient parallel and async architecture using Go concurrency.
- Safe retry behavior through idempotent task design.
- Bounded memory usage on 64 GB RAM.
- Supports Windows, Linux, and macOS where Go dependencies allow.
- Scriptable plain CLI behavior for automation, optional lightweight TUI for terminal monitoring, and optional desktop GUI monitoring without changing the SQLite source of truth.

## 3. User input requirements

### Source domains

- Passed through repeatable `--source` flags, config files, or newline input where commands support it.
- One domain or URL per value.
- Maximum 10 valid source domains.
- App normalizes input such as:
  - `techcrunch.com`
  - `https://www.techcrunch.com/path`
  - `www.techcrunch.com`
- App stores normalized source domain and original input.
- Invalid inputs are reported before a run starts.

### Crawl selection

- User selects month/year or explicit crawl ID through CLI flags.
- App maps month/year to Common Crawl crawl IDs and graph releases.
- App shows resolved crawl IDs before or at run start.
- Multiple months can be selected.
- Web graph releases may span multiple crawls; this must be visible in details/status output.

### Extraction modes

Mode flags:

- `web_graph`
- `wat`
- `warc`

At least one mode must be selected.

## 4. Output requirements

Candidate domains must show:

- Domain
- Availability status
- Confidence score
- Source domains
- Source page count
- Extraction modes where observed
- DNS summary
- RDAP summary
- Registrar summary
- First seen / last seen in run
- Evidence samples
- Score
- Notes/watchlist status

Scoring should prioritize registrar-confirmed availability, page-confirmed evidence, multi-source-domain mentions, source page count, TLD policy/buyability, and anchor/context quality. It must not hardcode `.com` as the only valuable TLD.

Exports:

- CSV
- JSON
- Parquet
- SQLite DB already exists as run artifact

## 5. Availability classification

Supported statuses:

```text
available_registrar_confirmed
available_likely
registered_active
registered_no_dns
pending_delete
redemption_period
reserved
restricted_tld
premium
unsupported_tld
rate_limited
ambiguous
error
ignored
```

Confidence levels:

```text
low
medium
high
highest
```

Rules:

- DNS absence alone cannot produce `available_registrar_confirmed`.
- RDAP not found can produce `available_likely`, subject to TLD-specific rules.
- Registrar API availability can produce `available_registrar_confirmed`.
- Porkbun is the first registrar availability provider.
- Registrar checks are gated by pre-registrar DNS/RDAP classification. The production default confirms only `available_likely` domains with Porkbun to preserve provider quota.
- Registrar provider rate limits defer the availability task until the provider retry window instead of finalizing the candidate as rate-limited.
- Premium, reserved, restricted, and unsupported statuses must remain separate from normal availability.
- Restricted/non-retail TLDs such as `.gov`, `.mil`, `.edu`, `.int`, and `.arpa` must be classified without registrar checks unless an explicit provider policy says otherwise.

### TLD policy

Default behavior:

- Process every candidate that has a PSL-valid registrable domain.
- Attempt RDAP where the TLD is supported; attempt registrar checks only for configured pre-registrar statuses, defaulting to `available_likely`.
- Classify domains under restricted or non-retail suffixes as `restricted_tld`.
- Classify TLDs with no supported RDAP/registrar path as `unsupported_tld` unless cached policy metadata says otherwise.
- Let users add blocked TLDs/domains/suffixes to reduce noise, but do not require a positive allow-list before a run can start.

## 6. Evidence model

Evidence levels:

```text
graph_only
wat_confirmed
warc_confirmed
warc_with_anchor_context
registrar_confirmed
```

Evidence should include, when available:

- Source domain
- Source URL
- Crawl ID
- Capture timestamp
- Common Crawl WARC filename/offset/length
- Outbound URL
- Registrable domain
- Anchor text
- Link rel attributes
- Surrounding text snippet
- Extraction mode

The app should not store full HTML by default.
The app should preserve all normalized link observations in Parquet/ZSTD artifacts by default, while CLI/TUI output shows compact samples per candidate.

## 6.1 Watchlist behavior

Watchlist rechecks are manual by default through “Recheck Now” or “Recheck Later.” Automatic scheduled rechecks are disabled unless explicitly enabled in settings.

## 7. Progress and ETA

The app must show:

- Overall progress bar or percentage
- ETA recalculated every 10 seconds
- Stage progress rows:
  - Discovery
  - Extraction
  - Domain Checks
  - Export
- Status summary:
  - Running/paused/stopping/completed/error
  - Elapsed time
  - Cache hits
  - Crash recovery enabled

ETA must use moving averages and dynamic queue projections. It must be marked low confidence early in a run or after major queue growth.

## 8. Storage requirements

Use separate operational stores:

```text
domain_cache.sqlite   # cross-run domain availability cache
run_state.sqlite      # per-run crash recovery and run state
```

Use Parquet/ZSTD only for large immutable artifacts:

```text
link_observations/*.parquet
graph_candidates/*.parquet
exports/*.parquet
```

## 9. Performance expectations

On a Ryzen 5950X with 64 GB RAM:

- Use bounded concurrency for network operations.
- Avoid loading entire Common Crawl graph releases into RAM.
- Avoid importing full graph files into SQLite.
- Use batch writes for SQLite.
- Use streaming parsers for WARC/WAT/graph files.
