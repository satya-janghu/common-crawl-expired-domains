# Product Brief

## Product name

Expired Domain Finder

## One-line description

A local Go CLI/TUI application, with optional Fyne GUI monitoring, that processes Common Crawl data for selected source domains, extracts outbound domains, checks domain availability, and returns evidence-backed expired-domain candidates.

## Target user

A technical SEO consultant, growth marketer, or domain researcher who wants to discover potentially valuable expired domains that were previously linked from authoritative publisher/source domains.

## Primary workflow

1. User runs `edf`.
2. User starts a new run.
3. User provides up to 10 source domains.
4. User selects one or more crawl months/crawl IDs.
5. User selects extraction modes: Web Graph, WAT, WARC.
6. User starts the run from the CLI.
7. App processes Common Crawl data, extracts outbound links/domains, deduplicates, checks domain availability, and continuously saves progress.
8. User monitors progress and ETA through line-oriented CLI output, the optional TUI/watch view, or the optional Fyne GUI for supported one-source WARC+WAT runs.
9. User reviews candidate domains with evidence.
10. User exports or adds candidates to a watchlist.

## Non-goals

- This is not a cloud SaaS platform.
- This is not a live web crawler.
- This is not a domain marketplace.
- This does not automatically purchase domains.
- This does not guarantee SEO value or legal safety of a domain.

## Success definition

The product succeeds if a user can run a multi-hour/multi-day Common Crawl extraction job, recover from crashes, avoid duplicate checks across runs, and produce a ranked list of candidate expired domains with enough evidence to manually verify and act.

## Key risks

| Risk | Mitigation |
|---|---|
| Common Crawl graph/WAT/WARC formats vary by release | Use release manifests, strict parsers, and integration tests with real sample files |
| RDAP/registrar rate limits slow availability checking | Bounded concurrency, provider retry windows, persisted `next_attempt_at`, cache TTLs, and resume workflow |
| False positives for domain availability | Multi-stage classification; registrar-confirmed availability is highest confidence |
| Restricted/non-retail TLDs look available in weak signals | Maintain TLD policy metadata and classify them separately from buyable domains |
| SQLite write contention | Dedicated writer tasks and batched transactions |
| TUI appears too dense | Keep minimal terminal layout; keep detailed diagnostics in logs and status/details commands |
| GUI hides operational truth behind presentation | Derive GUI state from SQLite snapshots/results through the tested monitor view model |
| Huge graph files overwhelm disk/RAM | Stream files; never import whole graph into SQLite; cache only filtered results |
