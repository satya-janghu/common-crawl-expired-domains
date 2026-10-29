# Product Decisions

These decisions are part of the current product contract unless a later PRD update changes them.

## Registrar availability provider

Porkbun is the first registrar provider.

- Registrar-confirmed availability is stronger than DNS or RDAP alone.
- API credentials must be provided through runtime references such as environment variables.
- Secrets must never appear in logs, exports, errors, or generated documentation.
- Registrar checks are quota-sensitive and must be gated by the pre-registrar DNS/RDAP classification.
- The production default is `registrarCheckStatuses: ["available_likely"]`, so registered, restricted, reserved, unsupported, pending-delete, redemption, error, and ambiguous candidates do not spend Porkbun quota.
- Registrar rate limits must persist the provider retry window in run state and defer the availability task instead of writing a final candidate result or consuming retry attempts.

## Target TLDs

The app checks all PSL-valid registrable domains by default, except domains/suffixes blocked by user or system policy and TLDs that cannot be bought as ordinary retail registrations.

Restricted or non-retail examples:

```text
.gov, .mil, .edu, .int, .arpa
```

Restricted, reserved, unsupported, premium, and ambiguous domains must be classified separately instead of hidden or presented as ordinary available domains.

## Evidence depth

The app preserves normalized link observations in Parquet/ZSTD artifacts by default and shows compact samples in CLI/watch output.

## Web Graph downloads

The default strategy is to stream graph files and cache only filtered source-domain results/artifacts. Raw graph file caching may be added later as an advanced setting because full graph releases are large.

## Operating system target

The intended target platforms are Windows, Linux, and macOS where Go dependencies allow. Development and automated validation start on Linux, with path and secret-reference behavior kept portable.

## API key storage

Registrar API secrets are provided through environment variables. The supported Porkbun variable names are `PORKBUN_API_KEY` and `PORKBUN_SECRET_KEY`; docs and config examples may reference those names but must not contain secret values. Plaintext local config should only be used behind explicit user choice and warning.

## Optional GUI distribution

The GUI is an optional, separately built binary (`edf-gui`, Linux with cgo). It writes user-facing CSV exports to `~/Documents`, while run state and audit evidence stay in the configured SQLite data directory.

## Interface design system

The GUI design source of truth is [design/07_gui_design_system.md](../design/07_gui_design_system.md). GUI direction is Precision & Density: a monitoring-console layout with a run-control rail, operations monitor, stage monitor, candidate intelligence and alerts. The CLI stays scriptable first, and SQLite stays authoritative.

## Domain scoring

Scoring should prioritize registrar-confirmed availability, page-confirmed evidence, multi-source-domain mentions, source page count, TLD policy/buyability, and anchor/context quality. It must not hardcode `.com` as the only valuable TLD.

## Watchlist behavior

Watchlist rechecks are manual by default through explicit user action. Automatic scheduled rechecks are disabled unless explicitly enabled in settings.

## Production readiness

Documented profiles, completeness checks, the resume workflow and the manual candidate verification process live in [smoke-run.md](smoke-run.md). The canonical live test is the single-source `example.org` / `CC-MAIN-2024-10` WARC+WAT `edf find` run.

Unbounded production means uncapped inside the selected source/crawl/mode scope, not global Common Crawl processing. Source domains, crawl IDs or months, and modes still define the scope. CDX, task, availability and runtime caps can be set to `0`. Byte ceilings must stay positive, so runs with no practical size limit use deliberately high byte ceilings. Operators still need to account for Common Crawl service availability, graph object size, registrar credentials, provider rate limits, monitoring, completeness checks and manual review before buying domains.
