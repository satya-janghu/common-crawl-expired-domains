# Security, Privacy, and Compliance

## Security scope

This is a local CLI/TUI tool with an optional desktop GUI. It still needs safeguards because it uses provider credentials at runtime and stores network logs and domain research data.

## Secrets

Registrar API keys and other credentials must:

- Be stored outside logs.
- Be masked in CLI/TUI output.
- Be excluded from exports.
- Be supplied through environment variables, currently `PORKBUN_API_KEY` and `PORKBUN_SECRET_KEY`.
- Be referenced from config by environment variable name only, such as `env:PORKBUN_API_KEY`, when config references are used.
- Never be copied into source code constants or generated documentation.

## Network behavior

The app must:

- Respect provider rate limits.
- Respect Retry-After headers.
- Avoid unbounded request floods.
- Use user-agent identifying the app.
- Avoid scraping live websites; use Common Crawl data for extraction.

## Data privacy

Local DBs may contain:

- Source domains searched by the user.
- Candidate domains.
- Evidence URLs.
- Registrar check results.
- API provider metadata.

Do not send this data to any third-party service except explicitly configured availability providers.

## Legal/operational cautions

- Domain availability signals can be wrong.
- Some domains are reserved, restricted, trademarked, premium, auctioned, or unavailable despite weak signals.
- The tool must not auto-purchase domains.
- Exports should include a disclaimer field or note: “manual verification required.”
- Restricted/non-retail TLDs must never be presented as ordinary buyable inventory.

## Logging

Logs must not include:

- API keys
- Authorization headers
- Full provider credential objects

Logs may include:

- Domain names
- Status codes
- Error types
- Task IDs
- Timestamps

Terminal runs seal compact operational events to `logs/events.jsonl.zst` for auditability. These rows include event type, severity, component, message, optional structured payload, and timestamp; they must not include credential material.

Current logs and SQLite state are sufficient for expected production failures such as task errors, cap failures, provider rate limits, artifact problems, and recovery after interruption. Panic stack traces go to stdout/stderr, so production wrappers should capture them; persisting them to the run database is on the roadmap.

## GUI security

The optional Fyne GUI follows the same credential policy as CLI/TUI:

- Do not display API key values, secret key values, authorization headers, or provider credential objects.
- Documentation and setup screens may name `PORKBUN_API_KEY` and `PORKBUN_SECRET_KEY`, but must not include secret values.
- GUI exports include candidate evidence and manual-verification fields, not credential material.
- GUI run state and progress remain backed by the same local SQLite files and event logs as CLI runs.

## Safe defaults

- Conservative RDAP/registrar concurrency.
- Registrar checks are gated to likely-available candidates by default.
- Registrar rate limits are persisted as retry windows instead of retried aggressively.
- No auto-recheck schedule unless user enables it.
- No external telemetry by default.
- No cloud sync by default.
