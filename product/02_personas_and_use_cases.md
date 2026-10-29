# Personas and Use Cases

## Primary persona

### Technical SEO / growth consultant

Needs:

- Find expired domains linked from authoritative sites.
- Avoid wasting time on domains already checked.
- See evidence before registering a domain.
- Run heavy processing locally without cloud costs.
- Export results for manual review or client workflows.

Constraints:

- Desktop workstation, not cluster infrastructure.
- Wants scriptable CLI commands with an optional clean TUI for live monitoring.
- Accepts long-running jobs if progress and recovery are reliable.

## Secondary persona

### Technical domain researcher

Needs:

- Search multiple publishers/source domains.
- Compare graph-only vs page-confirmed evidence.
- Maintain watchlists and recheck later.
- Export structured candidate datasets.

## Core use cases

### UC-001: Run latest crawl for TechCrunch

Input:

- Source domain: `techcrunch.com`
- Crawl months: latest selected crawl month
- Modes: Web Graph + WARC

Expected:

- Candidate domains appear during processing.
- Cache hits skip repeated checks.
- Available domains are marked with confidence and evidence.

### UC-002: Run several source domains together

Input:

- `techcrunch.com`
- `theverge.com`
- `venturebeat.com`

Expected:

- Candidate table shows which source domains linked to each candidate.
- Score improves when candidate appears across multiple source domains.

### UC-003: Resume after crash

Input:

- Any large run.
- App is forcibly killed mid-run.

Expected:

- On restart, run can resume.
- Completed tasks are not repeated unnecessarily.
- Expired leases are returned to pending.
- Artifact `.tmp` files are cleaned or ignored.

### UC-004: Reuse cache across runs

Input:

- Run A checks 100,000 domains.
- Run B later discovers many of the same domains.

Expected:

- Fresh cache entries are reused.
- Expired TTL entries are rechecked.
- User sees cache hit count.

### UC-005: Export candidate list

Input:

- Completed or partially completed run.

Expected:

- User can export available/likely/pending-delete domains.
- Export includes evidence and status details.
- Export identifies restricted, premium, reserved, unsupported, and ambiguous domains so they are not mistaken for buyable candidates.
