# GUI Design System

The optional Fyne desktop monitor (`cmd/edf-gui`) turns a long-running crawl into a readable operations console. This document is the source of truth for its visual direction and information architecture.

## Direction

**Personality:** Precision & Density
**Foundation:** Cool neutral
**Depth:** Borders-only

Expired Domain Finder is a long-running operational tool. The interface should feel like a monitoring console: dense, legible, status-first, and calm under uncertainty. Decorative visuals are avoided; hierarchy comes from spacing, typography, grouping, and status language.

## Tokens

### Spacing
Base: 4px
Scale: 4, 8, 12, 16, 24, 32

### Colors
Use native Fyne theme colors where possible.

Semantic intent:
- Foreground: primary text and high-priority run status
- Secondary: labels, metadata, low-priority counters
- Accent: primary action, active progress
- Attention: recoverable work, failed units, warnings
- Success: completed run and export-ready candidates

### Radius
Scale: 4px, 6px, 8px

### Typography
Font: system
Scale: compact Fyne defaults; use bold labels for section headers and current run headline.
Weights: regular for values, semibold/bold for labels and headings.
Numbers: align in tabular-feeling rows with explicit labels.

## Patterns

### Run Controls
- Fixed-width left column
- Domain/year/month inputs grouped together
- Primary action directly below inputs
- Cancel and resume remain visible only when meaningful

### Monitor Overview
- First visible content in the main area
- Headline states current run phase in plain language
- KPI row shows progress, ETA, elapsed, cache hits, candidates, and export-ready count
- Warnings are shown as operational alerts, not inline prose

### Stage Monitor
- Each stage row shows percent, complete/total, state, throughput, ETA, and failed count
- Use compact rows for scanning
- Avoid raw debug-like text as the primary UI

### Candidate Intelligence
- Candidate status summary is visible during the run
- Top candidates show domain, status, confidence, score, and manual-check requirement
- Export readiness is explicitly named

## Decisions

| Decision | Rationale | Date |
|----------|-----------|------|
| Precision & Density direction | Crawl monitoring is operational and data-heavy; users need scanning and trust rather than decoration. | 2026-05-12 |
| Borders-only depth | Native desktop Fyne controls already provide structure; heavy shadows would reduce density. | 2026-05-12 |
| Three-zone console layout | Separates inputs, live run health, and candidate intelligence so long-running crawl status is understandable at a glance. | 2026-05-12 |
| Testable monitor view model | Keeps GUI insight logic verifiable and prevents widget code from becoming the source of truth. | 2026-05-12 |

## Information Architecture

### 1. Run Control Rail

Purpose: start, cancel, resume, and identify a run without competing with progress monitoring.

Content:

- Source domain
- Year/month crawl selector
- Start, cancel, resume actions
- Run ID
- Resolved crawl ID
- Export path

### 2. Operations Monitor

Purpose: show whether the crawl is healthy and how long it may take.

Content:

- Human-readable headline
- Status, progress, ETA, elapsed, cache hits
- Stage rows for Discovery, Extraction, Domain Checks, Export
- Failed-unit alerts
- Recovery/resume state

### 3. Candidate Intelligence

Purpose: show expired-domain findings during the run, before export.

Content:

- Candidate mix
- Export-ready candidate count
- Best confidence and evidence mode
- Top candidate rows with score and manual-verification signal

## Future polish

- Add full-screen terminal Bubble Tea TUI only if it can reuse the same SQLite-backed monitor model.
- Add screenshot-based GUI layout regression checks when a stable headless Fyne workflow is available.
- Add multi-source and Web Graph controls to the GUI only after they can preserve CLI-equivalent scope clarity and completion checks.
