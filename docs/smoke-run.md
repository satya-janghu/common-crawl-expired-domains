# Small Real-World Smoke And Final Test Runs

This procedure covers small one-domain live runs. The canonical final test uses one explicit historical crawl with WARC+WAT and no task/runtime budget inside the selected scope; the bounded smoke examples use a recent crawl with tight limits. Both paths verify live CDX discovery, WARC/WAT extraction, resumable SQLite state, availability classification, and exports without starting a broad crawl.

## Preconditions

- Run from the repository root.
- Network access to `index.commoncrawl.org` and `data.commoncrawl.org`.
- Optional: Porkbun credentials if you want registrar-confirmed availability. Without credentials, results still classify from policy, DNS, and RDAP signals. Provide credentials only through `PORKBUN_API_KEY` and `PORKBUN_SECRET_KEY`; do not put secret values in repo files.
- Default registrar pacing checks only `available_likely` candidates with Porkbun. Registrar rate limits defer tasks with `next_attempt_at`; inspect deferred counts before assuming a run has no remaining availability work.
- `sqlite3` and `jq` are useful for inspection, but the core run only needs Go.

Safe credential preflight:

```bash
test -n "$PORKBUN_API_KEY" && echo PORKBUN_API_KEY=set || echo PORKBUN_API_KEY=missing
test -n "$PORKBUN_SECRET_KEY" && echo PORKBUN_SECRET_KEY=set || echo PORKBUN_SECRET_KEY=missing
```

## Optional GUI Validation

The Fyne GUI is optional and intended for local single-domain WARC+WAT monitoring. It needs Linux with cgo:

```bash
go build -buildvcs=false -o bin/edf-gui ./cmd/edf-gui
```

To test the GUI engine path headlessly, use `--run-once`. It drives the same engine without opening the desktop window:

```bash
LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 ./bin/edf-gui \
  --run-once \
  --domain example.org \
  --year 2024 \
  --month 03
```

Audit the resulting run:

```bash
RUN_DB="$HOME/.local/share/expired-domain-finder/runs/<run-id>/run_state.sqlite"
sqlite3 -cmd '.timeout 30000' "$RUN_DB" "select status from runs; select stage,completed_units,total_known_units,failed_units from progress_counters order by stage; select count(*) from errors; select event_type,count(*) from events where event_type='leases_recovered' group by event_type;"
```

A healthy run shows `completed`, every progress counter complete with `failed_units=0`, `errors=0`, and no `leases_recovered` row. GUI CSV exports are written to `~/Documents`. CLI exports stay under the run artifacts directory unless `--export-output` is given.

Confirm the current crawl catalog before running:

```bash
curl -s https://index.commoncrawl.org/collinfo.json | jq -r '.[0].id, .[0].from, .[0].to'
```

The bounded smoke commands below use `CC-MAIN-2026-17` as an example. Replace it with the first ID from the catalog command for current smoke validation.

## Final Testing Method

The final live test method uses one small source domain, one explicit historical crawl, and both WARC and WAT modes. Limits set to `0` mean the command should drain the selected `example.org` / `CC-MAIN-2024-10` scope rather than stop at a testing budget. This is still not a global Common Crawl run.

```bash
export EDF_DATA_DIR=/tmp/edf-production-tests

go run ./cmd/edf --data-dir "$EDF_DATA_DIR" find \
  --source example.org \
  --crawl CC-MAIN-2024-10 \
  --mode warc \
  --mode wat \
  --cdx-limit 0 \
  --warc-task-limit 0 \
  --wat-task-limit 0 \
  --availability-limit 0 \
  --max-runtime 0
```

`find` creates storage/config as needed, executes the selected scope, records the default CSV export under the run directory, and prints a compact result preview. Use the advanced commands below only for SQLite inspection, recovery, or export regeneration.

Expected complete validation:

```bash
RUN_DB="$EDF_DATA_DIR/runs/<run-id>/run_state.sqlite"
sqlite3 "$RUN_DB" "select status from runs;"
sqlite3 "$RUN_DB" "select state,count(*) from cdx_discovery_pages group by state;"
sqlite3 "$RUN_DB" "select state,count(*) from warc_tasks where length > 0 group by state;"
sqlite3 "$RUN_DB" "select state,count(*) from wat_tasks where wat_filename <> 'pending-wat-sidecar' group by state;"
sqlite3 "$RUN_DB" "select state,count(*) from availability_tasks group by state;"
sqlite3 "$RUN_DB" "select artifact_type,count(*),sum(row_count) from artifact_files group by artifact_type;"
sqlite3 "$RUN_DB" "select component,error_message,retryable from errors order by id;"
```

## Run

```bash
export EDF_SMOKE_DIR=/tmp/edf-smoke
go run ./cmd/edf --data-dir "$EDF_SMOKE_DIR" init

go run ./cmd/edf --data-dir "$EDF_SMOKE_DIR" run start \
  --source example.org \
  --crawl CC-MAIN-2026-17 \
  --mode warc \
  --cdx-limit 50 \
  --warc-task-limit 5 \
  --availability-limit 20 \
  --max-runtime 10m \
  --max-warc-record-bytes 2097152 \
  --max-warc-decoded-bytes 4194304 \
  --no-tui
```

Safety bounds in this command:

- `--cdx-limit 50` fails instead of silently truncating if discovery would exceed fifty filtered CDX records.
- `--warc-task-limit 5` fetches at most five WARC ranges in this foreground pass.
- `--availability-limit 20` checks at most twenty candidate domains in this foreground pass.
- `--max-runtime 10m` cancels the foreground pass if it runs too long.
- WARC record and decoded-body byte caps keep individual records small.
- Common Crawl HTTP calls use a default timeout and retry/backoff policy; `--max-runtime` is still required as the outer foreground budget.

If the command is interrupted, resume with the same bounds:

```bash
go run ./cmd/edf --data-dir "$EDF_SMOKE_DIR" run status
go run ./cmd/edf --data-dir "$EDF_SMOKE_DIR" run resume <run-id> \
  --cdx-limit 50 \
  --warc-task-limit 5 \
  --availability-limit 20 \
  --max-runtime 10m \
  --max-warc-record-bytes 2097152 \
  --max-warc-decoded-bytes 4194304
```

## Expected Output

The start command should print:

```text
run_id=run_...
run_dir=/tmp/edf-smoke/runs/run_...
sources=example.org
crawls=CC-MAIN-2026-17
modes=warc
tui=disabled
execution=foreground
...
status=completed
```

If a configured safety budget is hit, the command should exit nonzero and print an explicit error such as `exceeded --cdx-limit` or `context deadline exceeded`. The run state remains in SQLite and can be inspected or resumed.

If a task-count budget is reached while work remains, the command exits successfully with `execution=paused reason="...work budget reached"` and the run status becomes `paused`. Resume with the same or higher bounds when you are ready to continue.

Expected files:

```text
$EDF_SMOKE_DIR/config.json
$EDF_SMOKE_DIR/cache/domain_cache.sqlite
$EDF_SMOKE_DIR/runs/<run-id>/run_state.sqlite
$EDF_SMOKE_DIR/runs/<run-id>/artifacts/link_observations/*.parquet
$EDF_SMOKE_DIR/runs/<run-id>/logs/events.jsonl.zst
```

The link-observation artifact may be absent if the sampled WARC pages contain no outbound HTTP(S) links.
The sealed event log is written only after a terminal status such as `completed`, `cancelled`, or `failed`; a budget-paused run keeps events in SQLite until it is resumed and reaches a terminal state.

## Inspect

Set `RUN_ID` from the printed output:

```bash
export RUN_ID=<run-id>
export RUN_DB="$EDF_SMOKE_DIR/runs/$RUN_ID/run_state.sqlite"

sqlite3 "$RUN_DB" "select id,status,created_at,updated_at from runs;"
sqlite3 "$RUN_DB" "select crawl_id,pattern,page,state,record_count,last_error from cdx_discovery_pages order by crawl_id,pattern,page;"
sqlite3 "$RUN_DB" "select state,count(*) from warc_tasks group by state;"
sqlite3 "$RUN_DB" "select state,count(*) from availability_tasks group by state;"
sqlite3 "$RUN_DB" "select count(*), min(next_attempt_at) from availability_tasks where state='pending' and next_attempt_at is not null;"
sqlite3 "$RUN_DB" "select domain,availability_status,confidence,tld_policy_category,source_page_count,outbound_url_count,evidence_modes from (select domain,availability_status,confidence,tld_policy_category,source_page_count,outbound_url_count,case when seen_in_warc=1 then 'warc' else '' end as evidence_modes from candidate_domains) order by domain limit 20;"

go run ./cmd/edf --data-dir "$EDF_SMOKE_DIR" results "$RUN_ID"
go run ./cmd/edf --data-dir "$EDF_SMOKE_DIR" export "$RUN_ID" \
  --format csv \
  --output "$EDF_SMOKE_DIR/results.csv"
```

A healthy bounded run has:

- One run row with `status='completed'`, or `failed` only with an explicit safety/network error.
- `status='paused'` is also healthy when a task-count budget stopped the pass before all pending tasks were drained.
- `cdx_discovery_pages` rows showing `succeeded` pages and record counts.
- At most five succeeded WARC tasks from this pass.
- At most twenty completed availability tasks from this pass.
- Candidate rows with distinct `source_page_count` and `outbound_url_count`.
- A CSV export containing `manual_verification_required`.

## Optional WAT Or Graph Smoke

Run WAT only after WARC succeeds, and keep the sidecar cap small:

```bash
go run ./cmd/edf --data-dir "$EDF_SMOKE_DIR" run start \
  --source example.org \
  --crawl CC-MAIN-2026-17 \
  --mode wat \
  --cdx-limit 5 \
  --wat-task-limit 1 \
  --availability-limit 20 \
  --max-runtime 10m \
  --max-wat-object-bytes 104857600 \
  --no-tui
```

Graph releases can be huge. Only run graph smoke with a known small fixture path or with an intentionally low graph object cap:

```bash
go run ./cmd/edf --data-dir "$EDF_SMOKE_DIR" run start \
  --source example.org \
  --crawl CC-MAIN-2026-17 \
  --mode web_graph \
  --graph-task-limit 1 \
  --availability-limit 20 \
  --max-runtime 10m \
  --max-graph-object-bytes 104857600 \
  --no-tui
```

If the graph file exceeds the object cap, the task should fail visibly rather than scanning an unbounded object.

## Production Profiles

Use these profiles as presets. Increase budgets only after inspecting the previous run state.

Smoke profile:

```bash
go run ./cmd/edf --data-dir "$EDF_DATA_DIR" run start \
  --source example.org \
  --crawl CC-MAIN-2026-17 \
  --mode warc \
  --cdx-limit 50 \
  --warc-task-limit 20 \
  --availability-limit 5 \
  --max-runtime 3m \
  --max-warc-record-bytes 2097152 \
  --max-warc-decoded-bytes 4194304 \
  --no-tui
```

Pilot profile:

```bash
go run ./cmd/edf --data-dir "$EDF_DATA_DIR" run start \
  --source source-a.example \
  --source source-b.example \
  --crawl CC-MAIN-2026-17 \
  --mode warc \
  --mode wat \
  --cdx-limit 50000 \
  --warc-task-limit 5000 \
  --wat-task-limit 50 \
  --availability-limit 1000 \
  --max-runtime 8h \
  --max-warc-record-bytes 8388608 \
  --max-warc-decoded-bytes 16777216 \
  --max-wat-object-bytes 1073741824 \
  --no-tui
```

Bounded full production profile:

```bash
go run ./cmd/edf --data-dir "$EDF_DATA_DIR" run start \
  --source source-a.example \
  --source source-b.example \
  --crawl CC-MAIN-2026-17 \
  --crawl CC-MAIN-2026-12 \
  --mode warc \
  --mode wat \
  --cdx-limit 0 \
  --warc-task-limit 0 \
  --wat-task-limit 0 \
  --availability-limit 0 \
  --max-runtime 0 \
  --max-warc-record-bytes 8388608 \
  --max-warc-decoded-bytes 16777216 \
  --max-wat-object-bytes 1073741824
```

Unbounded production means uncapped inside the selected source/crawl/mode scope, not global crawling. For example, `--source example.org --crawl 2024-03 --mode warc --mode wat` processes matching `example.org` WARC/WAT work for the Common Crawl crawl IDs resolved from March 2024. It does not process every domain or every crawl.

Use the unbounded profile only after confirming the selected source domains and crawl IDs are intentional. `find` prints progress every 10 seconds by default, and `run watch` can monitor the same run from another terminal. The app requires positive byte ceilings for WARC/WAT/graph safety settings; when you want no practical size limit, set those ceilings very high instead of omitting them, because omitting the flags uses configured defaults.

Advanced monitoring and export regeneration:

```bash
go run ./cmd/edf --data-dir "$EDF_DATA_DIR" run watch <run-id>
go run ./cmd/edf --data-dir "$EDF_DATA_DIR" export <run-id> \
  --format csv \
  --output "$EDF_DATA_DIR/<domain>-results.csv"
```

`--cdx-limit`, task limits, `--availability-limit`, `--max-runtime`, and object byte caps can make a run incomplete. Set CDX/task/availability/runtime limits to `0` to remove those caps inside the selected scope. Byte ceilings cannot be set to zero in the current implementation; use intentionally high byte ceilings when the operator wants no practical size limit. Any cap failure means that object was not fully processed.

## Completeness Checks

A run is complete when all of these checks pass:

```bash
sqlite3 "$RUN_DB" "select status from runs;"
sqlite3 "$RUN_DB" "select state,count(*) from cdx_discovery_pages group by state;"
sqlite3 "$RUN_DB" "select state,count(*) from warc_tasks where length > 0 group by state;"
sqlite3 "$RUN_DB" "select state,count(*) from wat_tasks where wat_filename <> 'pending-wat-sidecar' group by state;"
sqlite3 "$RUN_DB" "select state,count(*) from graph_tasks where file_path <> 'pending-graph-release' group by state;"
sqlite3 "$RUN_DB" "select state,count(*) from availability_tasks group by state;"
sqlite3 "$RUN_DB" "select count(*), min(next_attempt_at) from availability_tasks where state='pending' and next_attempt_at is not null;"
sqlite3 "$RUN_DB" "select artifact_type,count(*),sum(row_count) from artifact_files group by artifact_type;"
sqlite3 "$RUN_DB" "select component,error_message,retryable from errors order by id;"
```

Expected complete output has `status='completed'`, no `failed_retryable` tasks, no pending tasks for enabled modes, no CDX page failures, and an export artifact from `edf find` or a regenerated `edf export`. A `paused` run is recoverable but incomplete. A `failed` run is incomplete until the error is fixed and the run is resumed or rerun.

## Manual Verification

Exports intentionally mark every row with `manual_verification_required=yes`. Before buying a candidate, verify registrar availability, premium price, auction or aftermarket status, eligibility restrictions, trademark risk, historical content risk, and whether the TLD is actually supported by your registrar. Treat `available` and `likely_available` as signals, not purchase advice.

## Troubleshooting

Common failure signatures:

- `exceeded --cdx-limit`: the run would be incomplete under the current CDX budget. Inspect `cdx_discovery_pages`, raise `--cdx-limit`, narrow sources/crawls, or treat the run as failed evidence.
- `common crawl object exceeded max bytes`: a WAT, WARC, or graph object exceeded the configured safety cap. Raise the relevant byte cap only after confirming disk, runtime, and memory budgets.
- `web_graph could not resolve a domain graph release`: no configured release and no compatible live `graphinfo.json` domain release matched the selected crawl. Configure `commonCrawl.graphReleases` or pass `--graph-vertices` and `--graph-edges`.
- `common crawl object http 404`: the derived WAT or graph object is missing. The task is final-failed when missing sidecars are non-retryable.
- `rate_limited`: RDAP checks hit provider throttling. Registrar rate limits defer the availability task until the provider retry window; resume later if all remaining tasks are waiting on `next_attempt_at`.
- `registrarCheckStatuses`: defaults to `["available_likely"]`, so Porkbun is only used after DNS/RDAP evidence says the domain is likely available. Use `["available_likely","ambiguous"]` or `["*"]` only for tightly capped investigation runs.
- `database is locked`: SQLite retries are built into write paths, but repeated lock errors mean another process is holding the run DB. Stop duplicate workers for the same run and resume once the lock clears.
- `connection refused`, `timeout`, or `context deadline exceeded`: live Common Crawl or provider network failure. Inspect persisted state, then resume with the same or larger caps.
- `status='paused'`: a task or availability budget stopped the pass. Resume with the same or higher task caps.
- `status='failed'`: inspect `errors`, `events`, and task tables. Fix the cap/provider/network problem before resuming or rerunning.

## Validation Highlights

Live runs against Common Crawl confirmed these behaviours:

| Scenario | Result |
| --- | --- |
| Bounded WARC smoke (`example.org`, `CC-MAIN-2026-17`) | Discovery 8/8, extraction 8/8, domain checks 1/1; sealed Parquet link artifacts, export and event log. |
| Budget pause and resume | A run paused by `--warc-task-limit` resumed to `completed` with no duplicated observation batches. |
| Bounded WAT smoke | 31,771 link observations and 20,341 candidate domains ingested in idempotent batches, with no SQLite lock failures. |
| Web Graph object cap | `graphinfo.json` resolved the current domain release. A 1 MiB cap failed explicitly against the live 922 MB vertex and 18.2 GB edge objects instead of producing partial results. |
| Non-truncating CDX budget | A high-volume source with `--cdx-limit 1000` failed with an explicit truncation error. |
| Force-kill drill | `timeout -s KILL` during CDX discovery left a recoverable run. On resume, the lease was recovered and no committed CDX records were duplicated. |
| Registrar rate limits | Porkbun `RATE_LIMIT_EXCEEDED` responses were turned into a retry window, and the availability task was deferred through `next_attempt_at`. |
