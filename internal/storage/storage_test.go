package storage

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"expired-domain-finder/internal/artifact"
	"expired-domain-finder/internal/availability"
	"expired-domain-finder/internal/commoncrawl"
	"expired-domain-finder/internal/config"
	"expired-domain-finder/internal/extraction"
	"expired-domain-finder/internal/normalization"

	"github.com/klauspost/compress/zstd"
)

func TestInitAppliesCacheSchemaAndWAL(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Availability.BlockedTLDs = []string{"zip"}
	cfg.Availability.BlockedSuffixes = []string{"blocked.example"}
	cfg.Availability.BlockedDomains = []string{"blocked.example"}
	if err := New(dir).Init(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := New(dir).Init(context.Background(), cfg); err != nil {
		t.Fatalf("second init should be idempotent: %v", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "cache", CacheDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var mode string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("migration count = %d, want 1", count)
	}
	var policy string
	if err := db.QueryRow(`SELECT policy_category FROM tld_support WHERE tld='gov'`).Scan(&policy); err != nil {
		t.Fatal(err)
	}
	if policy != "restricted" {
		t.Fatalf("gov policy = %s, want restricted", policy)
	}
	if err := db.QueryRow(`SELECT policy_category FROM tld_support WHERE tld='zip'`).Scan(&policy); err != nil {
		t.Fatal(err)
	}
	if policy != "unsupported_unknown" {
		t.Fatalf("zip policy = %s, want unsupported_unknown", policy)
	}
	category, ok, err := New(dir).TLDPolicyCategory(context.Background(), "anything.zip")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || category != "unsupported_unknown" {
		t.Fatalf("category=%s ok=%v", category, ok)
	}
	var ignoreRules int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ignore_rules WHERE enabled=1 AND ((rule_type='tld' AND pattern='zip') OR (rule_type='domain' AND pattern='blocked.example') OR (rule_type='suffix' AND pattern='blocked.example'))`).Scan(&ignoreRules); err != nil {
		t.Fatal(err)
	}
	if ignoreRules != 3 {
		t.Fatalf("ignore rules = %d, want 3", ignoreRules)
	}
}

func TestInitUsesEmbeddedSchemasOutsideRepoCWD(t *testing.T) {
	dir := t.TempDir()
	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalWD); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	})
	cfg := config.Default()
	cfg.DataDirectory = dir
	if err := New(dir).Init(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "cache", CacheDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("migration count = %d, want 1", count)
	}
}

func TestRetrySQLiteLockedRetriesLockedErrors(t *testing.T) {
	attempts := 0
	err := retrySQLiteLocked(context.Background(), func() error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("database is locked")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestTaskLeasingAndCompletion(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("example.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := store.LeaseTasks(context.Background(), run.ID, "warc_tasks", "test-worker", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("leased tasks = %d, want 1", len(tasks))
	}
	if err := store.StartTask(context.Background(), run.ID, "warc_tasks", tasks[0].ID); err != nil {
		t.Fatal(err)
	}
	runDir, err := store.RunDir(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(runDir, RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.QueryRow(`SELECT state FROM warc_tasks WHERE id=?`, tasks[0].ID).Scan(&state); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	if state != "running" {
		t.Fatalf("state = %s, want running", state)
	}
	if err := store.CompleteTask(context.Background(), run.ID, "warc_tasks", tasks[0].ID, true, ""); err != nil {
		t.Fatal(err)
	}
	more, err := store.LeaseTasks(context.Background(), run.ID, "warc_tasks", "test-worker", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(more) != 0 {
		t.Fatalf("leased completed task again: %+v", more)
	}
}

func TestCompleteTaskMarksFailedFinalWhenAttemptsExhausted(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("example.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	runDir, err := store.RunDir(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(runDir, RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE warc_tasks SET attempts=max_attempts WHERE run_id=?`, run.ID); err != nil {
		db.Close()
		t.Fatal(err)
	}
	var id int64
	if err := db.QueryRow(`SELECT id FROM warc_tasks WHERE run_id=? LIMIT 1`, run.ID).Scan(&id); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	if err := store.CompleteTask(context.Background(), run.ID, "warc_tasks", id, false, "boom"); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite", filepath.Join(runDir, RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var state string
	if err := db.QueryRow(`SELECT state FROM warc_tasks WHERE id=?`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "failed_final" {
		t.Fatalf("state = %s, want failed_final", state)
	}
	var errors int
	if err := db.QueryRow(`SELECT COUNT(*) FROM errors WHERE task_table='warc_tasks' AND task_id=? AND retryable=1`, id).Scan(&errors); err != nil {
		t.Fatal(err)
	}
	if errors != 1 {
		t.Fatalf("errors = %d, want 1", errors)
	}
}

func TestExpiredLeaseRecoveredOnOpen(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("example.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := store.LeaseTasks(context.Background(), run.ID, "warc_tasks", "crash-worker", 1, -31*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("leased tasks = %d, want 1", len(tasks))
	}
	snapshot, err := store.Snapshot(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.RecoveryState != "recovered" {
		t.Fatalf("recovery state = %s, want recovered", snapshot.RecoveryState)
	}
	runDir, err := store.RunDir(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(runDir, RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var state string
	if err := db.QueryRow(`SELECT state FROM warc_tasks WHERE id=?`, tasks[0].ID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "pending" {
		t.Fatalf("state = %s, want pending", state)
	}
	var events int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_type='leases_recovered'`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("recovery events = %d, want 1", events)
	}
}

func TestSnapshotExcludesPlaceholderExtractionTasks(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("example.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc", "wat", "web_graph"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range snapshot.Stages {
		if stage.Name == "Extraction" && stage.Total != 0 {
			t.Fatalf("extraction total = %d, want placeholder tasks excluded", stage.Total)
		}
	}
}

func TestSnapshotCountsOnlyEnabledExtractionModes(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("example.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	records := []commoncrawl.CDXRecord{{
		URLKey: "com,example)/", Timestamp: "20260401000000", URL: "https://example.com/",
		Mime: "text/html", Status: "200", Digest: "abc", Length: "100", Offset: "20", Filename: "crawl.warc.gz",
	}}
	if err := store.AddCDXRecords(context.Background(), run.ID, "example.com", "CC-MAIN-2026-04", records); err != nil {
		t.Fatal(err)
	}
	runDir, err := store.RunDir(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(runDir, RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE warc_tasks SET state='succeeded' WHERE filename='crawl.warc.gz'`); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range snapshot.Stages {
		if stage.Name == "Extraction" {
			if stage.Complete != 1 || stage.Total != 1 {
				t.Fatalf("extraction stage = %+v, want complete=1 total=1 for WARC-only run", stage)
			}
			return
		}
	}
	t.Fatal("missing extraction stage")
}

func TestListRunsDiscoversPersistedRuns(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("example.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	runs, err := New(dir).ListRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].ID != run.ID || runs[0].Status != "running" || runs[0].CreatedAt.IsZero() {
		t.Fatalf("runs = %+v", runs)
	}
}

func TestCreateRunIDsAreUniqueForSameSource(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("example.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	first, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatalf("duplicate run id %s", first.ID)
	}
}

func TestEnsureRunnableHonorsPauseAndResume(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("example.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetRunStatus(context.Background(), run.ID, "pause_requested"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureRunnable(context.Background(), run.ID); !errors.Is(err, ErrRunPaused) {
		t.Fatalf("pause err = %v, want ErrRunPaused", err)
	}
	status, err := store.RunStatus(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != "paused" {
		t.Fatalf("status = %s, want paused", status)
	}
	if err := store.SetRunStatus(context.Background(), run.ID, "resume_requested"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureRunnable(context.Background(), run.ID); err != nil {
		t.Fatalf("resume runnable err = %v", err)
	}
	status, err = store.RunStatus(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != "running" {
		t.Fatalf("status = %s, want running", status)
	}
	events, err := store.RecentEvents(context.Background(), run.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Message == "run status changed" {
			found = true
		}
	}
	if !found {
		t.Fatalf("status change event missing: %+v", events)
	}
	plan, err := store.RunPlan(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Sources) != 1 || plan.Sources[0] != "example.com" || len(plan.Crawls) != 1 || len(plan.Modes) != 1 {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestUpdateCrawlResolutionPersistsForResumePlan(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("example.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateCrawlResolution(context.Background(), run.ID, "2026-04", "CC-MAIN-2026-18", `{"from":"2026-04-01"}`); err != nil {
		t.Fatal(err)
	}
	plan, err := store.RunPlan(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Crawls) != 1 || plan.Crawls[0] != "CC-MAIN-2026-18" {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestRecoveredAvailabilityTaskWithStoredResultDoesNotBecomeRunnable(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddLinkObservations(context.Background(), run.ID, []extraction.LinkObservation{{
		SourceDomain:      "source.com",
		SourceURL:         "https://source.com/",
		CrawlID:           "CC-MAIN-2026-04",
		OutboundURL:       "https://target.com/",
		RegistrableDomain: "target.com",
		Mode:              "warc",
	}}, cfg); err != nil {
		t.Fatal(err)
	}
	runDir, err := store.RunDir(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(runDir, RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE candidate_domains SET availability_status='registered_active', confidence='high', last_checked_at=CURRENT_TIMESTAMP WHERE domain='target.com'`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE availability_tasks SET state='running', lease_owner='crashed-worker', leased_until=datetime('now','-31 minutes') WHERE domain='target.com'`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	snapshot, err := store.Snapshot(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.RecoveryState != "recovered" {
		t.Fatalf("recovery state = %s, want recovered", snapshot.RecoveryState)
	}
	runnable, err := store.RunnableTaskCount(context.Background(), run.ID, "availability_tasks")
	if err != nil {
		t.Fatal(err)
	}
	if runnable != 0 {
		t.Fatalf("runnable availability tasks = %d, want 0 when result is already stored", runnable)
	}
	db, err = sql.Open("sqlite", filepath.Join(runDir, RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var tasks, candidates int
	if err := db.QueryRow(`SELECT COUNT(*) FROM availability_tasks WHERE domain='target.com'`).Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM candidate_domains WHERE domain='target.com'`).Scan(&candidates); err != nil {
		t.Fatal(err)
	}
	if tasks != 1 || candidates != 1 {
		t.Fatalf("tasks=%d candidates=%d, want 1/1", tasks, candidates)
	}
}

func TestStopCancelsPendingTasks(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("example.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc", "wat", "web_graph"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetRunStatus(context.Background(), run.ID, "cancelling"); err != nil {
		t.Fatal(err)
	}
	runDir, err := store.RunDir(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(runDir, RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"warc_tasks", "wat_tasks", "graph_tasks"} {
		var nonCancelled int
		query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE state <> 'cancelled'`, table)
		if err := db.QueryRow(query).Scan(&nonCancelled); err != nil {
			t.Fatal(err)
		}
		if nonCancelled != 0 {
			t.Fatalf("%s non-cancelled tasks = %d", table, nonCancelled)
		}
	}
}

func TestCompletionBlockersDetectCancelledAndDeferredWork(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	records := []commoncrawl.CDXRecord{{
		URLKey: "com,source)/", Timestamp: "20260401000000", URL: "https://source.com/", Mime: "text/html", Status: "200", Digest: "abc", Length: "100", Offset: "20", Filename: "crawl.warc.gz", RawJSON: "{}",
	}}
	if err := store.AddCDXRecords(context.Background(), run.ID, "source.com", "CC-MAIN-2026-04", records); err != nil {
		t.Fatal(err)
	}
	blockers, err := store.CompletionBlockers(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if blockers["warc_active"] != 1 {
		t.Fatalf("blockers = %+v, want warc_active=1", blockers)
	}
	if err := store.SetRunStatus(context.Background(), run.ID, "cancelling"); err != nil {
		t.Fatal(err)
	}
	blockers, err = store.CompletionBlockers(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if blockers["warc_cancelled"] != 1 {
		t.Fatalf("blockers = %+v, want warc_cancelled=1", blockers)
	}

	run2, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	observations := []extraction.LinkObservation{{SourceDomain: "source.com", SourceURL: "https://source.com/", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://target.com/", RegistrableDomain: "target.com", Mode: "warc"}}
	if err := store.AddLinkObservations(context.Background(), run2.ID, observations, cfg); err != nil {
		t.Fatal(err)
	}
	tasks, err := store.LeaseAvailabilityTasks(context.Background(), run2.ID, "test", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	if err := store.DeferAvailabilityTask(context.Background(), run2.ID, tasks[0].ID, time.Now().Add(time.Hour), "rate limited"); err != nil {
		t.Fatal(err)
	}
	blockers, err = store.CompletionBlockers(context.Background(), run2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if blockers["availability_active"] != 1 {
		t.Fatalf("blockers = %+v, want availability_active=1", blockers)
	}
}

func TestObservationIngestionAvailabilityCacheAndResults(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	err = store.AddLinkObservations(context.Background(), run.ID, []extraction.LinkObservation{{
		SourceDomain:      "source.com",
		SourceURL:         "https://source.com/page",
		CrawlID:           "CC-MAIN-2026-04",
		OutboundURL:       "https://candidate.example/path",
		RegistrableDomain: "candidate.example",
		Mode:              "warc",
	}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	err = store.StoreAvailability(context.Background(), run.ID, AvailabilityRecord{
		Domain:            "candidate.example",
		Status:            availability.AvailableRegistrarConfirmed,
		Confidence:        availability.ConfidenceHighest,
		TLDPolicyCategory: "open_retail",
		Reason:            "mock registrar available",
		Provider:          "mock",
		Premium:           true,
		PriceCents:        9999,
		Currency:          "USD",
		CheckedAt:         now,
		ExpiresAt:         now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	cached, ok, err := store.CachedAvailability(context.Background(), "candidate.example", now)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || cached.Status != availability.AvailableRegistrarConfirmed {
		t.Fatalf("cached = %+v ok=%v", cached, ok)
	}
	if cached.Provider != "mock" || !cached.Premium || cached.PriceCents != 9999 || cached.Currency != "USD" {
		t.Fatalf("cached registrar details = %+v", cached)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Domain != "candidate.example" || results[0].Status != string(availability.AvailableRegistrarConfirmed) {
		t.Fatalf("results = %+v", results)
	}
	if results[0].EvidenceSample != "https://source.com/page" {
		t.Fatalf("evidence sample = %q", results[0].EvidenceSample)
	}
}

func TestCachedAvailabilityIgnoresStaleRecords(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := store.StoreAvailability(context.Background(), run.ID, AvailabilityRecord{
		Domain:            "candidate.example",
		Status:            availability.AvailableLikely,
		Confidence:        availability.ConfidenceMedium,
		TLDPolicyCategory: "open_retail",
		CheckedAt:         now.Add(-2 * time.Hour),
		ExpiresAt:         now.Add(-time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.CachedAvailability(context.Background(), "candidate.example", now); err != nil || ok {
		t.Fatalf("cache ok=%v err=%v, want stale miss", ok, err)
	}
}

func TestObservationIngestionSkipsBlockedCandidatesWithoutFailingBatch(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Availability.BlockedSuffixes = []string{"blocked.example"}
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	err = store.AddLinkObservations(context.Background(), run.ID, []extraction.LinkObservation{
		{SourceDomain: "source.com", SourceURL: "https://source.com/page", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://ignored.blocked.example/", RegistrableDomain: "ignored.blocked.example", Mode: "warc"},
		{SourceDomain: "source.com", SourceURL: "https://source.com/page", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://kept.com/", RegistrableDomain: "kept.com", Mode: "warc"},
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Domain != "kept.com" {
		t.Fatalf("results = %+v", results)
	}
}

func TestObservationIngestionSeedsAvailabilityTask(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	observations := []extraction.LinkObservation{
		{SourceDomain: "source.com", SourceURL: "https://source.com/a", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://candidate.example/a", RegistrableDomain: "candidate.example", Mode: "warc"},
		{SourceDomain: "source.com", SourceURL: "https://source.com/b", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://candidate.example/b", RegistrableDomain: "candidate.example", Mode: "warc"},
	}
	if err := store.AddLinkObservations(context.Background(), run.ID, observations, cfg); err != nil {
		t.Fatal(err)
	}
	tasks, err := store.LeaseAvailabilityTasks(context.Background(), run.ID, "test-worker", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Domain != "candidate.example" || tasks[0].Method != "classifier" {
		t.Fatalf("tasks = %+v", tasks)
	}
	if err := store.CompleteTask(context.Background(), run.ID, "availability_tasks", tasks[0].ID, true, ""); err != nil {
		t.Fatal(err)
	}
	more, err := store.LeaseAvailabilityTasks(context.Background(), run.ID, "test-worker", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(more) != 0 {
		t.Fatalf("leased completed availability task again: %+v", more)
	}
}

func TestLinkObservationBatchIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	observations := []extraction.LinkObservation{{
		SourceDomain:      "source.com",
		SourceURL:         "https://source.com/page",
		CrawlID:           "CC-MAIN-2026-04",
		OutboundURL:       "https://candidate.example/path",
		RegistrableDomain: "candidate.example",
		Mode:              "warc",
	}}
	manifest := artifact.Manifest{Path: filepath.Join(dir, "runs", run.ID, "artifacts", "link_observations", "warc-1.parquet"), Format: "parquet", Compression: "zstd", RowCount: 1, ByteCount: 10, SHA256: "abc"}
	for i := 0; i < 2; i++ {
		if err := store.AddLinkObservationBatch(context.Background(), run.ID, "warc:1:test", observations, cfg, manifest); err != nil {
			t.Fatal(err)
		}
	}
	runDir, err := store.RunDir(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(runDir, RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var sourcePages, outboundURLs, sourceSeen, progressComplete, artifacts, batches int
	if err := db.QueryRow(`SELECT source_page_count,outbound_url_count FROM candidate_domains WHERE domain='candidate.example'`).Scan(&sourcePages, &outboundURLs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT seen_count FROM candidate_sources WHERE domain='candidate.example'`).Scan(&sourceSeen); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT completed_units FROM progress_counters WHERE stage='extraction'`).Scan(&progressComplete); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM artifact_files WHERE artifact_type='link_observations' AND status='sealed'`).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM observation_batches WHERE batch_key='warc:1:test'`).Scan(&batches); err != nil {
		t.Fatal(err)
	}
	if sourcePages != 1 || outboundURLs != 1 || sourceSeen != 1 || progressComplete != 1 || artifacts != 1 || batches != 1 {
		t.Fatalf("counts source=%d outbound=%d seen=%d progress=%d artifacts=%d batches=%d, want all 1", sourcePages, outboundURLs, sourceSeen, progressComplete, artifacts, batches)
	}
}

func TestAddCDXRecordsSeedsWARCTasksIdempotently(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	records := []commoncrawl.CDXRecord{{
		URLKey: "com,source)/", Timestamp: "20260401000000", URL: "https://source.com/", Mime: "text/html", Status: "200", Digest: "abc", Length: "100", Offset: "20", Filename: "crawl.warc.gz", RawJSON: "{}",
	}}
	if err := store.AddCDXRecords(context.Background(), run.ID, "source.com", "CC-MAIN-2026-04", records); err != nil {
		t.Fatal(err)
	}
	if err := store.AddCDXRecords(context.Background(), run.ID, "source.com", "CC-MAIN-2026-04", records); err != nil {
		t.Fatal(err)
	}
	runDir, err := store.RunDir(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(runDir, RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var cdxCount, taskCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM cdx_records`).Scan(&cdxCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM warc_tasks WHERE filename='crawl.warc.gz' AND offset=20 AND length=100`).Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if cdxCount != 1 || taskCount != 1 {
		t.Fatalf("cdx=%d tasks=%d, want 1/1", cdxCount, taskCount)
	}
	snapshot, err := store.Snapshot(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Stages[0].Total == 0 {
		t.Fatalf("snapshot did not recalculate totals: %+v", snapshot.Stages)
	}
}

func TestCreateRunPersistsRecoverableState(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("https://www.example.com/path")
	if err != nil {
		t.Fatal(err)
	}
	run, err := New(dir).CreateRun(context.Background(), RunRequest{
		Sources: []normalization.Result{source},
		Crawls:  []string{"2026-04"},
		Modes:   []string{"warc", "wat", "web_graph"},
		Config:  cfg,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(dir).RunDir(run.ID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := New(dir).Snapshot(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != "running" {
		t.Fatalf("status = %q, want running", snapshot.Status)
	}
	if len(snapshot.Stages) != 4 {
		t.Fatalf("stages = %d, want 4", len(snapshot.Stages))
	}
	gotOrder := []string{string(snapshot.Stages[0].Name), string(snapshot.Stages[1].Name), string(snapshot.Stages[2].Name), string(snapshot.Stages[3].Name)}
	wantOrder := []string{"Discovery", "Extraction", "Domain Checks", "Export"}
	for i := range wantOrder {
		if gotOrder[i] != wantOrder[i] {
			t.Fatalf("stage order = %+v, want %+v", gotOrder, wantOrder)
		}
	}
}

func TestOpenRunCleansTemporaryArtifacts(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("example.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	runDir, err := store.RunDir(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := filepath.Join(runDir, "artifacts", "link_observations", "leftover.parquet.tmp")
	if err := os.WriteFile(tmpPath, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	stale := time.Now().Add(-10 * time.Minute)
	if err := os.Chtimes(tmpPath, stale, stale); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Snapshot(context.Background(), run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("temporary artifact still exists or stat failed: %v", err)
	}
}

func TestOpenRunPreservesFreshTemporaryArtifacts(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("example.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	runDir, err := store.RunDir(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := filepath.Join(runDir, "artifacts", "link_observations", "active.parquet.tmp")
	if err := os.WriteFile(tmpPath, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Snapshot(context.Background(), run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmpPath); err != nil {
		t.Fatalf("fresh temporary artifact should be preserved: %v", err)
	}
}

func TestCompletedRunSealsEventLogArtifact(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("example.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetRunStatus(context.Background(), run.ID, "completed"); err != nil {
		t.Fatal(err)
	}
	runDir, err := store.RunDir(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(runDir, "logs", "events.jsonl.zst")
	file, err := os.Open(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := zstd.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	scanner := bufio.NewScanner(reader)
	lines := 0
	foundCompleted := false
	for scanner.Scan() {
		lines++
		var row map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		if row["eventType"] == "run_status_changed" && row["severity"] == "info" {
			foundCompleted = true
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if lines == 0 || !foundCompleted {
		t.Fatalf("event log lines=%d foundCompleted=%v", lines, foundCompleted)
	}
	db, err := sql.Open("sqlite", filepath.Join(runDir, RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var artifacts int
	if err := db.QueryRow(`SELECT COUNT(*) FROM artifact_files WHERE artifact_type='event_log' AND format='jsonl' AND compression='zstd' AND status='sealed'`).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if artifacts != 1 {
		t.Fatalf("event log artifacts = %d, want 1", artifacts)
	}
}

func TestCompletedRunRecalculatesProgressBeforeSealing(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := New(dir)
	run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	observations := []extraction.LinkObservation{
		{SourceDomain: "source.com", SourceURL: "https://source.com/a", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://candidate.example/a", RegistrableDomain: "candidate.example", Mode: "warc"},
	}
	if err := store.AddLinkObservations(context.Background(), run.ID, observations, cfg); err != nil {
		t.Fatal(err)
	}
	record := AvailabilityRecord{
		Domain:            "candidate.example",
		Status:            availability.RegisteredActive,
		Confidence:        availability.ConfidenceHigh,
		TLDPolicyCategory: "open_retail",
		CheckedAt:         time.Now().UTC(),
	}
	if err := store.StoreAvailability(context.Background(), run.ID, record); err != nil {
		t.Fatal(err)
	}
	if err := store.SetRunStatus(context.Background(), run.ID, "completed"); err != nil {
		t.Fatal(err)
	}
	runDir, err := store.RunDir(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(runDir, RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var complete, total int
	if err := db.QueryRow(`SELECT completed_units,total_known_units FROM progress_counters WHERE stage='domain_checks'`).Scan(&complete, &total); err != nil {
		t.Fatal(err)
	}
	if complete != 1 || total != 1 {
		t.Fatalf("domain progress = %d/%d, want 1/1", complete, total)
	}
}

func TestTerminalRunStatusesSealEventLogArtifact(t *testing.T) {
	for _, status := range []string{"cancelled", "failed"} {
		t.Run(status, func(t *testing.T) {
			dir := t.TempDir()
			cfg := config.Default()
			cfg.DataDirectory = dir
			source, err := normalization.NormalizeSource("example.com")
			if err != nil {
				t.Fatal(err)
			}
			store := New(dir)
			run, err := store.CreateRun(context.Background(), RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.SetRunStatus(context.Background(), run.ID, status); err != nil {
				t.Fatal(err)
			}
			runDir, err := store.RunDir(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(runDir, "logs", "events.jsonl.zst")); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", filepath.Join(runDir, RunDBName))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var artifacts int
			if err := db.QueryRow(`SELECT COUNT(*) FROM artifact_files WHERE artifact_type='event_log' AND status='sealed'`).Scan(&artifacts); err != nil {
				t.Fatal(err)
			}
			if artifacts != 1 {
				t.Fatalf("event log artifacts = %d, want 1", artifacts)
			}
		})
	}
}
