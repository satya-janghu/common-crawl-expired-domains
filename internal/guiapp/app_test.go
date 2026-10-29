package guiapp

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"expired-domain-finder/internal/availability"
	"expired-domain-finder/internal/commoncrawl"
	"expired-domain-finder/internal/config"
	"expired-domain-finder/internal/extraction"
	"expired-domain-finder/internal/normalization"
	"expired-domain-finder/internal/progress"
	"expired-domain-finder/internal/storage"

	_ "modernc.org/sqlite"
)

func TestResolveCrawlUsesClosestOlderOrExactCatalogEntry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/collinfo.json" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"id":"future","from":"2026-04-01"},
			{"id":"older","from":"2026-02-28"},
			{"id":"oldest","from":"2026-01-01"}
		]`))
	}))
	defer server.Close()

	cfg := config.Default()
	cfg.DataDirectory = t.TempDir()
	cfg.CommonCrawl.IndexBaseURL = server.URL
	engine := NewEngine(cfg)

	got, err := engine.ResolveCrawl(context.Background(), 2026, time.March)
	if err != nil {
		t.Fatal(err)
	}
	if got.ResolvedID != "older" {
		t.Fatalf("resolved crawl = %q, want older", got.ResolvedID)
	}
}

func TestResolveCrawlFallsBackWhenCatalogEndpointIsUnavailable(t *testing.T) {
	cfg := config.Default()
	cfg.DataDirectory = t.TempDir()
	engine := NewEngine(cfg)
	engine.CommonCrawl.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("connect: connection refused")
	})}

	got, err := engine.ResolveCrawl(context.Background(), 2024, time.May)
	if err != nil {
		t.Fatal(err)
	}
	if got.ResolvedID != "CC-MAIN-2024-22" {
		t.Fatalf("resolved crawl = %q, want fallback 2024-05 crawl", got.ResolvedID)
	}
}

func TestWriteFilteredCSVExportOnlyExpiredStatuses(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	store := storage.New(dir)
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.CreateRun(ctx, storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddLinkObservations(ctx, run.ID, []extraction.LinkObservation{
		{SourceDomain: "source.com", SourceURL: "https://source.com/", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://confirmed.com/", RegistrableDomain: "confirmed.com", Mode: "warc"},
		{SourceDomain: "source.com", SourceURL: "https://source.com/", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://likely.com/", RegistrableDomain: "likely.com", Mode: "warc"},
		{SourceDomain: "source.com", SourceURL: "https://source.com/", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://registered.com/", RegistrableDomain: "registered.com", Mode: "warc"},
	}, cfg); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, record := range []storage.AvailabilityRecord{
		{Domain: "confirmed.com", Status: availability.AvailableRegistrarConfirmed, Confidence: availability.ConfidenceHighest, CheckedAt: now, ExpiresAt: now.Add(time.Hour)},
		{Domain: "likely.com", Status: availability.AvailableLikely, Confidence: availability.ConfidenceMedium, CheckedAt: now, ExpiresAt: now.Add(time.Hour)},
		{Domain: "registered.com", Status: availability.RegisteredActive, Confidence: availability.ConfidenceHigh, CheckedAt: now, ExpiresAt: now.Add(time.Hour)},
	} {
		if err := store.StoreAvailability(ctx, run.ID, record); err != nil {
			t.Fatal(err)
		}
	}

	output := filepath.Join(t.TempDir(), "Documents", "expired.csv")
	rows, manifest, err := WriteFilteredCSVExport(ctx, store, run.ID, output)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || manifest.RowCount != 2 {
		t.Fatalf("exported rows=%d manifest rows=%d, want 2", len(rows), manifest.RowCount)
	}
	file, err := os.Open(output)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("csv records = %d, want header + 2 rows", len(records))
	}
	got := map[string]bool{records[1][0]: true, records[2][0]: true}
	if !got["confirmed.com"] || !got["likely.com"] || got["registered.com"] {
		t.Fatalf("exported domains = %#v", got)
	}
}

func TestFinishRunCompletesWithWarningWhenExtractionHasFailedFinalTasks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	store := storage.New(dir)
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.CreateRun(ctx, storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"wat"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddCDXRecords(ctx, run.ID, "source.com", "CC-MAIN-2026-04", []commoncrawl.CDXRecord{{
		URLKey: "com,source)/", Timestamp: "20260401000000", URL: "https://source.com/", Mime: "text/html", Status: "200", Digest: "abc", Length: "100", Offset: "0", Filename: "crawl-data/CC-MAIN-2026-04/segments/x/warc/file.warc.gz", RawJSON: "{}",
	}}); err != nil {
		t.Fatal(err)
	}
	runDir, err := store.RunDir(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(runDir, storage.RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := db.QueryRow(`SELECT id FROM wat_tasks WHERE wat_filename <> 'pending-wat-sidecar'`).Scan(&id); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE wat_tasks SET attempts=max_attempts WHERE id=?`, id); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	if err := store.CompleteTask(ctx, run.ID, "wat_tasks", id, false, "stream failed"); err != nil {
		t.Fatal(err)
	}
	engine := Engine{DocumentsDir: t.TempDir()}
	result, err := engine.finishRun(ctx, store, run, "CC-MAIN-2026-04", source, "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Warning == "" || result.Recoverable {
		t.Fatalf("result = %+v, want completed with warning and no resume requirement", result)
	}
}

func TestDocumentsExportPathUsesDocumentsDirectory(t *testing.T) {
	engine := Engine{DocumentsDir: filepath.Join(t.TempDir(), "Documents")}
	path := engine.DocumentsExportPath("example.com", "CC-MAIN-2026-04", "run/id")
	if filepath.Dir(path) != engine.DocumentsDir {
		t.Fatalf("export path dir = %q, want %q", filepath.Dir(path), engine.DocumentsDir)
	}
	if filepath.Base(path) != "edf-expired-domains-example.com-2026-04-run-id.csv" {
		t.Fatalf("export path = %q", path)
	}
}

func TestEngineSnapshotResultsCancelAndFinishRun(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	store := storage.New(dir)
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.CreateRun(ctx, storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddLinkObservations(ctx, run.ID, []extraction.LinkObservation{{
		SourceDomain:      "source.com",
		SourceURL:         "https://source.com/",
		CrawlID:           "CC-MAIN-2026-04",
		OutboundURL:       "https://available.com/",
		RegistrableDomain: "available.com",
		Mode:              "warc",
	}}, cfg); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := store.StoreAvailability(ctx, run.ID, storage.AvailabilityRecord{Domain: "available.com", Status: availability.AvailableLikely, Confidence: availability.ConfidenceMedium, CheckedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	engine := Engine{Config: cfg, Store: store, DocumentsDir: filepath.Join(t.TempDir(), "Documents")}
	snapshot, err := engine.Snapshot(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.RunID != run.ID {
		t.Fatalf("snapshot run = %q, want %q", snapshot.RunID, run.ID)
	}
	results, err := engine.Results(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Domain != "available.com" {
		t.Fatalf("results = %+v", results)
	}
	result, err := engine.finishRun(ctx, store, run, "CC-MAIN-2026-04", source, "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.ExportRows != 1 || result.ExportPath == "" {
		t.Fatalf("finish result = %+v", result)
	}
	run2, err := store.CreateRun(ctx, storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Cancel(ctx, run2.ID); err != nil {
		t.Fatal(err)
	}
	status, err := store.RunStatus(ctx, run2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != "cancelling" {
		t.Fatalf("status = %q, want cancelling", status)
	}
}

func TestEngineStartAndResumeCompleteEmptyCrawl(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/collinfo.json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"CC-MAIN-2026-04","from":"2026-04-01"}]`))
		case strings.HasSuffix(r.URL.Path, "/CC-MAIN-2026-04-index"):
			_, _ = w.Write([]byte(`0`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := config.Default()
	cfg.DataDirectory = t.TempDir()
	cfg.CommonCrawl.IndexBaseURL = server.URL
	cfg.CommonCrawl.CDXPageSize = 100
	engine := NewEngine(cfg)
	engine.DocumentsDir = filepath.Join(t.TempDir(), "Documents")
	engine.ProgressInterval = time.Millisecond
	result, err := engine.Start(context.Background(), RunRequest{Domain: "source.com", Year: 2026, Month: time.April}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.CrawlID != "CC-MAIN-2026-04" || result.ExportRows != 0 {
		t.Fatalf("start result = %+v", result)
	}
	if result.ExportPath == "" {
		t.Fatal("missing export path")
	}
	resumed, err := engine.Resume(context.Background(), result.RunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Status != "completed" || resumed.RunID != result.RunID {
		t.Fatalf("resume result = %+v", resumed)
	}
}

func TestStartSnapshotUpdatesEmitsSnapshotsAndErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	store := storage.New(dir)
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.CreateRun(ctx, storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	updates := make(chan Update, 4)
	stop := startSnapshotUpdates(ctx, store, run.ID, 10*time.Millisecond, updates)
	select {
	case update := <-updates:
		if update.Snapshot.RunID != run.ID {
			t.Fatalf("update = %+v", update)
		}
		stop()
	case <-time.After(time.Second):
		stop()
		t.Fatal("timed out waiting for snapshot update")
	}
}

func TestDrainRunnableTasksProcessesAvailabilityWork(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	store := storage.New(dir)
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.CreateRun(ctx, storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddLinkObservations(ctx, run.ID, []extraction.LinkObservation{
		{SourceDomain: "source.com", SourceURL: "https://source.com/1", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://one.com/", RegistrableDomain: "one.com", Mode: "warc"},
		{SourceDomain: "source.com", SourceURL: "https://source.com/2", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://two.com/", RegistrableDomain: "two.com", Mode: "warc"},
	}, cfg); err != nil {
		t.Fatal(err)
	}
	calls := 0
	err = drainRunnableTasks(ctx, store, run.ID, "availability_tasks", 0, 1, func(batchLimit int) error {
		calls++
		tasks, err := store.LeaseAvailabilityTasks(ctx, run.ID, "test", batchLimit, time.Minute)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		for _, task := range tasks {
			if err := store.StoreAvailability(ctx, run.ID, storage.AvailabilityRecord{Domain: task.Domain, Status: availability.RegisteredActive, Confidence: availability.ConfidenceHigh, CheckedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
				return err
			}
			if err := store.CompleteTask(ctx, run.ID, "availability_tasks", task.ID, true, ""); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("process calls = %d, want 2", calls)
	}
	pending, err := store.RunnableTaskCount(ctx, run.ID, "availability_tasks")
	if err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("pending = %d, want 0", pending)
	}
}

func TestGUIHelperFunctions(t *testing.T) {
	if !onlyCDXRetryableBlockers(map[string]int{"cdx_failed_retryable": 2}) {
		t.Fatal("expected cdx-only blocker to be resumable")
	}
	if onlyCDXRetryableBlockers(map[string]int{"cdx_failed_retryable": 1, "warc_active": 1}) {
		t.Fatal("mixed blockers should not be cdx-only")
	}
	if !hasCancelledBlocker(map[string]int{"warc_cancelled": 1}) {
		t.Fatal("expected cancelled blocker")
	}
	if !isControlledStop(storage.ErrRunPaused) || isControlledStop(errors.New("boom")) {
		t.Fatal("controlled stop classification mismatch")
	}
	if got := taskBatchLimit(2, 8); got != 2 {
		t.Fatalf("taskBatchLimit = %d, want 2", got)
	}
	if got := taskBatchLimit(0, 0); got != 1 {
		t.Fatalf("taskBatchLimit default = %d, want 1", got)
	}
	if got := safeFileComponent("../bad run"); got != "bad-run" {
		t.Fatalf("safe component = %q", got)
	}
	if got := first([]string{"a", "b"}); got != "a" {
		t.Fatalf("first = %q", got)
	}
	if got := first(nil); got != "" {
		t.Fatalf("first nil = %q", got)
	}
	engine := Engine{}
	if got := engine.progressInterval(); got != 10*time.Second {
		t.Fatalf("default interval = %s", got)
	}
	engine.ProgressInterval = time.Second
	if got := engine.progressInterval(); got != time.Second {
		t.Fatalf("custom interval = %s", got)
	}
	_ = progress.PlaceholderSnapshot("run")
}

func TestRegistrarFromEnvAndCommonCrawlClientConfig(t *testing.T) {
	cfg := config.Default()
	provider := "porkbun"
	cfg.Availability.RegistrarProvider = &provider
	t.Setenv("PORKBUN_API_KEY", "key")
	t.Setenv("PORKBUN_SECRET_KEY", "secret")
	if registrarFromEnv(cfg) == nil {
		t.Fatal("expected porkbun provider from env")
	}
	cfg.Runtime.RequestTimeoutSeconds = 1200
	cfg.Runtime.MaxRetryAttempts = 7
	cfg.Runtime.InitialRetryBackoffMillis = 123
	cfg.Runtime.MaxRetryBackoffSeconds = 9
	client := commonCrawlClient(cfg)
	if client.MaxRetries != 7 || client.RetryBackoff != 123*time.Millisecond || client.MaxBackoff != 9*time.Second {
		t.Fatalf("client retry config = %+v", client)
	}
	if client.HTTPClient == nil || client.HTTPClient.Timeout != 0 {
		t.Fatalf("client timeout = %+v, want no Common Crawl HTTP timeout", client.HTTPClient)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
