package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"expired-domain-finder/internal/availability"
	"expired-domain-finder/internal/config"
	"expired-domain-finder/internal/extraction"
	"expired-domain-finder/internal/normalization"
	"expired-domain-finder/internal/storage"

	_ "modernc.org/sqlite"
)

func TestExecuteHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "edf run start") {
		t.Fatalf("help output missing run start: %s", stdout.String())
	}
}

func TestRunStartRequiresMode(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"run", "start", "--source", "example.com", "--crawl", "2026-04"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("code = 0, want validation failure")
	}
	if !strings.Contains(stderr.String(), "requires at least one --mode") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunStartRejectsTooManySources(t *testing.T) {
	args := []string{"run", "start", "--crawl", "CC-MAIN-2026-04", "--mode", "warc"}
	for i := 0; i < 11; i++ {
		args = append(args, "--source", fmt.Sprintf("source-%d.com", i))
	}
	var stdout, stderr bytes.Buffer
	code := Execute(args, &stdout, &stderr)
	if code == 0 {
		t.Fatal("code = 0, want validation failure")
	}
	if !strings.Contains(stderr.String(), "1-10 --source") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunStartRejectsUnsupportedMode(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"run", "start", "--source", "example.com", "--crawl", "CC-MAIN-2026-04", "--mode", "dns"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("code = 0, want validation failure")
	}
	if !strings.Contains(stderr.String(), "unsupported mode") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestValidateRunLimitFlagsRejectsNegativeValues(t *testing.T) {
	if err := validateRunLimitFlags(1, 0, 0, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := validateRunLimitFlags(1, -1, 0, 0, 0, 0); err == nil || !strings.Contains(err.Error(), "--warc-task-limit") {
		t.Fatalf("err = %v, want warc task limit validation", err)
	}
	if err := validateRunLimitFlags(1, 0, 0, 0, 0, -time.Second); err == nil || !strings.Contains(err.Error(), "--max-runtime") {
		t.Fatalf("err = %v, want max runtime validation", err)
	}
}

func TestCommonCrawlClientUsesConstructorDefaults(t *testing.T) {
	cfg := config.Default()
	cfg.CommonCrawl.IndexBaseURL = "https://index.test"
	cfg.CommonCrawl.DataBaseURL = "https://data.test"
	cfg.CommonCrawl.UserAgent = "edf-test"
	client := commonCrawlClient(cfg)
	if client.IndexBaseURL != "https://index.test" || client.DataBaseURL != "https://data.test" || client.UserAgent != "edf-test" {
		t.Fatalf("client = %+v", client)
	}
	if client.HTTPClient == nil {
		t.Fatalf("client HTTP client not configured")
	}
	if client.HTTPClient.Timeout != 0 {
		t.Fatalf("timeout = %s, want no Common Crawl HTTP timeout", client.HTTPClient.Timeout)
	}
	if client.MaxRetries != cfg.Runtime.MaxRetryAttempts ||
		client.RetryBackoff != time.Duration(cfg.Runtime.InitialRetryBackoffMillis)*time.Millisecond ||
		client.MaxBackoff != time.Duration(cfg.Runtime.MaxRetryBackoffSeconds)*time.Second {
		t.Fatalf("retry config not applied: retries=%d backoff=%s max=%s", client.MaxRetries, client.RetryBackoff, client.MaxBackoff)
	}
}

func TestNeedsCDX(t *testing.T) {
	if !needsCDX([]string{"web_graph", "wat"}) {
		t.Fatal("wat mode should need CDX")
	}
	if needsCDX([]string{"web_graph"}) {
		t.Fatal("graph-only mode should not need CDX")
	}
}

func TestGraphReleasesForSelections(t *testing.T) {
	releases := []config.GraphRelease{
		{ID: "graph-apr", Months: []string{"2026-04"}, VertexPath: "vertices-apr", EdgePath: "edges-apr"},
		{ID: "graph-may", CrawlIDs: []string{"CC-MAIN-2026-22"}, VertexPath: "vertices-may", EdgePath: "edges-may"},
	}
	got := graphReleasesForSelections([]string{"2026-04", "CC-MAIN-2026-22"}, releases)
	if len(got) != 2 || got[0].ID != "graph-apr" || got[1].ID != "graph-may" {
		t.Fatalf("releases = %+v", got)
	}
}

func TestGraphReleasesForSelectionsDedupesByReleaseID(t *testing.T) {
	releases := []config.GraphRelease{{ID: "graph-apr", Months: []string{"2026-04"}, CrawlIDs: []string{"CC-MAIN-2026-18"}, VertexPath: "vertices", EdgePath: "edges"}}
	got := graphReleasesForSelections([]string{"2026-04", "CC-MAIN-2026-18"}, releases)
	if len(got) != 1 || got[0].ID != "graph-apr" {
		t.Fatalf("releases = %+v", got)
	}
}

func TestConfigShowRedactsSecrets(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"config", "show"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr=%s", code, stderr.String())
	}
	if strings.Contains(stdout.String(), "apiKey") {
		t.Fatalf("config output should not include API key material: %s", stdout.String())
	}
}

func TestRegistrarFromEnvSupportsConfigRefs(t *testing.T) {
	provider := "porkbun"
	apiRef := "env:EDF_TEST_API_KEY"
	secretRef := "EDF_TEST_SECRET_KEY"
	t.Setenv("PORKBUN_API_KEY", "")
	t.Setenv("PORKBUN_SECRET_KEY", "")
	t.Setenv("EDF_TEST_API_KEY", "api")
	t.Setenv("EDF_TEST_SECRET_KEY", "secret")
	cfg := config.Default()
	cfg.Availability.RegistrarProvider = &provider
	cfg.Availability.RegistrarAPIKeyRef = &apiRef
	cfg.Availability.RegistrarSecretRef = &secretRef
	if registrarFromEnv(cfg) == nil {
		t.Fatal("expected registrar from configured secret refs")
	}
}

func TestRunStartHonorsBlockedSuffixes(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Availability.BlockedSuffixes = []string{"blocked.example"}
	if err := config.Save(config.ConfigPath(dir), cfg); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--data-dir", dir, "run", "start", "--source", "thing.blocked.example", "--crawl", "CC-MAIN-2026-04", "--mode", "warc"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("expected blocked suffix failure, stdout=%s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "blocked suffix") {
		t.Fatalf("stderr = %s", stderr.String())
	}
}

func TestRunStartExecutesForegroundByDefault(t *testing.T) {
	dir := t.TempDir()
	server := newMockWARCServer(t, "https://agency.gov/")
	defer server.Close()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.CommonCrawl.IndexBaseURL = server.URL
	cfg.CommonCrawl.DataBaseURL = server.URL
	if err := config.Save(config.ConfigPath(dir), cfg); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--data-dir", dir, "run", "start", "--source", "source.com", "--crawl", "CC-MAIN-2026-04", "--mode", "warc", "--cdx-limit", "1", "--no-tui"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "execution=foreground") || !strings.Contains(stdout.String(), "status=completed") {
		t.Fatalf("stdout = %s", stdout.String())
	}
	runs, err := storage.New(dir).ListRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Status != "completed" {
		t.Fatalf("runs = %+v", runs)
	}
	results, err := storage.New(dir).Results(context.Background(), runs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Domain != "agency.gov" || results[0].Status != string(availability.RestrictedTLD) {
		t.Fatalf("results = %+v", results)
	}
}

func TestFindAutoInitializesAndWritesDefaultCSVExport(t *testing.T) {
	dir := t.TempDir()
	configDir := t.TempDir()
	server := newMockWARCServer(t, "https://agency.gov/")
	defer server.Close()
	cfg := config.Default()
	cfg.DataDirectory = configDir
	cfg.CommonCrawl.IndexBaseURL = server.URL
	cfg.CommonCrawl.DataBaseURL = server.URL
	cfgPath := config.ConfigPath(configDir)
	if err := config.Save(cfgPath, cfg); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--config", cfgPath, "--data-dir", dir, "find", "--source", "source.com", "--crawl", "CC-MAIN-2026-04", "--mode", "warc", "--cdx-limit", "1", "--quiet"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "cache", storage.CacheDBName)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(config.ConfigPath(dir)); err != nil {
		t.Fatal(err)
	}
	runs, err := storage.New(dir).ListRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Status != "completed" {
		t.Fatalf("runs = %+v", runs)
	}
	output := filepath.Join(dir, "runs", runs[0].ID, "artifacts", "exports", "results.csv")
	if !strings.Contains(stdout.String(), "export="+output) || !strings.Contains(stdout.String(), "agency.gov") {
		t.Fatalf("stdout = %s", stdout.String())
	}
	assertExportArtifactCount(t, dir, runs[0].ID, "csv", 1)
}

func TestFindRecoverablePartialDiscoveryWritesLabeledExport(t *testing.T) {
	dir := t.TempDir()
	payload := "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><a href=\"https://agency.gov/\">target</a></html>"
	warc := "WARC/1.0\r\nWARC-Type: response\r\nContent-Length: " + strconv.Itoa(len(payload)) + "\r\n\r\n" + payload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/collinfo.json":
			http.NotFound(w, r)
		case strings.Contains(r.URL.Path, "CC-MAIN-2026-04-index"):
			pattern := r.URL.Query().Get("url")
			if r.URL.Query().Get("showNumPages") == "true" {
				io.WriteString(w, "1")
				return
			}
			if pattern == "source.com/*" {
				http.Error(w, "temporary common crawl failure", http.StatusServiceUnavailable)
				return
			}
			io.WriteString(w, fmt.Sprintf(`{"urlkey":"com,source,www)/","timestamp":"20260401000000","url":"https://www.source.com/","mime":"text/html","status":"200","digest":"abc","length":"%d","offset":"0","filename":"crawl.warc.gz"}`+"\n", len(warc)))
		case r.URL.Path == "/crawl.warc.gz":
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(warc)-1, len(warc)))
			w.WriteHeader(http.StatusPartialContent)
			io.WriteString(w, warc)
		default:
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
	}))
	defer server.Close()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.CommonCrawl.IndexBaseURL = server.URL
	cfg.CommonCrawl.DataBaseURL = server.URL
	cfg.Runtime.MaxRetryAttempts = 1
	cfg.Runtime.InitialRetryBackoffMillis = 1
	cfg.Runtime.MaxRetryBackoffSeconds = 1
	if err := config.Save(config.ConfigPath(dir), cfg); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--data-dir", dir, "find", "--source", "source.com", "--crawl", "CC-MAIN-2026-04", "--mode", "warc", "--cdx-limit", "0", "--quiet"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	runs, err := storage.New(dir).ListRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Status != "recoverable" {
		t.Fatalf("runs = %+v; stdout=%s", runs, stdout.String())
	}
	if !strings.Contains(stdout.String(), "warning=partial_results") || !strings.Contains(stdout.String(), "resume_command=") || !strings.Contains(stdout.String(), "export=") || !strings.Contains(stdout.String(), "agency.gov") {
		t.Fatalf("stdout = %s", stdout.String())
	}
	assertExportArtifactCount(t, dir, runs[0].ID, "csv", 1)
}

func TestFindExportFormatsAndNoExport(t *testing.T) {
	for _, format := range []string{"json", "parquet"} {
		t.Run(format, func(t *testing.T) {
			dir := t.TempDir()
			server := newMockWARCServer(t, "https://agency.gov/")
			defer server.Close()
			cfg := config.Default()
			cfg.DataDirectory = dir
			cfg.CommonCrawl.IndexBaseURL = server.URL
			cfg.CommonCrawl.DataBaseURL = server.URL
			if err := config.Save(config.ConfigPath(dir), cfg); err != nil {
				t.Fatal(err)
			}
			output := filepath.Join(dir, "custom-results."+format)
			var stdout, stderr bytes.Buffer
			code := Execute([]string{"--data-dir", dir, "find", "--source", "source.com", "--crawl", "CC-MAIN-2026-04", "--mode", "warc", "--cdx-limit", "1", "--quiet", "--export-format", format, "--export-output", output}, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			if _, err := os.Stat(output); err != nil {
				t.Fatal(err)
			}
			runs, err := storage.New(dir).ListRuns(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			assertExportArtifactCount(t, dir, runs[0].ID, format, 1)
		})
	}

	dir := t.TempDir()
	server := newMockWARCServer(t, "https://agency.gov/")
	defer server.Close()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.CommonCrawl.IndexBaseURL = server.URL
	cfg.CommonCrawl.DataBaseURL = server.URL
	if err := config.Save(config.ConfigPath(dir), cfg); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--data-dir", dir, "find", "--source", "source.com", "--crawl", "CC-MAIN-2026-04", "--mode", "warc", "--cdx-limit", "1", "--quiet", "--no-export"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	runs, err := storage.New(dir).ListRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertExportArtifactCount(t, dir, runs[0].ID, "", 0)
}

func TestFindFailureMarksRunFailedWithoutExport(t *testing.T) {
	dir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/graphinfo.json":
			io.WriteString(w, "[]")
		default:
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
	}))
	defer server.Close()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.CommonCrawl.IndexBaseURL = server.URL
	if err := config.Save(config.ConfigPath(dir), cfg); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--data-dir", dir, "find", "--source", "source.com", "--crawl", "CC-MAIN-2026-04", "--mode", "web_graph", "--quiet"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("code=0, want failure; stdout=%s", stdout.String())
	}
	runs, err := storage.New(dir).ListRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Status != "failed" {
		t.Fatalf("runs = %+v, stderr=%s", runs, stderr.String())
	}
	assertExportArtifactCount(t, dir, runs[0].ID, "", 0)
}

func TestRunStartCreateOnlyDoesNotLeaveRunningRun(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	if err := config.Save(config.ConfigPath(dir), cfg); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--data-dir", dir, "run", "start", "--source", "source.com", "--crawl", "CC-MAIN-2026-04", "--mode", "warc", "--create-only"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "execution=create-only") || strings.Contains(stdout.String(), "status=running") {
		t.Fatalf("stdout = %s", stdout.String())
	}
	runs, err := storage.New(dir).ListRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Status != "created" {
		t.Fatalf("runs = %+v", runs)
	}
}

func TestRunStartFailureMarksRunFailed(t *testing.T) {
	dir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/graphinfo.json":
			io.WriteString(w, "[]")
		default:
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
	}))
	defer server.Close()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.CommonCrawl.IndexBaseURL = server.URL
	if err := config.Save(config.ConfigPath(dir), cfg); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--data-dir", dir, "run", "start", "--source", "source.com", "--crawl", "CC-MAIN-2026-04", "--mode", "web_graph", "--no-tui"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("code=0, want failure; stdout=%s", stdout.String())
	}
	runs, err := storage.New(dir).ListRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Status != "failed" {
		t.Fatalf("runs = %+v, stderr=%s", runs, stderr.String())
	}
}

func TestWriteParquetExport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.parquet")
	err := writeExport(path, "parquet", []storage.Candidate{{
		Domain:             "candidate.example",
		Status:             "available_registrar_confirmed",
		Confidence:         "highest",
		TLDPolicyCategory:  "open_retail",
		EvidenceModes:      "warc",
		ManualVerification: "yes",
		Score:              1200,
	}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 4 || string(data[:4]) != "PAR1" {
		t.Fatalf("missing parquet magic bytes: %q", data[:min(len(data), 4)])
	}
}

func TestExportCommandRecordsArtifactManifest(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddLinkObservations(context.Background(), run.ID, []extraction.LinkObservation{{SourceDomain: "source.com", SourceURL: "https://source.com/", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://target.com/", RegistrableDomain: "target.com", Mode: "warc"}}, cfg); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := store.StoreAvailability(context.Background(), run.ID, storage.AvailabilityRecord{Domain: "target.com", Status: availability.AvailableLikely, Confidence: availability.ConfidenceMedium, TLDPolicyCategory: "open_retail", CheckedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "runs", run.ID, "artifacts", "exports", "results.csv")
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--data-dir", dir, "export", run.ID, "--format", "csv", "--output", output}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	file, err := os.Open(output)
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(file).ReadAll()
	closeErr := file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if len(records) != 2 {
		t.Fatalf("csv records = %d, want 2", len(records))
	}
	header := strings.Join(records[0], ",")
	for _, field := range []string{"source_page_count", "graph_edge_count", "outbound_url_count"} {
		if !strings.Contains(header, field) {
			t.Fatalf("csv header missing %s: %s", field, header)
		}
	}
	if records[1][4] != "1" || records[1][5] != "0" || records[1][6] != "1" {
		t.Fatalf("csv evidence counts = %v, want source=1 graph=0 outbound=1", records[1][4:7])
	}
	runDir, err := store.RunDir(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(runDir, storage.RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var artifacts int
	if err := db.QueryRow(`SELECT COUNT(*) FROM artifact_files WHERE artifact_type='export' AND format='csv' AND status='sealed'`).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if artifacts != 1 {
		t.Fatalf("export artifacts = %d, want 1", artifacts)
	}
	var complete, total int
	if err := db.QueryRow(`SELECT completed_units,total_known_units FROM progress_counters WHERE stage='export'`).Scan(&complete, &total); err != nil {
		t.Fatal(err)
	}
	if complete != 1 || total != 1 {
		t.Fatalf("export progress = %d/%d, want 1/1", complete, total)
	}
	stdout.Reset()
	stderr.Reset()
	code = Execute([]string{"--data-dir", dir, "export", run.ID, "--format", "csv", "--output", output}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("second export code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM artifact_files WHERE artifact_type='export' AND format='csv' AND status='sealed'`).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if artifacts != 1 {
		t.Fatalf("export artifacts after regeneration = %d, want 1", artifacts)
	}
}

func TestDrainRunnableTasksProcessesMultipleBatches(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	observations := []extraction.LinkObservation{
		{SourceDomain: "source.com", SourceURL: "https://source.com/a", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://a-target.com/", RegistrableDomain: "a-target.com", Mode: "warc"},
		{SourceDomain: "source.com", SourceURL: "https://source.com/b", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://b-target.com/", RegistrableDomain: "b-target.com", Mode: "warc"},
	}
	if err := store.AddLinkObservations(context.Background(), run.ID, observations, cfg); err != nil {
		t.Fatal(err)
	}
	calls := 0
	err = drainRunnableTasks(context.Background(), store, run.ID, "availability_tasks", func() error {
		tasks, err := store.LeaseAvailabilityTasks(context.Background(), run.ID, "test", 1, time.Minute)
		if err != nil {
			return err
		}
		if len(tasks) != 1 {
			t.Fatalf("leased tasks = %d, want 1", len(tasks))
		}
		task := tasks[0]
		if err := store.StartTask(context.Background(), run.ID, "availability_tasks", task.ID); err != nil {
			return err
		}
		now := time.Now().UTC()
		if err := store.StoreAvailability(context.Background(), run.ID, storage.AvailabilityRecord{Domain: task.Domain, Status: availability.AvailableLikely, Confidence: availability.ConfidenceMedium, TLDPolicyCategory: "open_retail", CheckedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
			return err
		}
		calls++
		return store.CompleteTask(context.Background(), run.ID, "availability_tasks", task.ID, true, "")
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("process calls = %d, want 2", calls)
	}
}

func TestDrainRunnableTasksWithLimitStopsAfterBudget(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	observations := []extraction.LinkObservation{
		{SourceDomain: "source.com", SourceURL: "https://source.com/a", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://a-target.com/", RegistrableDomain: "a-target.com", Mode: "warc"},
		{SourceDomain: "source.com", SourceURL: "https://source.com/b", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://b-target.com/", RegistrableDomain: "b-target.com", Mode: "warc"},
	}
	if err := store.AddLinkObservations(context.Background(), run.ID, observations, cfg); err != nil {
		t.Fatal(err)
	}
	calls := 0
	err = drainRunnableTasksWithLimit(context.Background(), store, run.ID, "availability_tasks", 1, 1, func(batchLimit int) error {
		if batchLimit != 1 {
			t.Fatalf("batchLimit = %d, want 1", batchLimit)
		}
		tasks, err := store.LeaseAvailabilityTasks(context.Background(), run.ID, "test", batchLimit, time.Minute)
		if err != nil {
			return err
		}
		if len(tasks) != 1 {
			t.Fatalf("leased tasks = %d, want 1", len(tasks))
		}
		task := tasks[0]
		if err := store.StartTask(context.Background(), run.ID, "availability_tasks", task.ID); err != nil {
			return err
		}
		now := time.Now().UTC()
		if err := store.StoreAvailability(context.Background(), run.ID, storage.AvailabilityRecord{Domain: task.Domain, Status: availability.AvailableLikely, Confidence: availability.ConfidenceMedium, TLDPolicyCategory: "open_retail", CheckedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
			return err
		}
		calls++
		return store.CompleteTask(context.Background(), run.ID, "availability_tasks", task.ID, true, "")
	})
	if !errors.Is(err, errWorkBudgetReached) {
		t.Fatalf("err = %v, want errWorkBudgetReached", err)
	}
	pending, err := store.RunnableTaskCount(context.Background(), run.ID, "availability_tasks")
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || pending != 1 {
		t.Fatalf("calls=%d pending=%d, want one processed and one left pending", calls, pending)
	}
}

func TestTaskBatchLimitUsesConcurrencyForUnboundedRuns(t *testing.T) {
	if got := taskBatchLimit(0, 48); got != 48 {
		t.Fatalf("batch = %d, want 48", got)
	}
	if got := taskBatchLimit(3, 48); got != 3 {
		t.Fatalf("batch = %d, want 3", got)
	}
	if got := taskBatchLimit(0, 0); got != 1 {
		t.Fatalf("batch = %d, want 1", got)
	}
}

func TestFinishIncompleteRunDoesNotCompleteOrExportCancelledWork(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	observations := []extraction.LinkObservation{{SourceDomain: "source.com", SourceURL: "https://source.com/", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://target.com/", RegistrableDomain: "target.com", Mode: "warc"}}
	if err := store.AddLinkObservations(context.Background(), run.ID, observations, cfg); err != nil {
		t.Fatal(err)
	}
	if err := store.SetRunStatus(context.Background(), run.ID, "cancelling"); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code, handled := finishIncompleteRunIfNeeded(store, run.ID, &stdout, &stderr, false, true)
	if code != 0 || !handled {
		t.Fatalf("code=%d handled=%v stderr=%s stdout=%s", code, handled, stderr.String(), stdout.String())
	}
	status, err := store.RunStatus(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != "cancelled" {
		t.Fatalf("status = %s, want cancelled", status)
	}
	if !strings.Contains(stdout.String(), "execution=cancelled") || strings.Contains(stdout.String(), "status=completed") {
		t.Fatalf("stdout = %s", stdout.String())
	}
	assertExportArtifactCount(t, dir, run.ID, "", 0)
}

func TestResumeCommandExecutesForegroundByDefault(t *testing.T) {
	dir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/collinfo.json":
			http.NotFound(w, r)
		case strings.Contains(r.URL.Path, "CC-MAIN-2026-04-index"):
			if r.URL.Query().Get("showNumPages") == "true" {
				io.WriteString(w, "0")
				return
			}
			io.WriteString(w, "")
		default:
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
	}))
	defer server.Close()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.CommonCrawl.IndexBaseURL = server.URL
	cfg.CommonCrawl.DataBaseURL = server.URL
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg, InitialStatus: "created"})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--data-dir", dir, "run", "resume", run.ID}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	status, err := store.RunStatus(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("status = %s, want completed; stdout=%s stderr=%s", status, stdout.String(), stderr.String())
	}
}

func TestRunStatusWithoutIDListsRuns(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--data-dir", dir, "run", "status"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), run.ID) || !strings.Contains(stdout.String(), "running") {
		t.Fatalf("stdout = %s", stdout.String())
	}
}

func TestRunWatchTicksOnce(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--data-dir", dir, "run", "watch", run.ID, "--ticks", "1", "--interval", "1ms"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), run.ID) || strings.Contains(stdout.String(), "\x1b[") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunWatchControlPauseResumeStop(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--data-dir", dir, "run", "watch", run.ID, "--control", "p"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("pause code=%d stderr=%s", code, stderr.String())
	}
	status, err := store.RunStatus(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != "pause_requested" {
		t.Fatalf("pause status = %s", status)
	}
	stdout.Reset()
	stderr.Reset()
	code = Execute([]string{"--data-dir", dir, "run", "watch", run.ID, "--control", "p"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("resume code=%d stderr=%s", code, stderr.String())
	}
	status, err = store.RunStatus(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != "resume_requested" {
		t.Fatalf("resume status = %s", status)
	}
	stdout.Reset()
	stderr.Reset()
	code = Execute([]string{"--data-dir", dir, "run", "watch", run.ID, "--control", "s"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("stop code=%d stderr=%s", code, stderr.String())
	}
	status, err = store.RunStatus(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != "cancelling" {
		t.Fatalf("stop status = %s", status)
	}
}

func TestRunWatchReadsStdinControls(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := ExecuteWithInput([]string{"--data-dir", dir, "run", "watch", run.ID, "--interval", "1s"}, &stdout, &stderr, strings.NewReader("p\nq\n"))
	if code != 0 {
		t.Fatalf("code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	status, err := store.RunStatus(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status != "pause_requested" {
		t.Fatalf("status = %s, want pause_requested", status)
	}
	if !strings.Contains(stdout.String(), "pause requested") {
		t.Fatalf("stdout = %s", stdout.String())
	}
}

func TestStartProgressPrinterRendersSnapshot(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	stop := startProgressPrinter(store, run.ID, &stdout, time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	stop()
	if !strings.Contains(stdout.String(), run.ID) {
		t.Fatalf("progress output = %q", stdout.String())
	}
}

func newMockWARCServer(t *testing.T, outboundURL string) *httptest.Server {
	t.Helper()
	payload := "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><a href=\"" + outboundURL + "\">target</a></html>"
	warc := "WARC/1.0\r\nWARC-Type: response\r\nContent-Length: " + strconv.Itoa(len(payload)) + "\r\n\r\n" + payload
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/collinfo.json":
			http.NotFound(w, r)
		case strings.Contains(r.URL.Path, "CC-MAIN-2026-04-index"):
			if r.URL.Query().Get("showNumPages") == "true" {
				if r.URL.Query().Get("url") == "*.source.com/*" {
					io.WriteString(w, "0")
					return
				}
				io.WriteString(w, "1")
				return
			}
			io.WriteString(w, fmt.Sprintf(`{"urlkey":"com,source)/","timestamp":"20260401000000","url":"https://source.com/","mime":"text/html","status":"200","digest":"abc","length":"%d","offset":"0","filename":"crawl.warc.gz"}`+"\n", len(warc)))
		case r.URL.Path == "/crawl.warc.gz":
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(warc)-1, len(warc)))
			w.WriteHeader(http.StatusPartialContent)
			io.WriteString(w, warc)
		default:
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
	}))
}

func assertExportArtifactCount(t *testing.T, dir, runID, format string, want int) {
	t.Helper()
	runDir, err := storage.New(dir).RunDir(runID)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(runDir, storage.RunDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	query := `SELECT COUNT(*) FROM artifact_files WHERE artifact_type='export' AND status='sealed'`
	args := []any{}
	if format != "" {
		query += ` AND format=?`
		args = append(args, format)
	}
	var got int
	if err := db.QueryRow(query, args...).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("export artifacts = %d, want %d", got, want)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
