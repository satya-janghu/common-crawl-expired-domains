package workflow

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"expired-domain-finder/internal/availability"
	"expired-domain-finder/internal/commoncrawl"
	"expired-domain-finder/internal/config"
	"expired-domain-finder/internal/extraction"
	"expired-domain-finder/internal/normalization"
	"expired-domain-finder/internal/storage"

	_ "modernc.org/sqlite"
)

func TestProcessWARCLeasesFetchesExtractsAndPersists(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Concurrency.WARC = 2
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	payload := "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><a href=\"https://target.com/\">Target</a></html>"
	warc := "WARC/1.0\r\nWARC-Type: response\r\nContent-Length: " + strconv.Itoa(len(payload)) + "\r\n\r\n" + payload
	if err := store.AddCDXRecords(context.Background(), run.ID, "source.com", "CC-MAIN-2026-04", []commoncrawl.CDXRecord{{
		URLKey: "com,source)/", Timestamp: "20260401000000", URL: "https://source.com/", Mime: "text/html", Status: "200", Digest: "abc", Length: strconv.Itoa(len(warc)), Offset: "0", Filename: "crawl.warc.gz", RawJSON: "{}",
	}}); err != nil {
		t.Fatal(err)
	}
	client := commoncrawl.Client{
		DataBaseURL: "https://data.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusPartialContent, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader([]byte(warc)))}, nil
		})},
	}
	err = (Runner{Store: store, Config: cfg, CommonCrawl: client, WorkerID: "test"}).ProcessWARC(context.Background(), run.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Domain != "target.com" || results[0].EvidenceModes != "warc" {
		t.Fatalf("results = %+v", results)
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
	var state string
	if err := db.QueryRow(`SELECT state FROM warc_tasks WHERE filename='crawl.warc.gz'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "succeeded" {
		t.Fatalf("task state = %s, want succeeded", state)
	}
	var artifacts int
	if err := db.QueryRow(`SELECT COUNT(*) FROM artifact_files WHERE artifact_type='link_observations' AND format='parquet' AND compression='zstd' AND status='sealed'`).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if artifacts != 1 {
		t.Fatalf("artifacts = %d, want 1", artifacts)
	}
}

func TestProcessWARCRetryDoesNotDuplicateObservationBatch(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Concurrency.WARC = 1
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	payload := "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><a href=\"https://target.com/\">Target</a></html>"
	warc := "WARC/1.0\r\nWARC-Type: response\r\nContent-Length: " + strconv.Itoa(len(payload)) + "\r\n\r\n" + payload
	if err := store.AddCDXRecords(context.Background(), run.ID, "source.com", "CC-MAIN-2026-04", []commoncrawl.CDXRecord{{
		URLKey: "com,source)/retry", Timestamp: "20260401000000", URL: "https://source.com/retry", Mime: "text/html", Status: "200", Digest: "retry", Length: strconv.Itoa(len(warc)), Offset: "0", Filename: "crawl-retry.warc.gz", RawJSON: "{}",
	}}); err != nil {
		t.Fatal(err)
	}
	client := commoncrawl.Client{
		DataBaseURL: "https://data.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusPartialContent, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader([]byte(warc)))}, nil
		})},
	}
	runner := Runner{Store: store, Config: cfg, CommonCrawl: client, WorkerID: "test"}
	if err := runner.ProcessWARC(context.Background(), run.ID, 10); err != nil {
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
	defer db.Close()
	if _, err := db.Exec(`UPDATE warc_tasks SET state='failed_retryable', lease_owner=NULL, leased_until=NULL WHERE filename='crawl-retry.warc.gz'`); err != nil {
		t.Fatal(err)
	}
	if err := runner.ProcessWARC(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	var sourcePages, outboundURLs, sourceSeen, progressComplete, artifacts, batches int
	if err := db.QueryRow(`SELECT source_page_count,outbound_url_count FROM candidate_domains WHERE domain='target.com'`).Scan(&sourcePages, &outboundURLs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT seen_count FROM candidate_sources WHERE domain='target.com'`).Scan(&sourceSeen); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT completed_units FROM progress_counters WHERE stage='extraction'`).Scan(&progressComplete); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM artifact_files WHERE artifact_type='link_observations' AND status='sealed'`).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM observation_batches`).Scan(&batches); err != nil {
		t.Fatal(err)
	}
	if sourcePages != 1 || outboundURLs != 1 || sourceSeen != 1 || progressComplete != 1 || artifacts != 1 || batches != 1 {
		t.Fatalf("counts source=%d outbound=%d seen=%d progress=%d artifacts=%d batches=%d, want all 1", sourcePages, outboundURLs, sourceSeen, progressComplete, artifacts, batches)
	}
}

func TestProcessWARCTruncatedRecordFailsFinal(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Concurrency.WARC = 1
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	payload := "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html>"
	warc := "WARC/1.0\r\nWARC-Type: response\r\nContent-Length: " + strconv.Itoa(len(payload)+10) + "\r\n\r\n" + payload
	if err := store.AddCDXRecords(context.Background(), run.ID, "source.com", "CC-MAIN-2026-04", []commoncrawl.CDXRecord{{
		URLKey: "com,source)/truncated", Timestamp: "20260401000000", URL: "https://source.com/truncated", Mime: "text/html", Status: "200", Digest: "truncated", Length: strconv.Itoa(len(warc)), Offset: "0", Filename: "truncated.warc.gz", RawJSON: "{}",
	}}); err != nil {
		t.Fatal(err)
	}
	client := commoncrawl.Client{
		DataBaseURL: "https://data.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusPartialContent, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader([]byte(warc)))}, nil
		})},
	}
	err = (Runner{Store: store, Config: cfg, CommonCrawl: client, WorkerID: "test"}).ProcessWARC(context.Background(), run.ID, 10)
	if err != nil {
		t.Fatalf("err = %v, want nil final parse failure", err)
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
	var state string
	if err := db.QueryRow(`SELECT state FROM warc_tasks WHERE filename='truncated.warc.gz'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "succeeded" {
		t.Fatalf("state = %s, want succeeded", state)
	}
}

func TestProcessWARCRangeForbiddenFailsFinalAndContinues(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Concurrency.WARC = 1
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddCDXRecords(context.Background(), run.ID, "source.com", "CC-MAIN-2026-04", []commoncrawl.CDXRecord{{
		URLKey: "com,source)/forbidden", Timestamp: "20260401000000", URL: "https://source.com/forbidden", Mime: "text/html", Status: "200", Digest: "forbidden", Length: "100", Offset: "0", Filename: "forbidden.warc.gz", RawJSON: "{}",
	}}); err != nil {
		t.Fatal(err)
	}
	client := commoncrawl.Client{
		DataBaseURL: "https://data.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusForbidden, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
		})},
	}
	if err := (Runner{Store: store, Config: cfg, CommonCrawl: client, WorkerID: "test"}).ProcessWARC(context.Background(), run.ID, 10); err != nil {
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
	defer db.Close()
	var state string
	if err := db.QueryRow(`SELECT state FROM warc_tasks WHERE filename='forbidden.warc.gz'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "succeeded" {
		t.Fatalf("state = %s, want succeeded", state)
	}
	var errors int
	if err := db.QueryRow(`SELECT COUNT(*) FROM errors WHERE task_table='warc_tasks' AND retryable=0`).Scan(&errors); err != nil {
		t.Fatal(err)
	}
	if errors != 0 {
		t.Fatalf("errors = %d, want 0", errors)
	}
}

func TestDiscoverCDXPersistsResolvedCrawlSelection(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	client := commoncrawl.Client{
		IndexBaseURL: "https://index.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.URL.Path == "/collinfo.json":
				body := `[{"id":"CC-MAIN-2026-18","from":"2026-04-01T00:00:00Z","to":"2026-04-30T23:59:59Z","cdx-api":"https://index.test/CC-MAIN-2026-18-index"}]`
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			case strings.Contains(req.URL.Path, "CC-MAIN-2026-18-index"):
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
			default:
				t.Fatalf("unexpected URL: %s", req.URL.String())
				return nil, nil
			}
		})},
	}
	if err := (Runner{Store: store, Config: cfg, CommonCrawl: client}).DiscoverCDX(context.Background(), run.ID, []string{"source.com"}, []string{"2026-04"}, 10); err != nil {
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

func TestDiscoverCDXRequiresCatalogForMonthSelection(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	client := commoncrawl.Client{
		IndexBaseURL: "https://index.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != "/collinfo.json" {
				t.Fatalf("unexpected CDX request before month resolution: %s", req.URL.String())
			}
			return &http.Response{StatusCode: http.StatusInternalServerError, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
		})},
		MaxRetries:   0,
		RetryBackoff: time.Nanosecond,
	}
	err = (Runner{Store: store, Config: cfg, CommonCrawl: client}).DiscoverCDX(context.Background(), run.ID, []string{"source.com"}, []string{"2026-04"}, 10)
	if err == nil || !strings.Contains(err.Error(), "resolve crawl months") {
		t.Fatalf("err = %v, want month resolution failure", err)
	}
}

func TestDiscoverCDXResumeSkipsSucceededPagesAndDoesNotDuplicateRecords(t *testing.T) {
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
	var pageFetches int32
	client := commoncrawl.Client{
		IndexBaseURL: "https://index.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path == "/collinfo.json" {
				return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
			}
			if req.URL.Query().Get("showNumPages") == "true" {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("1"))}, nil
			}
			atomic.AddInt32(&pageFetches, 1)
			body := `{"urlkey":"com,source)/","timestamp":"20260401000000","url":"https://source.com/","mime":"text/html","status":"200","digest":"abc","length":"100","offset":"20","filename":"crawl.warc.gz"}` + "\n"
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		})},
	}
	runner := Runner{Store: store, Config: cfg, CommonCrawl: client}
	if err := runner.DiscoverCDX(context.Background(), run.ID, []string{"source.com"}, []string{"CC-MAIN-2026-04"}, 10); err != nil {
		t.Fatal(err)
	}
	if err := runner.DiscoverCDX(context.Background(), run.ID, []string{"source.com"}, []string{"CC-MAIN-2026-04"}, 10); err != nil {
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
	defer db.Close()
	var records, tasks, succeededPages int
	if err := db.QueryRow(`SELECT COUNT(*) FROM cdx_records`).Scan(&records); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM warc_tasks WHERE filename='crawl.warc.gz'`).Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM cdx_discovery_pages WHERE state='succeeded'`).Scan(&succeededPages); err != nil {
		t.Fatal(err)
	}
	if records != 1 || tasks != 1 || succeededPages != 2 {
		t.Fatalf("records=%d tasks=%d pages=%d, want 1/1/2", records, tasks, succeededPages)
	}
	if pageFetches != 2 {
		t.Fatalf("page fetches = %d, want 2 because second discovery skips both succeeded pages", pageFetches)
	}
}

func TestDiscoverCDXContinuesAfterTransientPageFailure(t *testing.T) {
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
	client := commoncrawl.Client{
		IndexBaseURL: "https://index.test",
		MaxRetries:   0,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path == "/collinfo.json" {
				return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
			}
			pattern := req.URL.Query().Get("url")
			if req.URL.Query().Get("showNumPages") == "true" {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("1"))}, nil
			}
			if pattern == "source.com/*" {
				return nil, context.DeadlineExceeded
			}
			body := `{"urlkey":"com,source,www)/","timestamp":"20260401000000","url":"https://www.source.com/","mime":"text/html","status":"200","digest":"abc","length":"100","offset":"20","filename":"crawl.warc.gz"}` + "\n"
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		})},
	}
	if err := (Runner{Store: store, Config: cfg, CommonCrawl: client}).DiscoverCDX(context.Background(), run.ID, []string{"source.com"}, []string{"CC-MAIN-2026-04"}, 0); err != nil {
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
	defer db.Close()
	var failed, succeeded, records int
	if err := db.QueryRow(`SELECT COUNT(*) FROM cdx_discovery_pages WHERE state='failed_retryable'`).Scan(&failed); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM cdx_discovery_pages WHERE state='succeeded'`).Scan(&succeeded); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM cdx_records`).Scan(&records); err != nil {
		t.Fatal(err)
	}
	if failed != 1 || succeeded != 1 || records != 1 {
		t.Fatalf("failed=%d succeeded=%d records=%d, want 1/1/1", failed, succeeded, records)
	}
}

func TestDiscoverCDXUsesBoundedConcurrency(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Concurrency.CDX = 2
	sourceA, err := normalization.NormalizeSource("a.com")
	if err != nil {
		t.Fatal(err)
	}
	sourceB, err := normalization.NormalizeSource("b.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{sourceA, sourceB}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	var active, maxActive int32
	var bothActiveOnce sync.Once
	bothActive := make(chan struct{})
	client := commoncrawl.Client{
		IndexBaseURL: "https://index.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path == "/collinfo.json" {
				return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
			}
			current := atomic.AddInt32(&active, 1)
			for {
				previous := atomic.LoadInt32(&maxActive)
				if current <= previous || atomic.CompareAndSwapInt32(&maxActive, previous, current) {
					break
				}
			}
			if current >= 2 {
				bothActiveOnce.Do(func() { close(bothActive) })
			} else {
				select {
				case <-bothActive:
				case <-time.After(200 * time.Millisecond):
				}
			}
			atomic.AddInt32(&active, -1)
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
		})},
	}
	if err := (Runner{Store: store, Config: cfg, CommonCrawl: client}).DiscoverCDX(context.Background(), run.ID, []string{"a.com", "b.com"}, []string{"CC-MAIN-2026-04"}, 10); err != nil {
		t.Fatal(err)
	}
	if maxActive < 2 {
		t.Fatalf("max active CDX requests = %d, want at least 2", maxActive)
	}
}

func TestProcessWATLeasesFetchesExtractsAndPersists(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Concurrency.WAT = 2
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"wat"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddCDXRecords(context.Background(), run.ID, "source.com", "CC-MAIN-2026-04", []commoncrawl.CDXRecord{{
		URLKey: "com,source)/", Timestamp: "20260401000000", URL: "https://source.com/", Mime: "text/html", Status: "200", Digest: "abc", Length: "100", Offset: "0", Filename: "crawl-data/CC-MAIN-2026-04/segments/x/warc/file.warc.gz", RawJSON: "{}",
	}}); err != nil {
		t.Fatal(err)
	}
	wat := `{"Envelope":{"WARC-Header-Metadata":{"WARC-Target-URI":"https://source.com/page","WARC-Date":"2026-04-01T00:00:00Z"},"Payload-Metadata":{"HTTP-Response-Metadata":{"HTML-Metadata":{"Links":[{"path":"A@/href","url":"https://wat-target.com/","text":"Target"}]}}}}}` + "\n"
	client := commoncrawl.Client{
		DataBaseURL: "https://data.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if !strings.Contains(req.URL.Path, "/wat/") {
				t.Fatalf("expected WAT path, got %s", req.URL.Path)
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader([]byte(wat)))}, nil
		})},
	}
	err = (Runner{Store: store, Config: cfg, CommonCrawl: client, WorkerID: "test"}).ProcessWAT(context.Background(), run.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Domain != "wat-target.com" || results[0].EvidenceModes != "wat" {
		t.Fatalf("results = %+v", results)
	}
}

func TestProcessWATTransientReadFailureIsRetryableAndNonFatal(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Concurrency.WAT = 1
	cfg.Runtime.InitialRetryBackoffMillis = 1
	cfg.Runtime.MaxRetryBackoffSeconds = 1
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"wat"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddCDXRecords(context.Background(), run.ID, "source.com", "CC-MAIN-2026-04", []commoncrawl.CDXRecord{{
		URLKey: "com,source)/", Timestamp: "20260401000000", URL: "https://source.com/", Mime: "text/html", Status: "200", Digest: "abc", Length: "100", Offset: "0", Filename: "crawl-data/CC-MAIN-2026-04/segments/x/warc/file.warc.gz", RawJSON: "{}",
	}}); err != nil {
		t.Fatal(err)
	}
	wat := `{"Envelope":{"WARC-Header-Metadata":{"WARC-Target-URI":"https://source.com/page","WARC-Date":"2026-04-01T00:00:00Z"},"Payload-Metadata":{"HTTP-Response-Metadata":{"HTML-Metadata":{"Links":[{"path":"A@/href","url":"https://wat-target.com/","text":"Target"}]}}}}}` + "\n"
	var calls int32
	client := commoncrawl.Client{
		DataBaseURL: "https://data.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			if atomic.AddInt32(&calls, 1) == 1 {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(errorReader{err: context.DeadlineExceeded})}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(wat))}, nil
		})},
	}
	runner := Runner{Store: store, Config: cfg, CommonCrawl: client, WorkerID: "test"}
	if err := runner.ProcessWAT(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	if err := runner.ProcessWAT(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Domain != "wat-target.com" {
		t.Fatalf("results = %+v", results)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestProcessWATSkipsMalformedRecordsAndPersistsValidLinks(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Concurrency.WAT = 1
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"wat"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddCDXRecords(context.Background(), run.ID, "source.com", "CC-MAIN-2026-04", []commoncrawl.CDXRecord{{
		URLKey: "com,source)/", Timestamp: "20260401000000", URL: "https://source.com/", Mime: "text/html", Status: "200", Digest: "abc", Length: "100", Offset: "0", Filename: "crawl-data/CC-MAIN-2026-04/segments/x/warc/file.warc.gz", RawJSON: "{}",
	}}); err != nil {
		t.Fatal(err)
	}
	valid := `{"Envelope":{"WARC-Header-Metadata":{"WARC-Target-URI":"https://source.com/page","WARC-Date":"2026-04-01T00:00:00Z"},"Payload-Metadata":{"HTTP-Response-Metadata":{"HTML-Metadata":{"Links":[{"path":"A@/href","url":"https://valid-wat.com/","text":"Target"}]}}}}}`
	client := commoncrawl.Client{
		DataBaseURL: "https://data.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{broken\n" + valid + "\n"))}, nil
		})},
	}
	if err := (Runner{Store: store, Config: cfg, CommonCrawl: client, WorkerID: "test"}).ProcessWAT(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Domain != "valid-wat.com" {
		t.Fatalf("results = %+v", results)
	}
}

func TestProcessWATRetryDoesNotDuplicateObservationBatch(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Concurrency.WAT = 1
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"wat"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddCDXRecords(context.Background(), run.ID, "source.com", "CC-MAIN-2026-04", []commoncrawl.CDXRecord{{
		URLKey: "com,source)/", Timestamp: "20260401000000", URL: "https://source.com/", Mime: "text/html", Status: "200", Digest: "abc", Length: "100", Offset: "0", Filename: "crawl-data/CC-MAIN-2026-04/segments/x/warc/file.warc.gz", RawJSON: "{}",
	}}); err != nil {
		t.Fatal(err)
	}
	wat := `{"Envelope":{"WARC-Header-Metadata":{"WARC-Target-URI":"https://source.com/page","WARC-Date":"2026-04-01T00:00:00Z"},"Payload-Metadata":{"HTTP-Response-Metadata":{"HTML-Metadata":{"Links":[{"path":"A@/href","url":"https://wat-target.com/","text":"Target"}]}}}}}` + "\n"
	client := commoncrawl.Client{
		DataBaseURL: "https://data.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(wat))}, nil
		})},
	}
	runner := Runner{Store: store, Config: cfg, CommonCrawl: client, WorkerID: "test"}
	if err := runner.ProcessWAT(context.Background(), run.ID, 10); err != nil {
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
	defer db.Close()
	if _, err := db.Exec(`UPDATE wat_tasks SET state='failed_retryable', lease_owner=NULL, leased_until=NULL WHERE wat_filename <> 'pending-wat-sidecar'`); err != nil {
		t.Fatal(err)
	}
	if err := runner.ProcessWAT(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	var sourcePages, outboundURLs, sourceSeen, artifacts, batches int
	if err := db.QueryRow(`SELECT source_page_count,outbound_url_count FROM candidate_domains WHERE domain='wat-target.com'`).Scan(&sourcePages, &outboundURLs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT seen_count FROM candidate_sources WHERE domain='wat-target.com'`).Scan(&sourceSeen); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM artifact_files WHERE artifact_type='link_observations' AND status='sealed'`).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM observation_batches WHERE batch_key LIKE 'wat:%'`).Scan(&batches); err != nil {
		t.Fatal(err)
	}
	if sourcePages != 1 || outboundURLs != 1 || sourceSeen != 1 || artifacts != 1 || batches != 1 {
		t.Fatalf("counts source=%d outbound=%d seen=%d artifacts=%d batches=%d, want all 1", sourcePages, outboundURLs, sourceSeen, artifacts, batches)
	}
}

func TestProcessWATMissingSidecarFailsTaskFinalAndContinues(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Concurrency.WAT = 1
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"wat"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddCDXRecords(context.Background(), run.ID, "source.com", "CC-MAIN-2026-04", []commoncrawl.CDXRecord{{
		URLKey: "com,source)/", Timestamp: "20260401000000", URL: "https://source.com/", Mime: "text/html", Status: "200", Digest: "abc", Length: "100", Offset: "0", Filename: "crawl-data/CC-MAIN-2026-04/segments/x/warc/file.warc.gz", RawJSON: "{}",
	}}); err != nil {
		t.Fatal(err)
	}
	client := commoncrawl.Client{
		DataBaseURL: "https://data.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
		})},
	}
	if err := (Runner{Store: store, Config: cfg, CommonCrawl: client, WorkerID: "test"}).ProcessWAT(context.Background(), run.ID, 10); err != nil {
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
	defer db.Close()
	var state string
	if err := db.QueryRow(`SELECT state FROM wat_tasks WHERE wat_filename <> 'pending-wat-sidecar'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "succeeded" {
		t.Fatalf("state = %s, want succeeded", state)
	}
	var errors int
	if err := db.QueryRow(`SELECT COUNT(*) FROM errors WHERE task_table='wat_tasks' AND retryable=0`).Scan(&errors); err != nil {
		t.Fatal(err)
	}
	if errors != 0 {
		t.Fatalf("errors = %d, want 0", errors)
	}
}

func TestProcessGraphLeasesFetchesExtractsAndPersists(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Concurrency.Graph = 2
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"graph-2026-04"}, Modes: []string{"web_graph"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddGraphReleaseTask(context.Background(), run.ID, "graph-2026-04", "vertices.txt", "edges.txt", []string{"source.com"}); err != nil {
		t.Fatal(err)
	}
	vertices := []byte("1 source.com\n2 graph-target.com\n")
	edges := []byte("1 2\n1 2\n")
	client := commoncrawl.Client{
		DataBaseURL: "https://graph.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/vertices.txt":
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(vertices))}, nil
			case "/edges.txt":
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(edges))}, nil
			default:
				t.Fatalf("unexpected graph path: %s", req.URL.Path)
				return nil, nil
			}
		})},
	}
	if err := (Runner{Store: store, Config: cfg, CommonCrawl: client, WorkerID: "test"}).ProcessGraph(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Domain != "graph-target.com" || results[0].EvidenceModes != "web_graph" || results[0].GraphEdgeCount != 2 {
		t.Fatalf("results = %+v", results)
	}
	events, err := store.RecentEvents(context.Background(), run.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Message == "graph release selected" {
			found = true
		}
	}
	if !found {
		t.Fatalf("graph release event missing: %+v", events)
	}
}

func TestProcessGraphRetryDoesNotDuplicateObservationBatch(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Concurrency.Graph = 1
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"graph-2026-04"}, Modes: []string{"web_graph"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddGraphReleaseTask(context.Background(), run.ID, "graph-2026-04", "vertices.txt", "edges.txt", []string{"source.com"}); err != nil {
		t.Fatal(err)
	}
	vertices := []byte("1 source.com\n2 graph-target.com\n")
	edges := []byte("1 2\n1 2\n")
	client := commoncrawl.Client{
		DataBaseURL: "https://graph.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/vertices.txt":
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(vertices))}, nil
			case "/edges.txt":
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(edges))}, nil
			default:
				t.Fatalf("unexpected graph path: %s", req.URL.Path)
				return nil, nil
			}
		})},
	}
	runner := Runner{Store: store, Config: cfg, CommonCrawl: client, WorkerID: "test"}
	if err := runner.ProcessGraph(context.Background(), run.ID, 10); err != nil {
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
	defer db.Close()
	if _, err := db.Exec(`UPDATE graph_tasks SET state='failed_retryable', lease_owner=NULL, leased_until=NULL WHERE file_path='edges.txt'`); err != nil {
		t.Fatal(err)
	}
	if err := runner.ProcessGraph(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	var graphEdges, sourceSeen, artifacts, batches int
	if err := db.QueryRow(`SELECT graph_edge_count FROM candidate_domains WHERE domain='graph-target.com'`).Scan(&graphEdges); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT seen_count FROM candidate_sources WHERE domain='graph-target.com'`).Scan(&sourceSeen); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM artifact_files WHERE artifact_type='graph_candidates' AND status='sealed'`).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM observation_batches WHERE batch_key LIKE 'graph:%'`).Scan(&batches); err != nil {
		t.Fatal(err)
	}
	if graphEdges != 2 || sourceSeen != 2 || artifacts != 1 || batches != 1 {
		t.Fatalf("counts edges=%d seen=%d artifacts=%d batches=%d, want 2/2/1/1", graphEdges, sourceSeen, artifacts, batches)
	}
}

func TestCheckAvailabilityUsesRegistrarAndPersistsCache(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Concurrency.Registrar = 2
	cfg.Cache.AvailableTTLHours = 2
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
	rdapHTTP := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	runner := Runner{
		Store:       store,
		Config:      cfg,
		RDAP:        availability.RDAPClient{BaseURL: "https://rdap.test/domain", HTTPClient: rdapHTTP},
		Registrar:   fakeRegistrar{},
		DNSResolver: fakeDNS{},
	}
	if err := runner.CheckAvailability(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Status != "available_registrar_confirmed" || results[0].RegistrarProvider != "fake" || results[0].PriceSummary != "standard USD 12.34" {
		t.Fatalf("results = %+v", results)
	}
	if _, ok, err := store.CachedAvailability(context.Background(), "target.com", time.Now().UTC().Add(90*time.Minute)); err != nil || !ok {
		t.Fatalf("cache ok=%v err=%v", ok, err)
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
	var state string
	if err := db.QueryRow(`SELECT state FROM availability_tasks WHERE domain='target.com'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "succeeded" {
		t.Fatalf("availability task state = %s, want succeeded", state)
	}
}

func TestCheckAvailabilityClassifiesRestrictedBeforeNetwork(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Availability.RestrictedTLDs = []string{"gov"}
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddLinkObservations(context.Background(), run.ID, []extraction.LinkObservation{{SourceDomain: "source.com", SourceURL: "https://source.com/", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://agency.gov/", RegistrableDomain: "agency.gov", Mode: "warc"}}, cfg); err != nil {
		t.Fatal(err)
	}
	if err := (Runner{Store: store, Config: cfg, DNSResolver: panicDNS{}, Registrar: panicRegistrar{}}).CheckAvailability(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Status != "restricted_tld" || results[0].TLDPolicyCategory != "restricted" {
		t.Fatalf("results = %+v", results)
	}
}

func TestCheckAvailabilityUsesSQLiteTLDPolicy(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Availability.BlockedTLDs = []string{"zip"}
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddLinkObservations(context.Background(), run.ID, []extraction.LinkObservation{{SourceDomain: "source.com", SourceURL: "https://source.com/", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://candidate.com/", RegistrableDomain: "candidate.com", Mode: "warc"}}, cfg); err != nil {
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
	if _, err := db.Exec(`UPDATE candidate_domains SET domain='candidate.zip', tld='zip' WHERE domain='candidate.com'`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE availability_tasks SET domain='candidate.zip' WHERE domain='candidate.com'`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	if err := (Runner{Store: store, Config: cfg, DNSResolver: panicDNS{}, Registrar: panicRegistrar{}}).CheckAvailability(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Status != "unsupported_tld" || results[0].TLDPolicyCategory != "unsupported_unknown" {
		t.Fatalf("results = %+v", results)
	}
}

func TestCheckAvailabilityClassifiesNetworkError(t *testing.T) {
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
	runner := Runner{
		Store:       store,
		Config:      cfg,
		RDAP:        availability.RDAPClient{BaseURL: "https://rdap.test/domain", HTTPClient: &http.Client{Transport: errorRoundTripper{err: fmt.Errorf("rdap unavailable")}}},
		DNSResolver: fakeDNS{},
	}
	if err := runner.CheckAvailability(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Status != "error" {
		t.Fatalf("results = %+v", results)
	}
}

func TestCheckAvailabilityClassifiesRDAPRateLimit(t *testing.T) {
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
	rdapHTTP := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("Retry-After", "0")
		return &http.Response{StatusCode: http.StatusTooManyRequests, Header: header, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	runner := Runner{
		Store:       store,
		Config:      cfg,
		RDAP:        availability.RDAPClient{BaseURL: "https://rdap.test/domain", HTTPClient: rdapHTTP, MaxRetries: 1, RetryBackoff: time.Nanosecond},
		DNSResolver: fakeDNS{},
	}
	if err := runner.CheckAvailability(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Status != "rate_limited" {
		t.Fatalf("results = %+v", results)
	}
}

func TestCheckAvailabilityDefersRegistrarRateLimitMessage(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Availability.RegistrarCheckStatuses = []string{"*"}
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
	rdapHTTP := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	rdapClient := availability.RDAPClient{BaseURL: "https://rdap.test/domain", HTTPClient: rdapHTTP}
	if rdap, err := rdapClient.Lookup(context.Background(), "target.com"); err != nil || !rdap.NotFound {
		t.Fatalf("rdap=%+v err=%v, want not found", rdap, err)
	}
	runner := Runner{
		Store:       store,
		Config:      cfg,
		RDAP:        rdapClient,
		Registrar:   errorRegistrar{err: fmt.Errorf("registrar rate limited")},
		DNSResolver: emptyDNS{},
	}
	if err := runner.CheckAvailability(context.Background(), run.ID, 10); err != nil {
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
	defer db.Close()
	var status sql.NullString
	if err := db.QueryRow(`SELECT availability_status FROM candidate_domains WHERE domain='target.com'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status.Valid {
		t.Fatalf("availability_status = %s, want NULL while deferred", status.String)
	}
}

func TestCheckAvailabilitySkipsRegistrarForRegisteredDomain(t *testing.T) {
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
	rdapHTTP := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"status":["active"]}`))}, nil
	})}
	runner := Runner{
		Store:       store,
		Config:      cfg,
		RDAP:        availability.RDAPClient{BaseURL: "https://rdap.test/domain", HTTPClient: rdapHTTP},
		Registrar:   panicRegistrar{},
		DNSResolver: activeDNS{},
	}
	if err := runner.CheckAvailability(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Status != "registered_active" || results[0].RegistrarProvider != "" {
		t.Fatalf("results = %+v", results)
	}
}

func TestCheckAvailabilityDefersRegistrarRateLimitUntilRetryAt(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Availability.RegistrarCheckStatuses = []string{"*"}
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
	rdapHTTP := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	rdapClient := availability.RDAPClient{BaseURL: "https://rdap.test/domain", HTTPClient: rdapHTTP}
	if rdap, err := rdapClient.Lookup(context.Background(), "target.com"); err != nil || !rdap.NotFound {
		t.Fatalf("rdap=%+v err=%v, want not found", rdap, err)
	}
	runner := Runner{
		Store:       store,
		Config:      cfg,
		RDAP:        rdapClient,
		Registrar:   errorRegistrar{err: availability.RateLimitError{Provider: "porkbun", RetryAfter: time.Second}},
		DNSResolver: emptyDNS{},
	}
	if err := runner.CheckAvailability(context.Background(), run.ID, 10); err != nil {
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
	defer db.Close()
	var status sql.NullString
	if err := db.QueryRow(`SELECT availability_status FROM candidate_domains WHERE domain='target.com'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status.Valid {
		t.Fatalf("availability_status = %s, want NULL while deferred", status.String)
	}
	if count, err := store.RunnableTaskCount(context.Background(), run.ID, "availability_tasks"); err != nil || count != 0 {
		t.Fatalf("runnable before retry count=%d err=%v, want 0", count, err)
	}
	time.Sleep(1100 * time.Millisecond)
	runner.Registrar = fakeRegistrar{}
	if err := runner.CheckAvailability(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Status != "available_registrar_confirmed" {
		t.Fatalf("results = %+v", results)
	}
}

func TestCheckAvailabilityUsesDNSWorkersAndRegistrarLimit(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDirectory = dir
	cfg.Concurrency.DNS = 4
	cfg.Concurrency.RDAP = 4
	cfg.Concurrency.Registrar = 1
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.New(dir)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	var observations []extraction.LinkObservation
	for i := 0; i < 4; i++ {
		domain := fmt.Sprintf("target-%d.com", i)
		observations = append(observations, extraction.LinkObservation{SourceDomain: "source.com", SourceURL: "https://source.com/", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://" + domain + "/", RegistrableDomain: domain, Mode: "warc"})
	}
	if err := store.AddLinkObservations(context.Background(), run.ID, observations, cfg); err != nil {
		t.Fatal(err)
	}
	rdapHTTP := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})}
	registrar := &trackingRegistrar{}
	runner := Runner{
		Store:       store,
		Config:      cfg,
		RDAP:        availability.RDAPClient{BaseURL: "https://rdap.test/domain", HTTPClient: rdapHTTP},
		Registrar:   registrar,
		DNSResolver: fakeDNS{},
	}
	if err := runner.CheckAvailability(context.Background(), run.ID, 10); err != nil {
		t.Fatal(err)
	}
	if registrar.maxActive != 1 {
		t.Fatalf("registrar max active = %d, want 1", registrar.maxActive)
	}
	results, err := store.Results(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 4 {
		t.Fatalf("results = %+v", results)
	}
}

func TestClassifyTLDPolicy(t *testing.T) {
	cfg := config.Default()
	cfg.Availability.BlockedTLDs = []string{"zip"}
	if category, signals := classifyTLDPolicy("example.zip", cfg); category != "unsupported_unknown" || !signals.UnsupportedTLD {
		t.Fatalf("blocked = %s %+v", category, signals)
	}
	if category, signals := classifyTLDPolicy("thing.test", cfg); category != "reserved_infrastructure" || !signals.ReservedTLD {
		t.Fatalf("reserved = %s %+v", category, signals)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type errorRoundTripper struct{ err error }

func (e errorRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, e.err
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}

type fakeRegistrar struct{}

func (fakeRegistrar) Name() string { return "fake" }
func (fakeRegistrar) Check(context.Context, string) (availability.RegistrarResult, error) {
	return availability.RegistrarResult{Available: true, PriceCents: 1234, Currency: "USD"}, nil
}

type errorRegistrar struct{ err error }

func (errorRegistrar) Name() string { return "error" }
func (r errorRegistrar) Check(context.Context, string) (availability.RegistrarResult, error) {
	return availability.RegistrarResult{}, r.err
}

type trackingRegistrar struct {
	active    int32
	maxActive int32
}

func (*trackingRegistrar) Name() string { return "tracking" }

func (r *trackingRegistrar) Check(context.Context, string) (availability.RegistrarResult, error) {
	current := atomic.AddInt32(&r.active, 1)
	for {
		previous := atomic.LoadInt32(&r.maxActive)
		if current <= previous || atomic.CompareAndSwapInt32(&r.maxActive, previous, current) {
			break
		}
	}
	time.Sleep(5 * time.Millisecond)
	atomic.AddInt32(&r.active, -1)
	return availability.RegistrarResult{Available: true, Currency: "USD"}, nil
}

type fakeDNS struct{}

func (fakeDNS) LookupNS(context.Context, string) ([]*net.NS, error)        { return nil, net.ErrClosed }
func (fakeDNS) LookupHost(context.Context, string) ([]string, error)       { return nil, net.ErrClosed }
func (fakeDNS) LookupMX(context.Context, string) ([]*net.MX, error)        { return nil, net.ErrClosed }
func (fakeDNS) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) { return nil, net.ErrClosed }

type emptyDNS struct{}

func (emptyDNS) LookupNS(context.Context, string) ([]*net.NS, error)        { return nil, nil }
func (emptyDNS) LookupHost(context.Context, string) ([]string, error)       { return nil, nil }
func (emptyDNS) LookupMX(context.Context, string) ([]*net.MX, error)        { return nil, nil }
func (emptyDNS) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) { return nil, nil }

type activeDNS struct{}

func (activeDNS) LookupNS(context.Context, string) ([]*net.NS, error) {
	return []*net.NS{{Host: "ns1.example.com."}}, nil
}
func (activeDNS) LookupHost(context.Context, string) ([]string, error) {
	return []string{"192.0.2.10"}, nil
}
func (activeDNS) LookupMX(context.Context, string) ([]*net.MX, error) {
	return []*net.MX{{Host: "mail.example.com.", Pref: 10}}, nil
}
func (activeDNS) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return []net.IPAddr{{IP: net.ParseIP("192.0.2.10")}}, nil
}

type panicDNS struct{}

func (panicDNS) LookupNS(context.Context, string) ([]*net.NS, error) {
	panic("dns should not be called")
}
func (panicDNS) LookupHost(context.Context, string) ([]string, error) {
	panic("dns should not be called")
}
func (panicDNS) LookupMX(context.Context, string) ([]*net.MX, error) {
	panic("dns should not be called")
}
func (panicDNS) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	panic("dns should not be called")
}

type panicRegistrar struct{}

func (panicRegistrar) Name() string { return "panic" }
func (panicRegistrar) Check(context.Context, string) (availability.RegistrarResult, error) {
	panic("registrar should not be called")
}
