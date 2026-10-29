package guiapp

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"expired-domain-finder/internal/artifact"
	"expired-domain-finder/internal/availability"
	"expired-domain-finder/internal/commoncrawl"
	"expired-domain-finder/internal/config"
	"expired-domain-finder/internal/normalization"
	"expired-domain-finder/internal/progress"
	"expired-domain-finder/internal/storage"
	"expired-domain-finder/internal/workflow"
)

type RunRequest struct {
	Domain string
	Year   int
	Month  time.Month
}

type RunResult struct {
	RunID       string
	RunDir      string
	CrawlID     string
	Status      string
	ExportPath  string
	ExportRows  int
	Warning     string
	Recoverable bool
}

type Update struct {
	Snapshot progress.Snapshot
	Message  string
	CrawlID  string
}

type Engine struct {
	Config           config.Config
	Store            storage.Store
	CommonCrawl      commoncrawl.Client
	RDAP             availability.RDAPClient
	Registrar        availability.RegistrarProvider
	DNSResolver      availability.DNSResolver
	DocumentsDir     string
	ProgressInterval time.Duration
}

func NewEngine(cfg config.Config) Engine {
	return Engine{
		Config:           cfg,
		Store:            storage.New(cfg.DataDirectory),
		CommonCrawl:      commonCrawlClient(cfg),
		RDAP:             availability.RDAPClient{UserAgent: cfg.CommonCrawl.UserAgent},
		Registrar:        registrarFromEnv(cfg),
		DocumentsDir:     defaultDocumentsDir(),
		ProgressInterval: 10 * time.Second,
	}
}

func (e Engine) ResolveCrawl(ctx context.Context, year int, month time.Month) (commoncrawl.CrawlSelection, error) {
	if year < 1996 || year > 9999 {
		return commoncrawl.CrawlSelection{}, fmt.Errorf("year must be four digits")
	}
	if month < time.January || month > time.December {
		return commoncrawl.CrawlSelection{}, fmt.Errorf("month is required")
	}
	catalogClient := e.CommonCrawl
	if catalogClient.MaxRetries <= 0 || catalogClient.MaxRetries > 1 {
		catalogClient.MaxRetries = 1
	}
	if catalogClient.RetryBackoff <= 0 || catalogClient.RetryBackoff > 250*time.Millisecond {
		catalogClient.RetryBackoff = 250 * time.Millisecond
	}
	if catalogClient.MaxBackoff <= 0 || catalogClient.MaxBackoff > time.Second {
		catalogClient.MaxBackoff = time.Second
	}
	catalog, err := catalogClient.LoadCatalog(ctx)
	if err != nil {
		selection, fallbackErr := commoncrawl.ResolveClosestOlderOrExactMonth(fmt.Sprintf("%04d-%02d", year, month), commoncrawl.FallbackCatalog())
		if fallbackErr != nil {
			return commoncrawl.CrawlSelection{}, fmt.Errorf("load Common Crawl catalog: %w", err)
		}
		return selection, nil
	}
	return commoncrawl.ResolveClosestOlderOrExactMonth(fmt.Sprintf("%04d-%02d", year, month), catalog)
}

func (e Engine) Start(ctx context.Context, req RunRequest, updates chan<- Update) (RunResult, error) {
	source, err := normalization.NormalizeSource(req.Domain)
	if err != nil {
		return RunResult{}, err
	}
	selection, err := e.ResolveCrawl(ctx, req.Year, req.Month)
	if err != nil {
		return RunResult{}, err
	}
	cfg := e.Config
	if cfg.DataDirectory == "" {
		cfg.DataDirectory = config.DefaultDataDir()
	}
	store := e.Store
	if store.DataDir == "" {
		store = storage.New(cfg.DataDirectory)
	}
	run, err := store.CreateRun(ctx, storage.RunRequest{
		Sources: []normalization.Result{source},
		Crawls:  []string{selection.ResolvedID},
		Modes:   []string{"warc", "wat"},
		Config:  cfg,
	})
	if err != nil {
		return RunResult{}, err
	}
	if updates != nil {
		updates <- Update{Message: fmt.Sprintf("resolved crawl %s", selection.ResolvedID), CrawlID: selection.ResolvedID}
	}
	stop := startSnapshotUpdates(ctx, store, run.ID, e.progressInterval(), updates)
	defer stop()

	runner := workflow.Runner{
		Store:       store,
		Config:      cfg,
		CommonCrawl: e.CommonCrawl,
		RDAP:        e.RDAP,
		Registrar:   e.Registrar,
		DNSResolver: e.DNSResolver,
		WorkerID:    "gui",
	}
	err = ExecuteRunPlan(ctx, runner, run.ID, []string{source.RegistrableDomain}, []string{selection.ResolvedID}, []string{"warc", "wat"}, Limits{})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			_ = store.SetRunStatus(context.Background(), run.ID, "cancelling")
			_ = store.EnsureRunnable(context.Background(), run.ID)
			return e.finishRun(context.Background(), store, run, selection.ResolvedID, source, "cancelled")
		}
		if isControlledStop(err) {
			return e.finishRun(context.Background(), store, run, selection.ResolvedID, source, "")
		}
		_ = store.SetRunStatus(context.Background(), run.ID, "failed")
		return RunResult{RunID: run.ID, RunDir: run.Dir, CrawlID: selection.ResolvedID, Status: "failed"}, err
	}
	return e.finishRun(context.Background(), store, run, selection.ResolvedID, source, "")
}

func (e Engine) Resume(ctx context.Context, runID string, updates chan<- Update) (RunResult, error) {
	store := e.Store
	if store.DataDir == "" {
		store = storage.New(e.Config.DataDirectory)
	}
	plan, err := store.RunPlan(ctx, runID)
	if err != nil {
		return RunResult{}, err
	}
	if err := store.SetRunStatus(ctx, runID, "resume_requested"); err != nil {
		return RunResult{}, err
	}
	stop := startSnapshotUpdates(ctx, store, runID, e.progressInterval(), updates)
	defer stop()
	cfg := plan.Config
	if e.Config.DataDirectory != "" {
		cfg.DataDirectory = e.Config.DataDirectory
	}
	runner := workflow.Runner{
		Store:       store,
		Config:      cfg,
		CommonCrawl: e.CommonCrawl,
		RDAP:        e.RDAP,
		Registrar:   e.Registrar,
		DNSResolver: e.DNSResolver,
		WorkerID:    "gui-resume",
	}
	err = ExecuteRunPlan(ctx, runner, runID, plan.Sources, plan.Crawls, plan.Modes, Limits{})
	runDir, _ := store.RunDir(runID)
	source := normalization.Result{RegistrableDomain: first(plan.Sources)}
	crawl := first(plan.Crawls)
	run := storage.Run{ID: runID, Dir: runDir}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			_ = store.SetRunStatus(context.Background(), runID, "cancelling")
			_ = store.EnsureRunnable(context.Background(), runID)
			return e.finishRun(context.Background(), store, run, crawl, source, "cancelled")
		}
		if isControlledStop(err) {
			return e.finishRun(context.Background(), store, run, crawl, source, "")
		}
		_ = store.SetRunStatus(context.Background(), runID, "failed")
		return RunResult{RunID: runID, RunDir: runDir, CrawlID: crawl, Status: "failed"}, err
	}
	return e.finishRun(context.Background(), store, run, crawl, source, "")
}

func (e Engine) Cancel(ctx context.Context, runID string) error {
	store := e.Store
	if store.DataDir == "" {
		store = storage.New(e.Config.DataDirectory)
	}
	return store.SetRunStatus(ctx, runID, "cancelling")
}

func (e Engine) Snapshot(ctx context.Context, runID string) (progress.Snapshot, error) {
	store := e.Store
	if store.DataDir == "" {
		store = storage.New(e.Config.DataDirectory)
	}
	return store.Snapshot(ctx, runID)
}

func (e Engine) Results(ctx context.Context, runID string) ([]storage.Candidate, error) {
	store := e.Store
	if store.DataDir == "" {
		store = storage.New(e.Config.DataDirectory)
	}
	return store.Results(ctx, runID)
}

func (e Engine) finishRun(ctx context.Context, store storage.Store, run storage.Run, crawlID string, source normalization.Result, forcedStatus string) (RunResult, error) {
	status := forcedStatus
	warning := ""
	if status == "" {
		blockers, err := store.CompletionBlockers(ctx, run.ID)
		if err != nil {
			return RunResult{}, err
		}
		switch {
		case len(blockers) == 0:
			status = "completed"
		case onlyCDXRetryableBlockers(blockers):
			status = "recoverable"
			warning = "Partial Common Crawl discovery; resume is available."
		case hasCancelledBlocker(blockers):
			status = "cancelled"
		default:
			status = "paused"
			warning = "Incomplete work remains; resume is available."
		}
	}
	if status == "completed" {
		warning = failedUnitWarning(ctx, store, run.ID)
	}
	if err := store.SetRunStatus(ctx, run.ID, status); err != nil {
		return RunResult{}, err
	}
	result := RunResult{
		RunID:       run.ID,
		RunDir:      run.Dir,
		CrawlID:     crawlID,
		Status:      status,
		Warning:     warning,
		Recoverable: status == "recoverable" || status == "paused",
	}
	if status == "cancelled" || status == "failed" {
		return result, nil
	}
	path := e.DocumentsExportPath(source.RegistrableDomain, crawlID, run.ID)
	rows, manifest, err := WriteFilteredCSVExport(ctx, store, run.ID, path)
	if err != nil {
		return RunResult{}, err
	}
	if err := store.RecordArtifact(ctx, run.ID, "export", manifest); err != nil {
		return RunResult{}, err
	}
	result.ExportPath = manifest.Path
	result.ExportRows = len(rows)
	return result, nil
}

func (e Engine) DocumentsExportPath(domain, crawlID, runID string) string {
	dir := e.DocumentsDir
	if dir == "" {
		dir = defaultDocumentsDir()
	}
	month := crawlID
	if strings.HasPrefix(crawlID, "CC-MAIN-") {
		parts := strings.Split(crawlID, "-")
		if len(parts) >= 4 {
			month = parts[2] + "-" + parts[3]
		}
	}
	return filepath.Join(dir, fmt.Sprintf("edf-expired-domains-%s-%s-%s.csv", safeFileComponent(domain), safeFileComponent(month), safeFileComponent(runID)))
}

func WriteFilteredCSVExport(ctx context.Context, store storage.Store, runID, output string) ([]storage.Candidate, artifact.Manifest, error) {
	rows, err := store.Results(ctx, runID)
	if err != nil {
		return nil, artifact.Manifest{}, fmt.Errorf("load results: %w", err)
	}
	filtered := filterExpiredRows(rows)
	if dir := filepath.Dir(output); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, artifact.Manifest{}, fmt.Errorf("create export directory: %w", err)
		}
	}
	if err := writeCSV(output, filtered); err != nil {
		return nil, artifact.Manifest{}, fmt.Errorf("write export: %w", err)
	}
	manifest, err := artifact.ManifestFile(output, "csv", "", len(filtered))
	if err != nil {
		return nil, artifact.Manifest{}, fmt.Errorf("manifest export: %w", err)
	}
	return filtered, manifest, nil
}

func filterExpiredRows(rows []storage.Candidate) []storage.Candidate {
	out := make([]storage.Candidate, 0, len(rows))
	for _, row := range rows {
		switch row.Status {
		case string(availability.AvailableRegistrarConfirmed), string(availability.AvailableLikely):
			out = append(out, row)
		}
	}
	return out
}

func writeCSV(path string, rows []storage.Candidate) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{"domain", "availability_status", "confidence", "tld_policy_category", "source_page_count", "graph_edge_count", "outbound_url_count", "evidence_modes", "evidence_sample", "registrar_provider", "price_summary", "score", "manual_verification_required"}); err != nil {
		return err
	}
	for _, row := range rows {
		if err := writer.Write([]string{row.Domain, row.Status, row.Confidence, row.TLDPolicyCategory, fmt.Sprint(row.SourcePageCount), fmt.Sprint(row.GraphEdgeCount), fmt.Sprint(row.OutboundURLCount), row.EvidenceModes, row.EvidenceSample, row.RegistrarProvider, row.PriceSummary, fmt.Sprint(row.Score), row.ManualVerification}); err != nil {
			return err
		}
	}
	return writer.Error()
}

type Limits struct {
	CDXRecords         int
	WARCTasks          int
	WATTasks           int
	AvailabilityChecks int
}

func ExecuteRunPlan(ctx context.Context, runner workflow.Runner, runID string, sources, crawls, modes []string, limits Limits) error {
	if err := runner.DiscoverCDX(ctx, runID, sources, crawls, limits.CDXRecords); err != nil {
		return fmt.Errorf("discovery: %w", err)
	}
	for _, mode := range modes {
		switch mode {
		case "warc":
			if err := drainRunnableTasks(ctx, runner.Store, runID, "warc_tasks", limits.WARCTasks, taskBatchLimit(limits.WARCTasks, runner.Config.Concurrency.WARC), func(batchLimit int) error {
				return runner.ProcessWARC(ctx, runID, batchLimit)
			}); err != nil {
				return fmt.Errorf("warc: %w", err)
			}
		case "wat":
			if err := drainRunnableTasks(ctx, runner.Store, runID, "wat_tasks", limits.WATTasks, taskBatchLimit(limits.WATTasks, runner.Config.Concurrency.WAT), func(batchLimit int) error {
				return runner.ProcessWAT(ctx, runID, batchLimit)
			}); err != nil {
				return fmt.Errorf("wat: %w", err)
			}
		default:
			return fmt.Errorf("unsupported GUI mode: %s", mode)
		}
	}
	if err := drainRunnableTasks(ctx, runner.Store, runID, "availability_tasks", limits.AvailabilityChecks, taskBatchLimit(limits.AvailabilityChecks, runner.Config.Concurrency.DNS), func(batchLimit int) error {
		return runner.CheckAvailability(ctx, runID, batchLimit)
	}); err != nil {
		return fmt.Errorf("availability: %w", err)
	}
	return nil
}

func drainRunnableTasks(ctx context.Context, store storage.Store, runID, table string, totalLimit, batchLimit int, process func(batchLimit int) error) error {
	if batchLimit <= 0 {
		batchLimit = 1
	}
	processed := 0
	for {
		pending, err := store.RunnableTaskCount(ctx, runID, table)
		if err != nil {
			return err
		}
		if pending == 0 {
			return nil
		}
		if totalLimit > 0 && processed >= totalLimit {
			return fmt.Errorf("work budget reached")
		}
		remaining := batchLimit
		if totalLimit > 0 && totalLimit-processed < remaining {
			remaining = totalLimit - processed
		}
		if err := process(remaining); err != nil {
			return err
		}
		after, err := store.RunnableTaskCount(ctx, runID, table)
		if err != nil {
			return err
		}
		consumed := pending - after
		if consumed <= 0 {
			consumed = remaining
		}
		processed += consumed
	}
}

func startSnapshotUpdates(ctx context.Context, store storage.Store, runID string, interval time.Duration, updates chan<- Update) func() {
	if updates == nil {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				snapshot, err := store.Snapshot(context.Background(), runID)
				if err != nil {
					updates <- Update{Message: err.Error()}
					continue
				}
				updates <- Update{Snapshot: snapshot}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func commonCrawlClient(cfg config.Config) commoncrawl.Client {
	client := commoncrawl.NewClient(cfg.CommonCrawl.IndexBaseURL, cfg.CommonCrawl.UserAgent)
	if cfg.CommonCrawl.DataBaseURL != "" {
		client.DataBaseURL = cfg.CommonCrawl.DataBaseURL
	}
	if cfg.Runtime.MaxRetryAttempts > 0 {
		client.MaxRetries = cfg.Runtime.MaxRetryAttempts
	}
	if cfg.Runtime.InitialRetryBackoffMillis > 0 {
		client.RetryBackoff = time.Duration(cfg.Runtime.InitialRetryBackoffMillis) * time.Millisecond
	}
	if cfg.Runtime.MaxRetryBackoffSeconds > 0 {
		client.MaxBackoff = time.Duration(cfg.Runtime.MaxRetryBackoffSeconds) * time.Second
	}
	return client
}

func registrarFromEnv(cfg config.Config) availability.RegistrarProvider {
	if cfg.Availability.RegistrarProvider == nil || *cfg.Availability.RegistrarProvider != "porkbun" {
		return nil
	}
	apiKey := os.Getenv("PORKBUN_API_KEY")
	secretKey := os.Getenv("PORKBUN_SECRET_KEY")
	if apiKey == "" || secretKey == "" {
		return nil
	}
	return availability.PorkbunProvider{APIKey: apiKey, SecretKey: secretKey, HTTPClient: http.DefaultClient}
}

func onlyCDXRetryableBlockers(blockers map[string]int) bool {
	if len(blockers) == 0 {
		return false
	}
	for name, count := range blockers {
		if count <= 0 {
			continue
		}
		if name != "cdx_failed_retryable" {
			return false
		}
	}
	return blockers["cdx_failed_retryable"] > 0
}

func hasCancelledBlocker(blockers map[string]int) bool {
	for name, count := range blockers {
		if count > 0 && strings.Contains(name, "cancelled") {
			return true
		}
	}
	return false
}

func failedUnitWarning(ctx context.Context, store storage.Store, runID string) string {
	snapshot, err := store.Snapshot(ctx, runID)
	if err != nil {
		return ""
	}
	totalFailed := 0
	for _, stage := range snapshot.Stages {
		totalFailed += stage.Failed
	}
	if totalFailed == 0 {
		return ""
	}
	return fmt.Sprintf("Completed with %d failed work units; export includes results from successfully processed data.", totalFailed)
}

func isControlledStop(err error) bool {
	return errors.Is(err, storage.ErrRunPaused) || errors.Is(err, storage.ErrRunCancelled) || errors.Is(err, storage.ErrRunCompleted)
}

func taskBatchLimit(totalLimit, concurrency int) int {
	if concurrency <= 0 {
		concurrency = 1
	}
	if totalLimit > 0 && totalLimit < concurrency {
		return totalLimit
	}
	return concurrency
}

func safeFileComponent(value string) string {
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), ".-")
	if out == "" {
		return "run"
	}
	return out
}

func defaultDocumentsDir() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "Documents"
	}
	return filepath.Join(home, "Documents")
}

func (e Engine) progressInterval() time.Duration {
	if e.ProgressInterval > 0 {
		return e.ProgressInterval
	}
	return 10 * time.Second
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
