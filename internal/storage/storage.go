package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	schemadata "expired-domain-finder"
	"expired-domain-finder/internal/artifact"
	"expired-domain-finder/internal/availability"
	"expired-domain-finder/internal/commoncrawl"
	"expired-domain-finder/internal/config"
	"expired-domain-finder/internal/extraction"
	"expired-domain-finder/internal/graph"
	"expired-domain-finder/internal/normalization"
	"expired-domain-finder/internal/progress"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	_ "modernc.org/sqlite"
)

var (
	ErrRunPaused        = errors.New("run paused")
	ErrRunCancelled     = errors.New("run cancelled")
	ErrRunCompleted     = errors.New("run completed")
	runWriteMu          sync.Mutex
	availabilityWriteMu sync.Mutex
)

const (
	RunDBName   = "run_state.sqlite"
	CacheDBName = "domain_cache.sqlite"
)

type RunRequest struct {
	Sources       []normalization.Result
	Crawls        []string
	Modes         []string
	Config        config.Config
	InitialStatus string
}

type Run struct {
	ID        string    `json:"id"`
	Dir       string    `json:"dir"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
}

type RunPlan struct {
	ID      string
	Sources []string
	Crawls  []string
	Modes   []string
	Config  config.Config
}

type Candidate struct {
	Domain             string `json:"domain"`
	Status             string `json:"status"`
	Confidence         string `json:"confidence"`
	TLDPolicyCategory  string `json:"tldPolicyCategory"`
	SourcePageCount    int    `json:"sourcePageCount"`
	GraphEdgeCount     int    `json:"graphEdgeCount"`
	OutboundURLCount   int    `json:"outboundUrlCount"`
	EvidenceModes      string `json:"evidenceModes"`
	EvidenceSample     string `json:"evidenceSample"`
	RegistrarProvider  string `json:"registrarProvider,omitempty"`
	PriceSummary       string `json:"priceSummary,omitempty"`
	ManualVerification string `json:"manualVerificationRequired"`
	Score              int    `json:"score"`
}

type Event struct {
	Severity  string `json:"severity"`
	Component string `json:"component"`
	Message   string `json:"message"`
	CreatedAt string `json:"createdAt"`
}

type AvailabilityRecord struct {
	Domain            string
	Status            availability.Status
	Confidence        availability.Confidence
	TLDPolicyCategory string
	Reason            string
	Provider          string
	Premium           bool
	PriceCents        int
	Currency          string
	CheckedAt         time.Time
	ExpiresAt         time.Time
	CacheHit          bool
}

type CDXPageState struct {
	SourceDomain string
	CrawlID      string
	Pattern      string
	Page         int
	PageSize     int
	State        string
	Attempts     int
	RecordCount  int
	LastError    string
}

type LeasedTask struct {
	ID          int64
	Table       string
	State       string
	Attempts    int
	LeaseOwner  string
	LeasedUntil time.Time
}

type LeasedWARCTask struct {
	LeasedTask
	SourceDomain string
	CrawlID      string
	SourceURL    string
	Timestamp    string
	Filename     string
	Offset       int64
	Length       int64
}

type LeasedWATTask struct {
	LeasedTask
	CrawlID      string
	WATFilename  string
	SourceDomain string
}

type LeasedGraphTask struct {
	LeasedTask
	ReleaseID     string
	TaskType      string
	FilePath      string
	SourceDomains []string
	VertexPath    string
}

type LeasedAvailabilityTask struct {
	LeasedTask
	Domain   string
	Method   string
	Provider string
}

type Store struct {
	DataDir string
}

func New(dataDir string) Store {
	if dataDir == "" {
		dataDir = config.DefaultDataDir()
	}
	return Store{DataDir: dataDir}
}

func (s Store) Init(ctx context.Context, cfg config.Config) error {
	if err := os.MkdirAll(filepath.Join(s.DataDir, "cache"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(s.DataDir, "runs"), 0o755); err != nil {
		return err
	}
	if err := config.Save(config.ConfigPath(s.DataDir), cfg); err != nil {
		return err
	}
	db, err := openSQLite(filepath.Join(s.DataDir, "cache", CacheDBName))
	if err != nil {
		return err
	}
	defer db.Close()
	if err := applySchema(ctx, db, "domain_cache_schema.sql", "initial cache schema"); err != nil {
		return err
	}
	if err := seedTLDPolicy(ctx, db, cfg); err != nil {
		return err
	}
	return seedIgnoreRules(ctx, db, cfg)
}

func (s Store) CreateRun(ctx context.Context, req RunRequest) (Run, error) {
	if len(req.Sources) == 0 {
		return Run{}, fmt.Errorf("at least one source is required")
	}
	if len(req.Crawls) == 0 {
		return Run{}, fmt.Errorf("at least one crawl is required")
	}
	if len(req.Modes) == 0 {
		return Run{}, fmt.Errorf("at least one mode is required")
	}
	if err := s.Init(ctx, req.Config); err != nil {
		return Run{}, err
	}
	now := time.Now().UTC()
	runID := fmt.Sprintf("run_%s_%s_%s", now.Format("20060102T150405Z"), slug(req.Sources[0].RegistrableDomain), shortID())
	runDir := filepath.Join(s.DataDir, "runs", runID)
	for _, dir := range []string{
		runDir,
		filepath.Join(runDir, "artifacts", "link_observations"),
		filepath.Join(runDir, "artifacts", "graph_candidates"),
		filepath.Join(runDir, "artifacts", "exports"),
		filepath.Join(runDir, "logs"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return Run{}, err
		}
	}
	db, err := openSQLite(filepath.Join(runDir, RunDBName))
	if err != nil {
		return Run{}, err
	}
	defer db.Close()
	if err := applySchema(ctx, db, "run_state_schema.sql", "initial run schema"); err != nil {
		return Run{}, err
	}
	cfgJSON, err := json.Marshal(req.Config.Redacted())
	if err != nil {
		return Run{}, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Run{}, err
	}
	defer tx.Rollback()
	initialStatus := req.InitialStatus
	if initialStatus == "" {
		initialStatus = "running"
	}
	switch initialStatus {
	case "created", "running":
	default:
		return Run{}, fmt.Errorf("unsupported initial run status: %s", initialStatus)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO runs(id,name,status,app_version,config_json,created_at,started_at,updated_at) VALUES(?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`, runID, runID, initialStatus, "dev", string(cfgJSON)); err != nil {
		return Run{}, err
	}
	for _, source := range req.Sources {
		if _, err := tx.ExecContext(ctx, `INSERT INTO run_source_domains(run_id,source_domain,original_input) VALUES(?,?,?)`, runID, source.RegistrableDomain, source.Original); err != nil {
			return Run{}, err
		}
	}
	for _, crawl := range req.Crawls {
		selectionType := "crawl_id"
		if isMonth(crawl) {
			selectionType = "month"
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO run_crawl_selections(run_id,selection_type,selection_value,resolved_id) VALUES(?,?,?,?)`, runID, selectionType, crawl, crawl); err != nil {
			return Run{}, err
		}
	}
	for _, mode := range req.Modes {
		if _, err := tx.ExecContext(ctx, `INSERT INTO extraction_modes(run_id,mode,enabled) VALUES(?,?,1)`, runID, mode); err != nil {
			return Run{}, err
		}
	}
	for _, stage := range []string{"discovery", "extraction", "domain_checks", "export"} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO progress_counters(run_id,stage,total_known_units,confidence) VALUES(?,?,?,?)`, runID, stage, initialTotal(stage, req), "low"); err != nil {
			return Run{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,event_type,severity,component,message) VALUES(?,?,?,?,?)`, runID, "run_started", "info", "orchestrator", "run state initialized"); err != nil {
		return Run{}, err
	}
	maxAttempts := taskMaxAttempts(req.Config)
	for _, mode := range req.Modes {
		switch mode {
		case "warc":
			for _, source := range req.Sources {
				for _, crawl := range req.Crawls {
					_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO warc_tasks(run_id,crawl_id,source_domain,filename,offset,length,max_attempts) VALUES(?,?,?,?,0,0,?)`, runID, crawl, source.RegistrableDomain, "pending-cdx-range", maxAttempts)
				}
			}
		case "wat":
			for _, crawl := range req.Crawls {
				_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO wat_tasks(run_id,crawl_id,source_domain_set_hash,wat_filename,max_attempts) VALUES(?,?,?,?,?)`, runID, crawl, sourceSetHash(req.Sources), "pending-wat-sidecar", maxAttempts)
			}
		case "web_graph":
			for _, crawl := range req.Crawls {
				_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO graph_tasks(run_id,graph_release_id,task_type,file_path,source_domain_set_hash,max_attempts) VALUES(?,?,?,?,?,?)`, runID, crawl, "edge_scan", "pending-graph-release", sourceSetHash(req.Sources), maxAttempts)
			}
		}
		if err != nil {
			return Run{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Run{}, err
	}
	return Run{ID: runID, Dir: runDir, Status: initialStatus, CreatedAt: now}, nil
}

func (s Store) Snapshot(ctx context.Context, runID string) (progress.Snapshot, error) {
	var snapshot progress.Snapshot
	err := retrySQLiteLocked(ctx, func() error {
		var err error
		snapshot, err = s.snapshotOnce(ctx, runID)
		return err
	})
	return snapshot, err
}

func (s Store) snapshotOnce(ctx context.Context, runID string) (progress.Snapshot, error) {
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return progress.Snapshot{}, err
	}
	defer db.Close()
	if err := recalculateProgress(ctx, db, runID, time.Now().UTC()); err != nil {
		return progress.Snapshot{}, err
	}
	var status string
	var created string
	if err := db.QueryRowContext(ctx, `SELECT status, created_at FROM runs WHERE id = ?`, runID).Scan(&status, &created); err != nil {
		return progress.Snapshot{}, err
	}
	rows, err := db.QueryContext(ctx, `SELECT stage, completed_units, total_known_units, failed_units, recent_rate, COALESCE(eta_seconds,0), COALESCE(confidence,'low')
FROM progress_counters
WHERE run_id=?
ORDER BY CASE stage
  WHEN 'discovery' THEN 1
  WHEN 'extraction' THEN 2
  WHEN 'domain_checks' THEN 3
  WHEN 'export' THEN 4
  ELSE 99
END`, runID)
	if err != nil {
		return progress.Snapshot{}, err
	}
	defer rows.Close()
	var stages []progress.Stage
	totalComplete, totalKnown, totalFailed := 0, 0, 0
	for rows.Next() {
		var stage string
		var complete, known, failed int
		var rate float64
		var etaSec int
		var confidence string
		if err := rows.Scan(&stage, &complete, &known, &failed, &rate, &etaSec, &confidence); err != nil {
			return progress.Snapshot{}, err
		}
		totalComplete += complete
		totalKnown += known
		totalFailed += failed
		stages = append(stages, progress.Stage{Name: progress.StageName(titleStage(stage)), Complete: complete, Total: known, Failed: failed, Rate: rate, ETA: time.Duration(etaSec) * time.Second, Confidence: confidence})
	}
	pct := 0
	if totalKnown > 0 {
		pct = totalComplete * 100 / totalKnown
	}
	createdAt, _ := parseSQLiteTime(created)
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	return progress.Snapshot{
		RunID:         runID,
		Status:        status,
		OverallPct:    pct,
		Elapsed:       time.Since(createdAt).Round(time.Second),
		RecoveryState: recoveryState(ctx, db, runID),
		Stages:        stages,
		CacheHits:     cacheHits(ctx, db, runID),
		ETA:           aggregateETA(stages),
	}, nil
}

func (s Store) ListRuns(ctx context.Context) ([]Run, error) {
	runsDir := filepath.Join(s.DataDir, "runs")
	entries, err := os.ReadDir(runsDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	runs := make([]Run, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		runID := entry.Name()
		dbPath := filepath.Join(runsDir, runID, RunDBName)
		if _, err := os.Stat(dbPath); err != nil {
			continue
		}
		db, err := openSQLite(dbPath)
		if err != nil {
			return nil, err
		}
		var run Run
		var created string
		err = db.QueryRowContext(ctx, `SELECT id, status, created_at FROM runs LIMIT 1`).Scan(&run.ID, &run.Status, &created)
		if err == nil {
			err = recoverLeases(ctx, db, run.ID)
		}
		db.Close()
		if err != nil {
			return nil, err
		}
		run.Dir = filepath.Join(runsDir, run.ID)
		run.CreatedAt, _ = parseSQLiteTime(created)
		runs = append(runs, run)
	}
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].CreatedAt.After(runs[j].CreatedAt)
	})
	return runs, nil
}

func (s Store) SetRunStatus(ctx context.Context, runID, status string) error {
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET status=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`, status, runID); err != nil {
		return err
	}
	if status == "cancelling" || status == "cancelled" {
		for _, table := range []string{"warc_tasks", "wat_tasks", "graph_tasks", "availability_tasks"} {
			stmt := fmt.Sprintf(`UPDATE %s SET state='cancelled', lease_owner=NULL, leased_until=NULL, updated_at=CURRENT_TIMESTAMP WHERE run_id=? AND state IN ('pending','leased','running','failed_retryable')`, table)
			if _, err := tx.ExecContext(ctx, stmt, runID); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,event_type,severity,component,message) VALUES(?,?,?,?,?)`, runID, "run_cancel_requested", "warn", "orchestrator", "pending and leased tasks cancelled"); err != nil {
			return err
		}
	}
	if status != "cancelling" && status != "cancelled" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,event_type,severity,component,message,payload_json) VALUES(?,?,?,?,?,?)`, runID, "run_status_changed", "info", "orchestrator", "run status changed", fmt.Sprintf(`{"status":%q}`, status)); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if isTerminalStatus(status) {
		if err := s.RecalculateProgress(ctx, runID); err != nil {
			return err
		}
		return s.SealEventLog(ctx, runID)
	}
	return nil
}

func (s Store) RecalculateProgress(ctx context.Context, runID string) error {
	return retrySQLiteLocked(ctx, func() error {
		db, err := s.openRun(ctx, runID)
		if err != nil {
			return err
		}
		defer db.Close()
		return recalculateProgress(ctx, db, runID, time.Now().UTC())
	})
}

func (s Store) RunStatus(ctx context.Context, runID string) (string, error) {
	var status string
	err := retrySQLiteLocked(ctx, func() error {
		db, err := s.openRun(ctx, runID)
		if err != nil {
			return err
		}
		defer db.Close()
		return db.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=?`, runID).Scan(&status)
	})
	return status, err
}

func (s Store) UpdateCrawlResolution(ctx context.Context, runID, selectionValue, resolvedID, metadataJSON string) error {
	if selectionValue == "" || resolvedID == "" {
		return fmt.Errorf("selection value and resolved id are required")
	}
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return err
	}
	defer db.Close()
	result, err := db.ExecContext(ctx, `UPDATE run_crawl_selections SET resolved_id=?, metadata_json=COALESCE(NULLIF(?,''), metadata_json) WHERE run_id=? AND selection_value=?`, resolvedID, metadataJSON, runID, selectionValue)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("crawl selection %s not found for run %s", selectionValue, runID)
	}
	return nil
}

func (s Store) EnsureRunnable(ctx context.Context, runID string) error {
	status, err := s.RunStatus(ctx, runID)
	if err != nil {
		return err
	}
	switch status {
	case "running", "created", "resume_requested":
		if status == "resume_requested" {
			return s.SetRunStatus(ctx, runID, "running")
		}
		return nil
	case "pause_requested":
		if err := s.SetRunStatus(ctx, runID, "paused"); err != nil {
			return err
		}
		return ErrRunPaused
	case "paused":
		return ErrRunPaused
	case "cancelling":
		if err := s.SetRunStatus(ctx, runID, "cancelled"); err != nil {
			return err
		}
		return ErrRunCancelled
	case "cancelled":
		return ErrRunCancelled
	case "completed":
		return ErrRunCompleted
	default:
		return fmt.Errorf("run %s is not runnable in status %s", runID, status)
	}
}

func (s Store) RunPlan(ctx context.Context, runID string) (RunPlan, error) {
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return RunPlan{}, err
	}
	defer db.Close()
	plan := RunPlan{ID: runID, Config: config.Default()}
	var cfgJSON string
	if err := db.QueryRowContext(ctx, `SELECT config_json FROM runs WHERE id=?`, runID).Scan(&cfgJSON); err != nil {
		return RunPlan{}, err
	}
	if err := json.Unmarshal([]byte(cfgJSON), &plan.Config); err != nil {
		return RunPlan{}, err
	}
	if plan.Config.DataDirectory == "" {
		plan.Config.DataDirectory = s.DataDir
	}
	sourceRows, err := db.QueryContext(ctx, `SELECT source_domain FROM run_source_domains WHERE run_id=? ORDER BY source_domain`, runID)
	if err != nil {
		return RunPlan{}, err
	}
	for sourceRows.Next() {
		var source string
		if err := sourceRows.Scan(&source); err != nil {
			sourceRows.Close()
			return RunPlan{}, err
		}
		plan.Sources = append(plan.Sources, source)
	}
	sourceRows.Close()
	crawlRows, err := db.QueryContext(ctx, `SELECT resolved_id FROM run_crawl_selections WHERE run_id=? ORDER BY resolved_id`, runID)
	if err != nil {
		return RunPlan{}, err
	}
	for crawlRows.Next() {
		var crawl string
		if err := crawlRows.Scan(&crawl); err != nil {
			crawlRows.Close()
			return RunPlan{}, err
		}
		plan.Crawls = append(plan.Crawls, crawl)
	}
	crawlRows.Close()
	modeRows, err := db.QueryContext(ctx, `SELECT mode FROM extraction_modes WHERE run_id=? AND enabled=1 ORDER BY mode`, runID)
	if err != nil {
		return RunPlan{}, err
	}
	for modeRows.Next() {
		var mode string
		if err := modeRows.Scan(&mode); err != nil {
			modeRows.Close()
			return RunPlan{}, err
		}
		plan.Modes = append(plan.Modes, mode)
	}
	modeRows.Close()
	return plan, nil
}

func (s Store) LeaseTasks(ctx context.Context, runID, table, owner string, limit int, leaseFor time.Duration) ([]LeasedTask, error) {
	if !validTaskTable(table) {
		return nil, fmt.Errorf("invalid task table: %s", table)
	}
	if limit <= 0 {
		limit = 1
	}
	if owner == "" {
		return nil, fmt.Errorf("lease owner is required")
	}
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	query := fmt.Sprintf(`SELECT id FROM %s WHERE run_id=? AND state IN ('pending','failed_retryable') AND attempts < max_attempts ORDER BY created_at, id LIMIT ?`, table)
	rows, err := tx.QueryContext(ctx, query, runID, limit)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	leasedUntil := time.Now().UTC().Add(leaseFor)
	for _, id := range ids {
		stmt := fmt.Sprintf(`UPDATE %s SET state='leased', attempts=attempts+1, lease_owner=?, leased_until=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`, table)
		if _, err := tx.ExecContext(ctx, stmt, owner, sqliteTime(leasedUntil), id); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	leased := make([]LeasedTask, 0, len(ids))
	for _, id := range ids {
		leased = append(leased, LeasedTask{ID: id, Table: table, State: "leased", LeaseOwner: owner, LeasedUntil: leasedUntil})
	}
	return leased, nil
}

func (s Store) LeaseWARCTasks(ctx context.Context, runID, owner string, limit int, leaseFor time.Duration) ([]LeasedWARCTask, error) {
	var out []LeasedWARCTask
	err := retrySQLiteLocked(ctx, func() error {
		var err error
		out, err = s.leaseWARCTasksOnce(ctx, runID, owner, limit, leaseFor)
		return err
	})
	return out, err
}

func (s Store) leaseWARCTasksOnce(ctx context.Context, runID, owner string, limit int, leaseFor time.Duration) ([]LeasedWARCTask, error) {
	if limit <= 0 {
		limit = 1
	}
	if owner == "" {
		return nil, fmt.Errorf("lease owner is required")
	}
	runWriteMu.Lock()
	defer runWriteMu.Unlock()
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id FROM warc_tasks WHERE run_id=? AND state IN ('pending','failed_retryable') AND attempts < max_attempts AND length > 0 ORDER BY created_at, id LIMIT ?`, runID, limit)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	leasedUntil := time.Now().UTC().Add(leaseFor)
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `UPDATE warc_tasks SET state='leased', attempts=attempts+1, lease_owner=?, leased_until=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`, owner, sqliteTime(leasedUntil), id); err != nil {
			return nil, err
		}
	}
	out := make([]LeasedWARCTask, 0, len(ids))
	for _, id := range ids {
		var item LeasedWARCTask
		item.LeasedTask = LeasedTask{ID: id, Table: "warc_tasks", State: "leased", LeaseOwner: owner, LeasedUntil: leasedUntil}
		err := tx.QueryRowContext(ctx, `SELECT wt.attempts, wt.source_domain, wt.crawl_id, COALESCE(cr.url,''), COALESCE(cr.timestamp,''), wt.filename, wt.offset, wt.length
FROM warc_tasks wt
LEFT JOIN cdx_records cr ON cr.id = wt.cdx_record_id
WHERE wt.run_id=? AND wt.id=?`, runID, id).Scan(&item.Attempts, &item.SourceDomain, &item.CrawlID, &item.SourceURL, &item.Timestamp, &item.Filename, &item.Offset, &item.Length)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, rows.Err()
}

func (s Store) LeaseWATTasks(ctx context.Context, runID, owner string, limit int, leaseFor time.Duration) ([]LeasedWATTask, error) {
	var out []LeasedWATTask
	err := retrySQLiteLocked(ctx, func() error {
		var err error
		out, err = s.leaseWATTasksOnce(ctx, runID, owner, limit, leaseFor)
		return err
	})
	return out, err
}

func (s Store) leaseWATTasksOnce(ctx context.Context, runID, owner string, limit int, leaseFor time.Duration) ([]LeasedWATTask, error) {
	if limit <= 0 {
		limit = 1
	}
	if owner == "" {
		return nil, fmt.Errorf("lease owner is required")
	}
	runWriteMu.Lock()
	defer runWriteMu.Unlock()
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id FROM wat_tasks WHERE run_id=? AND state IN ('pending','failed_retryable') AND attempts < max_attempts AND wat_filename <> 'pending-wat-sidecar' ORDER BY created_at, id LIMIT ?`, runID, limit)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	leasedUntil := time.Now().UTC().Add(leaseFor)
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `UPDATE wat_tasks SET state='leased', attempts=attempts+1, lease_owner=?, leased_until=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`, owner, sqliteTime(leasedUntil), id); err != nil {
			return nil, err
		}
	}
	out := make([]LeasedWATTask, 0, len(ids))
	for _, id := range ids {
		var item LeasedWATTask
		item.LeasedTask = LeasedTask{ID: id, Table: "wat_tasks", State: "leased", LeaseOwner: owner, LeasedUntil: leasedUntil}
		if err := tx.QueryRowContext(ctx, `SELECT attempts, crawl_id, wat_filename, source_domain_set_hash FROM wat_tasks WHERE run_id=? AND id=?`, runID, id).Scan(&item.Attempts, &item.CrawlID, &item.WATFilename, &item.SourceDomain); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, rows.Err()
}

func (s Store) AddGraphReleaseTask(ctx context.Context, runID, releaseID, vertexPath, edgePath string, sourceDomains []string) error {
	if releaseID == "" || vertexPath == "" || edgePath == "" {
		return fmt.Errorf("graph release, vertex path, and edge path are required")
	}
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return err
	}
	defer db.Close()
	sourceHash := strings.Join(sourceDomains, ",") + "|vertex=" + vertexPath
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	maxAttempts, err := runTaskMaxAttempts(ctx, tx, runID)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO graph_tasks(run_id,graph_release_id,task_type,file_path,source_domain_set_hash,max_attempts) VALUES(?,?,?,?,?,?)`, runID, releaseID, "edge_scan", edgePath, sourceHash, maxAttempts); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,event_type,severity,component,message,payload_json) VALUES(?,?,?,?,?,?)`, runID, "graph_release_selected", "info", "graph", "graph release selected", fmt.Sprintf(`{"releaseId":%q,"vertexPath":%q,"edgePath":%q}`, releaseID, vertexPath, edgePath)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s Store) LeaseGraphTasks(ctx context.Context, runID, owner string, limit int, leaseFor time.Duration) ([]LeasedGraphTask, error) {
	var out []LeasedGraphTask
	err := retrySQLiteLocked(ctx, func() error {
		var err error
		out, err = s.leaseGraphTasksOnce(ctx, runID, owner, limit, leaseFor)
		return err
	})
	return out, err
}

func (s Store) leaseGraphTasksOnce(ctx context.Context, runID, owner string, limit int, leaseFor time.Duration) ([]LeasedGraphTask, error) {
	if limit <= 0 {
		limit = 1
	}
	if owner == "" {
		return nil, fmt.Errorf("lease owner is required")
	}
	runWriteMu.Lock()
	defer runWriteMu.Unlock()
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id FROM graph_tasks WHERE run_id=? AND state IN ('pending','failed_retryable') AND attempts < max_attempts AND file_path <> 'pending-graph-release' ORDER BY created_at, id LIMIT ?`, runID, limit)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	leasedUntil := time.Now().UTC().Add(leaseFor)
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `UPDATE graph_tasks SET state='leased', attempts=attempts+1, lease_owner=?, leased_until=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`, owner, sqliteTime(leasedUntil), id); err != nil {
			return nil, err
		}
	}
	out := make([]LeasedGraphTask, 0, len(ids))
	for _, id := range ids {
		var item LeasedGraphTask
		item.LeasedTask = LeasedTask{ID: id, Table: "graph_tasks", State: "leased", LeaseOwner: owner, LeasedUntil: leasedUntil}
		var sourceHash string
		if err := tx.QueryRowContext(ctx, `SELECT attempts, graph_release_id, task_type, file_path, source_domain_set_hash FROM graph_tasks WHERE run_id=? AND id=?`, runID, id).Scan(&item.Attempts, &item.ReleaseID, &item.TaskType, &item.FilePath, &sourceHash); err != nil {
			return nil, err
		}
		item.SourceDomains, item.VertexPath = parseGraphSourceHash(sourceHash)
		out = append(out, item)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, rows.Err()
}

func (s Store) CompleteTask(ctx context.Context, runID, table string, id int64, success bool, lastError string) error {
	return retrySQLiteLocked(ctx, func() error {
		return s.completeTaskOnce(ctx, runID, table, id, success, lastError)
	})
}

func (s Store) completeTaskOnce(ctx context.Context, runID, table string, id int64, success bool, lastError string) error {
	if !validTaskTable(table) {
		return fmt.Errorf("invalid task table: %s", table)
	}
	runWriteMu.Lock()
	defer runWriteMu.Unlock()
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return err
	}
	defer db.Close()
	if success {
		stmt := fmt.Sprintf(`UPDATE %s SET state='succeeded', lease_owner=NULL, leased_until=NULL, last_error=?, updated_at=CURRENT_TIMESTAMP WHERE run_id=? AND id=?`, table)
		_, err = db.ExecContext(ctx, stmt, lastError, runID, id)
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt := fmt.Sprintf(`UPDATE %s SET state=CASE WHEN attempts >= max_attempts THEN 'failed_final' ELSE 'failed_retryable' END, lease_owner=NULL, leased_until=NULL, last_error=?, updated_at=CURRENT_TIMESTAMP WHERE run_id=? AND id=?`, table)
	if _, err := tx.ExecContext(ctx, stmt, lastError, runID, id); err != nil {
		return err
	}
	if err := recordTaskFailure(ctx, tx, runID, table, id, lastError, true); err != nil {
		return err
	}
	return tx.Commit()
}

func (s Store) FailTaskFinal(ctx context.Context, runID, table string, id int64, lastError string) error {
	return retrySQLiteLocked(ctx, func() error {
		return s.failTaskFinalOnce(ctx, runID, table, id, lastError)
	})
}

func (s Store) failTaskFinalOnce(ctx context.Context, runID, table string, id int64, lastError string) error {
	if !validTaskTable(table) {
		return fmt.Errorf("invalid task table: %s", table)
	}
	runWriteMu.Lock()
	defer runWriteMu.Unlock()
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt := fmt.Sprintf(`UPDATE %s SET state='failed_final', lease_owner=NULL, leased_until=NULL, last_error=?, updated_at=CURRENT_TIMESTAMP WHERE run_id=? AND id=?`, table)
	if _, err := tx.ExecContext(ctx, stmt, lastError, runID, id); err != nil {
		return err
	}
	if err := recordTaskFailure(ctx, tx, runID, table, id, lastError, false); err != nil {
		return err
	}
	return tx.Commit()
}

func (s Store) StartTask(ctx context.Context, runID, table string, id int64) error {
	return retrySQLiteLocked(ctx, func() error {
		return s.startTaskOnce(ctx, runID, table, id)
	})
}

func (s Store) startTaskOnce(ctx context.Context, runID, table string, id int64) error {
	if !validTaskTable(table) {
		return fmt.Errorf("invalid task table: %s", table)
	}
	runWriteMu.Lock()
	defer runWriteMu.Unlock()
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return err
	}
	defer db.Close()
	stmt := fmt.Sprintf(`UPDATE %s SET state='running', updated_at=CURRENT_TIMESTAMP WHERE run_id=? AND id=? AND state='leased'`, table)
	result, err := db.ExecContext(ctx, stmt, runID, id)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("task %s:%d is not leased", table, id)
	}
	return nil
}

func (s Store) AddCDXRecords(ctx context.Context, runID, sourceDomain, crawlID string, records []commoncrawl.CDXRecord) error {
	if len(records) == 0 {
		return nil
	}
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	maxAttempts, err := runTaskMaxAttempts(ctx, tx, runID)
	if err != nil {
		return err
	}
	inserted := 0
	for _, record := range records {
		length := parseInt(record.Length)
		offset := parseInt(record.Offset)
		result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO cdx_records(run_id,source_domain,crawl_id,url,urlkey,timestamp,mime,status,digest,length,offset,filename,raw_json)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			runID, sourceDomain, crawlID, record.URL, record.URLKey, record.Timestamp, record.Mime, parseInt(record.Status), record.Digest, length, offset, record.Filename, record.RawJSON)
		if err != nil {
			return err
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			continue
		}
		inserted++
		var cdxID int64
		if err := tx.QueryRowContext(ctx, `SELECT id FROM cdx_records WHERE run_id=? AND crawl_id=? AND urlkey=? AND timestamp=?`, runID, crawlID, record.URLKey, record.Timestamp).Scan(&cdxID); err != nil {
			return err
		}
		if record.Filename != "" && length > 0 {
			if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO warc_tasks(run_id,crawl_id,source_domain,cdx_record_id,filename,offset,length,max_attempts) VALUES(?,?,?,?,?,?,?,?)`, runID, crawlID, sourceDomain, cdxID, record.Filename, offset, length, maxAttempts); err != nil {
				return err
			}
			if watPath, ok := commoncrawl.WATPath(record.Filename); ok {
				if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO wat_tasks(run_id,crawl_id,source_domain_set_hash,wat_filename,max_attempts) VALUES(?,?,?,?,?)`, runID, crawlID, sourceDomain, watPath, maxAttempts); err != nil {
					return err
				}
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE progress_counters SET completed_units=completed_units+?, total_known_units=MAX(total_known_units, completed_units+?), updated_at=CURRENT_TIMESTAMP WHERE run_id=? AND stage='discovery'`, inserted, inserted, runID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,event_type,severity,component,message,payload_json) VALUES(?,?,?,?,?,?)`, runID, "cdx_records_ingested", "info", "commoncrawl", "cdx records ingested", fmt.Sprintf(`{"sourceDomain":%q,"crawlId":%q,"count":%d}`, sourceDomain, crawlID, inserted)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s Store) CDXPage(ctx context.Context, runID, sourceDomain, crawlID, pattern string, page int) (CDXPageState, bool, error) {
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return CDXPageState{}, false, err
	}
	defer db.Close()
	var item CDXPageState
	item.SourceDomain = sourceDomain
	item.CrawlID = crawlID
	item.Pattern = pattern
	item.Page = page
	err = db.QueryRowContext(ctx, `SELECT page_size,state,attempts,record_count,COALESCE(last_error,'') FROM cdx_discovery_pages WHERE run_id=? AND source_domain=? AND crawl_id=? AND pattern=? AND page=?`, runID, sourceDomain, crawlID, pattern, page).Scan(&item.PageSize, &item.State, &item.Attempts, &item.RecordCount, &item.LastError)
	if err == sql.ErrNoRows {
		return item, false, nil
	}
	if err != nil {
		return CDXPageState{}, false, err
	}
	return item, true, nil
}

func (s Store) StartCDXPage(ctx context.Context, runID, sourceDomain, crawlID, pattern string, page, pageSize int) error {
	return retrySQLiteLocked(ctx, func() error {
		runWriteMu.Lock()
		defer runWriteMu.Unlock()
		db, err := s.openRun(ctx, runID)
		if err != nil {
			return err
		}
		defer db.Close()
		_, err = db.ExecContext(ctx, `INSERT INTO cdx_discovery_pages(run_id,source_domain,crawl_id,pattern,page,page_size,state,attempts,updated_at)
VALUES(?,?,?,?,?,?, 'running',1,CURRENT_TIMESTAMP)
ON CONFLICT(run_id,source_domain,crawl_id,pattern,page) DO UPDATE SET
  page_size=excluded.page_size,
  state='running',
  attempts=attempts+1,
  last_error=NULL,
  updated_at=CURRENT_TIMESTAMP
WHERE state <> 'succeeded'`,
			runID, sourceDomain, crawlID, pattern, page, pageSize)
		return err
	})
}

func (s Store) CompleteCDXPage(ctx context.Context, runID, sourceDomain, crawlID, pattern string, page, pageSize, recordCount int, success bool, lastError string) error {
	return retrySQLiteLocked(ctx, func() error {
		runWriteMu.Lock()
		defer runWriteMu.Unlock()
		db, err := s.openRun(ctx, runID)
		if err != nil {
			return err
		}
		defer db.Close()
		state := "succeeded"
		if !success {
			state = "failed_retryable"
		}
		_, err = db.ExecContext(ctx, `INSERT INTO cdx_discovery_pages(run_id,source_domain,crawl_id,pattern,page,page_size,state,attempts,record_count,last_error,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
ON CONFLICT(run_id,source_domain,crawl_id,pattern,page) DO UPDATE SET
  page_size=excluded.page_size,
  state=excluded.state,
  record_count=excluded.record_count,
  last_error=excluded.last_error,
  updated_at=CURRENT_TIMESTAMP`,
			runID, sourceDomain, crawlID, pattern, page, pageSize, state, 1, recordCount, nullEmpty(lastError))
		return err
	})
}

func (s Store) AddLinkObservations(ctx context.Context, runID string, observations []extraction.LinkObservation, cfg config.Config) error {
	if len(observations) == 0 {
		return nil
	}
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, obs := range observations {
		if obs.RegistrableDomain == "" || obs.SourceDomain == "" {
			continue
		}
		mode := obs.Mode
		if mode == "" {
			mode = "warc"
		}
		ok, err := upsertCandidate(ctx, tx, runID, obs.RegistrableDomain, mode, cfg, 0, 1, 0)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO candidate_sources(run_id,domain,source_domain,mode,crawl_or_graph_id,source_url_sample,anchor_sample,seen_count,last_seen_at)
VALUES(?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
ON CONFLICT(run_id,domain,source_domain,mode,crawl_or_graph_id) DO UPDATE SET
  seen_count=seen_count+excluded.seen_count,
  source_url_sample=COALESCE(candidate_sources.source_url_sample, excluded.source_url_sample),
  anchor_sample=COALESCE(candidate_sources.anchor_sample, excluded.anchor_sample),
  last_seen_at=CURRENT_TIMESTAMP`,
			runID, obs.RegistrableDomain, obs.SourceDomain, mode, obs.CrawlID, obs.SourceURL, obs.AnchorText, 1); err != nil {
			return err
		}
		if mode == "wat" || mode == "warc" {
			if err := recordSourcePage(ctx, tx, runID, obs.RegistrableDomain, obs.SourceDomain, mode, obs.CrawlID, obs.SourceURL); err != nil {
				return err
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE progress_counters SET completed_units=completed_units+?, updated_at=CURRENT_TIMESTAMP WHERE run_id=? AND stage='extraction'`, len(observations), runID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,event_type,severity,component,message,payload_json) VALUES(?,?,?,?,?,?)`, runID, "observations_ingested", "info", "storage", "link observations ingested", fmt.Sprintf(`{"count":%d}`, len(observations))); err != nil {
		return err
	}
	return tx.Commit()
}

func (s Store) AddLinkObservationBatch(ctx context.Context, runID, batchKey string, observations []extraction.LinkObservation, cfg config.Config, manifest artifact.Manifest) error {
	return retrySQLiteLocked(ctx, func() error {
		return s.addLinkObservationBatchOnce(ctx, runID, batchKey, observations, cfg, manifest)
	})
}

func (s Store) addLinkObservationBatchOnce(ctx context.Context, runID, batchKey string, observations []extraction.LinkObservation, cfg config.Config, manifest artifact.Manifest) error {
	if batchKey == "" {
		return fmt.Errorf("batch key is required")
	}
	if len(observations) == 0 {
		return nil
	}
	runWriteMu.Lock()
	defer runWriteMu.Unlock()
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var existing int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM observation_batches WHERE run_id=? AND batch_key=?`, runID, batchKey).Scan(&existing)
	if err == nil {
		return tx.Commit()
	}
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	for _, obs := range observations {
		if obs.RegistrableDomain == "" || obs.SourceDomain == "" {
			continue
		}
		mode := obs.Mode
		if mode == "" {
			mode = "warc"
		}
		ok, err := upsertCandidate(ctx, tx, runID, obs.RegistrableDomain, mode, cfg, 0, 1, 0)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO candidate_sources(run_id,domain,source_domain,mode,crawl_or_graph_id,source_url_sample,anchor_sample,seen_count,last_seen_at)
VALUES(?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
ON CONFLICT(run_id,domain,source_domain,mode,crawl_or_graph_id) DO UPDATE SET
  seen_count=seen_count+excluded.seen_count,
  source_url_sample=COALESCE(candidate_sources.source_url_sample, excluded.source_url_sample),
  anchor_sample=COALESCE(candidate_sources.anchor_sample, excluded.anchor_sample),
  last_seen_at=CURRENT_TIMESTAMP`,
			runID, obs.RegistrableDomain, obs.SourceDomain, mode, obs.CrawlID, obs.SourceURL, obs.AnchorText, 1); err != nil {
			return err
		}
		if mode == "wat" || mode == "warc" {
			if err := recordSourcePage(ctx, tx, runID, obs.RegistrableDomain, obs.SourceDomain, mode, obs.CrawlID, obs.SourceURL); err != nil {
				return err
			}
		}
	}
	if manifest.Path != "" {
		if err := recordArtifactTx(ctx, tx, runID, "link_observations", manifest); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO observation_batches(run_id,batch_key,artifact_path,artifact_type,row_count) VALUES(?,?,?,?,?)`,
		runID, batchKey, nullEmpty(manifest.Path), "link_observations", len(observations)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE progress_counters SET completed_units=completed_units+?, updated_at=CURRENT_TIMESTAMP WHERE run_id=? AND stage='extraction'`, len(observations), runID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,event_type,severity,component,message,payload_json) VALUES(?,?,?,?,?,?)`, runID, "observations_ingested", "info", "storage", "link observations ingested", fmt.Sprintf(`{"count":%d,"batchKey":%q}`, len(observations), batchKey)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s Store) AddGraphObservationBatch(ctx context.Context, runID, batchKey string, observations []graph.CandidateObservation, cfg config.Config) error {
	if batchKey == "" {
		return fmt.Errorf("batch key is required")
	}
	if len(observations) == 0 {
		return nil
	}
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var existing int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM observation_batches WHERE run_id=? AND batch_key=?`, runID, batchKey).Scan(&existing)
	if err == nil {
		return tx.Commit()
	}
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	for _, obs := range observations {
		if obs.TargetDomain == "" || obs.SourceDomain == "" {
			continue
		}
		ok, err := upsertCandidate(ctx, tx, runID, obs.TargetDomain, "web_graph", cfg, obs.EdgeCount, 0, 0)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO candidate_sources(run_id,domain,source_domain,mode,crawl_or_graph_id,seen_count,last_seen_at)
VALUES(?,?,?,?,?,?,CURRENT_TIMESTAMP)
ON CONFLICT(run_id,domain,source_domain,mode,crawl_or_graph_id) DO UPDATE SET
  seen_count=seen_count+excluded.seen_count,
  last_seen_at=CURRENT_TIMESTAMP`,
			runID, obs.TargetDomain, obs.SourceDomain, "web_graph", obs.ReleaseID, max(obs.EdgeCount, 1)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE progress_counters SET completed_units=completed_units+?, updated_at=CURRENT_TIMESTAMP WHERE run_id=? AND stage='extraction'`, len(observations), runID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO observation_batches(run_id,batch_key,artifact_type,row_count) VALUES(?,?,?,?)`,
		runID, batchKey, "graph_candidates", len(observations)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,event_type,severity,component,message,payload_json) VALUES(?,?,?,?,?,?)`, runID, "graph_observations_ingested", "info", "storage", "graph observations ingested", fmt.Sprintf(`{"count":%d}`, len(observations))); err != nil {
		return err
	}
	return tx.Commit()
}

func (s Store) StoreAvailability(ctx context.Context, runID string, record AvailabilityRecord) error {
	return retrySQLiteLocked(ctx, func() error {
		return s.storeAvailabilityOnce(ctx, runID, record)
	})
}

func (s Store) storeAvailabilityOnce(ctx context.Context, runID string, record AvailabilityRecord) error {
	availabilityWriteMu.Lock()
	defer availabilityWriteMu.Unlock()
	if record.Domain == "" {
		return fmt.Errorf("domain is required")
	}
	if record.CheckedAt.IsZero() {
		record.CheckedAt = time.Now().UTC()
	}
	if record.ExpiresAt.IsZero() {
		record.ExpiresAt = record.CheckedAt.Add(availability.DefaultTTLPolicy().TTL(record.Status))
	}
	if record.TLDPolicyCategory == "" {
		record.TLDPolicyCategory = policyCategory(record.Domain, config.Default())
	}
	if err := s.storeCacheAvailability(ctx, record); err != nil {
		return err
	}
	runWriteMu.Lock()
	defer runWriteMu.Unlock()
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, `UPDATE candidate_domains SET
  availability_status=?,
  confidence=?,
  tld_policy_category=?,
  classification_reason=?,
  registrar_provider=?,
  premium=?,
  price_cents=?,
  currency=?,
  cache_hit=?,
  score=?,
  last_checked_at=?
WHERE run_id=? AND domain=?`,
		string(record.Status), string(record.Confidence), record.TLDPolicyCategory, record.Reason, record.Provider, boolInt(record.Premium), nullablePositive(record.PriceCents), nullEmpty(record.Currency), boolInt(record.CacheHit), scoreCandidate(record), record.CheckedAt.Format(time.RFC3339), runID, record.Domain)
	return err
}

func (s Store) RecordArtifact(ctx context.Context, runID, artifactType string, manifest artifact.Manifest) error {
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := recordArtifactTx(ctx, tx, runID, artifactType, manifest); err != nil {
		return err
	}
	return tx.Commit()
}

func recordArtifactTx(ctx context.Context, tx *sql.Tx, runID, artifactType string, manifest artifact.Manifest) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_files(run_id,artifact_type,path,format,compression,row_count,byte_count,sha256,status,sealed_at)
VALUES(?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
ON CONFLICT(run_id,path) DO UPDATE SET
  artifact_type=excluded.artifact_type,
  format=excluded.format,
  compression=excluded.compression,
  row_count=excluded.row_count,
  byte_count=excluded.byte_count,
  sha256=excluded.sha256,
  status=excluded.status,
  sealed_at=CURRENT_TIMESTAMP`,
		runID, artifactType, manifest.Path, manifest.Format, manifest.Compression, manifest.RowCount, manifest.ByteCount, manifest.SHA256, "sealed"); err != nil {
		return err
	}
	if artifactType == "export" {
		if _, err := tx.ExecContext(ctx, `UPDATE progress_counters SET completed_units=MAX(completed_units,1), total_known_units=MAX(total_known_units,1), updated_at=CURRENT_TIMESTAMP WHERE run_id=? AND stage='export'`, runID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,event_type,severity,component,message,payload_json) VALUES(?,?,?,?,?,?)`, runID, "export_completed", "info", "export", "export artifact sealed", fmt.Sprintf(`{"path":%q,"format":%q,"rowCount":%d}`, manifest.Path, manifest.Format, manifest.RowCount)); err != nil {
			return err
		}
	}
	return nil
}

type eventLogRow struct {
	ID        int64           `json:"id"`
	RunID     string          `json:"runId"`
	Type      string          `json:"eventType"`
	Severity  string          `json:"severity"`
	Component string          `json:"component,omitempty"`
	Message   string          `json:"message"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	CreatedAt string          `json:"createdAt"`
}

func (s Store) SealEventLog(ctx context.Context, runID string) error {
	runDir, err := s.RunDir(runID)
	if err != nil {
		return err
	}
	path := filepath.Join(runDir, "logs", "events.jsonl.zst")
	tmp := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT id, run_id, event_type, severity, COALESCE(component,''), COALESCE(message,''), COALESCE(payload_json,''), created_at FROM events WHERE run_id=? ORDER BY created_at, id`, runID)
	if err != nil {
		return err
	}
	defer rows.Close()
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	closed := false
	closeFile := func() error {
		if closed {
			return nil
		}
		closed = true
		return file.Close()
	}
	defer func() {
		_ = closeFile()
		_ = os.Remove(tmp)
	}()
	writer, err := zstd.NewWriter(file)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(writer)
	count := 0
	for rows.Next() {
		var row eventLogRow
		var payload string
		if err := rows.Scan(&row.ID, &row.RunID, &row.Type, &row.Severity, &row.Component, &row.Message, &payload, &row.CreatedAt); err != nil {
			writer.Close()
			return err
		}
		if payload != "" {
			row.Payload = json.RawMessage(payload)
		}
		if err := enc.Encode(row); err != nil {
			writer.Close()
			return err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		writer.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	if err := closeFile(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	manifest, err := artifact.ManifestFile(path, "jsonl", "zstd", count)
	if err != nil {
		return err
	}
	return s.RecordArtifact(ctx, runID, "event_log", manifest)
}

func (s Store) CachedAvailability(ctx context.Context, domain string, now time.Time) (AvailabilityRecord, bool, error) {
	var record AvailabilityRecord
	var ok bool
	err := retrySQLiteLocked(ctx, func() error {
		var err error
		record, ok, err = s.cachedAvailabilityOnce(ctx, domain, now)
		return err
	})
	return record, ok, err
}

func (s Store) cachedAvailabilityOnce(ctx context.Context, domain string, now time.Time) (AvailabilityRecord, bool, error) {
	db, err := openSQLite(filepath.Join(s.DataDir, "cache", CacheDBName))
	if err != nil {
		return AvailabilityRecord{}, false, err
	}
	defer db.Close()
	var record AvailabilityRecord
	var status, confidence, policy, reason, checkedAt, expiresAt string
	err = db.QueryRowContext(ctx, `SELECT last_status, COALESCE(last_confidence,'low'), COALESCE(tld_policy_category,'unsupported_unknown'), COALESCE(classification_reason,''), COALESCE(last_checked_at,''), COALESCE(next_check_at,'') FROM domains WHERE domain=? AND next_check_at > ?`, domain, now.Format(time.RFC3339)).Scan(&status, &confidence, &policy, &reason, &checkedAt, &expiresAt)
	if err == sql.ErrNoRows {
		return AvailabilityRecord{}, false, nil
	}
	if err != nil {
		return AvailabilityRecord{}, false, err
	}
	record.Domain = domain
	record.Status = availability.Status(status)
	record.Confidence = availability.Confidence(confidence)
	record.TLDPolicyCategory = policy
	record.Reason = reason
	record.CheckedAt, _ = parseSQLiteTime(checkedAt)
	record.ExpiresAt, _ = parseSQLiteTime(expiresAt)
	record.CacheHit = true
	var premium int
	if err := db.QueryRowContext(ctx, `SELECT provider, COALESCE(premium,0), COALESCE(price_cents,0), COALESCE(currency,'') FROM registrar_cache WHERE domain=? ORDER BY checked_at DESC LIMIT 1`, domain).Scan(&record.Provider, &premium, &record.PriceCents, &record.Currency); err == nil {
		record.Premium = premium == 1
	}
	return record, true, nil
}

func (s Store) TLDPolicyCategory(ctx context.Context, domain string) (string, bool, error) {
	var category string
	var ok bool
	err := retrySQLiteLocked(ctx, func() error {
		var err error
		category, ok, err = s.tldPolicyCategoryOnce(ctx, domain)
		return err
	})
	return category, ok, err
}

func (s Store) tldPolicyCategoryOnce(ctx context.Context, domain string) (string, bool, error) {
	tld := domain
	if i := strings.LastIndexByte(domain, '.'); i >= 0 {
		tld = domain[i+1:]
	}
	tld = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(tld)), ".")
	if tld == "" {
		return "", false, nil
	}
	db, err := openSQLite(filepath.Join(s.DataDir, "cache", CacheDBName))
	if err != nil {
		return "", false, err
	}
	defer db.Close()
	var category string
	if err := db.QueryRowContext(ctx, `SELECT policy_category FROM tld_support WHERE tld=?`, tld).Scan(&category); err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}
	return category, true, nil
}

func (s Store) Results(ctx context.Context, runID string) ([]Candidate, error) {
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT domain, COALESCE(availability_status,'ambiguous'), COALESCE(confidence,'low'), COALESCE(tld_policy_category,'unsupported_unknown'), source_page_count, graph_edge_count, outbound_url_count, seen_in_graph, seen_in_wat, seen_in_warc, score,
COALESCE(registrar_provider,''), premium, COALESCE(price_cents,0), COALESCE(currency,''),
COALESCE((SELECT COALESCE(source_url_sample, source_domain || ' ' || mode) FROM candidate_sources cs WHERE cs.run_id=candidate_domains.run_id AND cs.domain=candidate_domains.domain ORDER BY seen_count DESC LIMIT 1),'')
FROM candidate_domains WHERE run_id=? ORDER BY score DESC, domain LIMIT 500`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Candidate
	for rows.Next() {
		var c Candidate
		var graph, wat, warc int
		var premium int
		var priceCents int
		var currency string
		if err := rows.Scan(&c.Domain, &c.Status, &c.Confidence, &c.TLDPolicyCategory, &c.SourcePageCount, &c.GraphEdgeCount, &c.OutboundURLCount, &graph, &wat, &warc, &c.Score, &c.RegistrarProvider, &premium, &priceCents, &currency, &c.EvidenceSample); err != nil {
			return nil, err
		}
		c.EvidenceModes = modesFromBits(graph, wat, warc)
		c.PriceSummary = priceSummary(premium == 1, priceCents, currency)
		c.ManualVerification = "yes"
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s Store) CachedCDXRecords(ctx context.Context, currentRunID, sourceDomain, crawlID, pattern string) ([]commoncrawl.CDXRecord, error) {
	prefix, err := commoncrawl.CDXPatternPrefix(pattern)
	if err != nil {
		return nil, err
	}
	runs, err := s.ListRuns(ctx)
	if err != nil {
		return nil, err
	}
	var out []commoncrawl.CDXRecord
	seen := map[string]bool{}
	for _, run := range runs {
		if run.ID == currentRunID {
			continue
		}
		db, err := s.openRun(ctx, run.ID)
		if err != nil {
			continue
		}
		rows, err := db.QueryContext(ctx, `SELECT COALESCE(urlkey,''),COALESCE(timestamp,''),url,COALESCE(mime,''),'',CAST(status AS TEXT),COALESCE(digest,''),CAST(length AS TEXT),CAST(offset AS TEXT),COALESCE(filename,''),COALESCE(raw_json,'')
FROM cdx_records
WHERE source_domain=? AND crawl_id=? AND urlkey LIKE ?
ORDER BY id`, sourceDomain, crawlID, prefix+"%")
		if err != nil {
			db.Close()
			continue
		}
		for rows.Next() {
			var record commoncrawl.CDXRecord
			if err := rows.Scan(&record.URLKey, &record.Timestamp, &record.URL, &record.Mime, &record.MimeDetected, &record.Status, &record.Digest, &record.Length, &record.Offset, &record.Filename, &record.RawJSON); err != nil {
				rows.Close()
				db.Close()
				return nil, err
			}
			key := record.URLKey + "\x00" + record.Timestamp + "\x00" + record.Filename + "\x00" + record.Offset + "\x00" + record.Length
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, record)
		}
		err = rows.Err()
		rows.Close()
		db.Close()
		if err != nil {
			return nil, err
		}
		if len(out) > 0 {
			return out, nil
		}
	}
	return out, nil
}

func (s Store) CandidateDomains(ctx context.Context, runID string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT domain FROM candidate_domains WHERE run_id=? AND availability_status IS NULL ORDER BY score DESC, domain LIMIT ?`, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var domains []string
	for rows.Next() {
		var domain string
		if err := rows.Scan(&domain); err != nil {
			return nil, err
		}
		domains = append(domains, domain)
	}
	return domains, rows.Err()
}

func (s Store) RunnableTaskCount(ctx context.Context, runID, table string) (int, error) {
	var count int
	err := retrySQLiteLocked(ctx, func() error {
		var err error
		count, err = s.runnableTaskCountOnce(ctx, runID, table)
		return err
	})
	return count, err
}

func (s Store) runnableTaskCountOnce(ctx context.Context, runID, table string) (int, error) {
	if !validTaskTable(table) {
		return 0, fmt.Errorf("invalid task table: %s", table)
	}
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	conditions := `run_id=? AND state IN ('pending','failed_retryable') AND attempts < max_attempts`
	switch table {
	case "warc_tasks":
		conditions += ` AND length > 0`
	case "wat_tasks":
		conditions += ` AND wat_filename <> 'pending-wat-sidecar'`
	case "graph_tasks":
		conditions += ` AND file_path <> 'pending-graph-release'`
	case "availability_tasks":
		conditions = `run_id=? AND state IN ('pending','failed_retryable') AND attempts < max_attempts AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP) AND EXISTS (
  SELECT 1 FROM candidate_domains cd
  WHERE cd.run_id=` + table + `.run_id AND cd.domain=` + table + `.domain AND cd.availability_status IS NULL
)`
	}
	var count int
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s`, table, conditions)
	if err := db.QueryRowContext(ctx, query, runID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s Store) CompletionBlockers(ctx context.Context, runID string) (map[string]int, error) {
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	blockers := map[string]int{}
	add := func(name, query string) error {
		var count int
		if err := db.QueryRowContext(ctx, query, runID).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			blockers[name] = count
		}
		return nil
	}
	activeStates := "('pending','leased','running','failed_retryable')"
	if err := add("cdx_failed_retryable", `SELECT COUNT(*) FROM cdx_discovery_pages WHERE run_id=? AND state='failed_retryable'`); err != nil {
		return nil, err
	}
	if err := add("warc_active", `SELECT COUNT(*) FROM warc_tasks WHERE run_id=? AND length > 0 AND state IN `+activeStates+` AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=warc_tasks.run_id AND mode='warc' AND enabled=1)`); err != nil {
		return nil, err
	}
	if err := add("wat_active", `SELECT COUNT(*) FROM wat_tasks WHERE run_id=? AND wat_filename <> 'pending-wat-sidecar' AND state IN `+activeStates+` AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=wat_tasks.run_id AND mode='wat' AND enabled=1)`); err != nil {
		return nil, err
	}
	if err := add("graph_active", `SELECT COUNT(*) FROM graph_tasks WHERE run_id=? AND file_path <> 'pending-graph-release' AND state IN `+activeStates+` AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=graph_tasks.run_id AND mode='web_graph' AND enabled=1)`); err != nil {
		return nil, err
	}
	if err := add("availability_active", `SELECT COUNT(*) FROM availability_tasks WHERE run_id=? AND state IN `+activeStates+` AND EXISTS (
  SELECT 1 FROM candidate_domains cd
  WHERE cd.run_id=availability_tasks.run_id AND cd.domain=availability_tasks.domain AND cd.availability_status IS NULL
)`); err != nil {
		return nil, err
	}
	cancelledQueries := map[string]string{
		"warc_cancelled":         `SELECT COUNT(*) FROM warc_tasks WHERE run_id=? AND length > 0 AND state='cancelled' AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=warc_tasks.run_id AND mode='warc' AND enabled=1)`,
		"wat_cancelled":          `SELECT COUNT(*) FROM wat_tasks WHERE run_id=? AND wat_filename <> 'pending-wat-sidecar' AND state='cancelled' AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=wat_tasks.run_id AND mode='wat' AND enabled=1)`,
		"graph_cancelled":        `SELECT COUNT(*) FROM graph_tasks WHERE run_id=? AND file_path <> 'pending-graph-release' AND state='cancelled' AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=graph_tasks.run_id AND mode='web_graph' AND enabled=1)`,
		"availability_cancelled": `SELECT COUNT(*) FROM availability_tasks WHERE run_id=? AND state='cancelled' AND EXISTS (SELECT 1 FROM candidate_domains cd WHERE cd.run_id=availability_tasks.run_id AND cd.domain=availability_tasks.domain AND cd.availability_status IS NULL)`,
	}
	for name, query := range cancelledQueries {
		if err := add(name, query); err != nil {
			return nil, err
		}
	}
	return blockers, nil
}

func (s Store) LeaseAvailabilityTasks(ctx context.Context, runID, owner string, limit int, leaseFor time.Duration) ([]LeasedAvailabilityTask, error) {
	var out []LeasedAvailabilityTask
	err := retrySQLiteLocked(ctx, func() error {
		var err error
		out, err = s.leaseAvailabilityTasksOnce(ctx, runID, owner, limit, leaseFor)
		return err
	})
	return out, err
}

func (s Store) leaseAvailabilityTasksOnce(ctx context.Context, runID, owner string, limit int, leaseFor time.Duration) ([]LeasedAvailabilityTask, error) {
	if limit <= 0 {
		limit = 1
	}
	if owner == "" {
		return nil, fmt.Errorf("lease owner is required")
	}
	runWriteMu.Lock()
	defer runWriteMu.Unlock()
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT at.id
FROM availability_tasks at
JOIN candidate_domains cd ON cd.run_id=at.run_id AND cd.domain=at.domain
WHERE at.run_id=?
  AND at.state IN ('pending','failed_retryable')
  AND at.attempts < at.max_attempts
  AND (at.next_attempt_at IS NULL OR at.next_attempt_at <= CURRENT_TIMESTAMP)
  AND cd.availability_status IS NULL
ORDER BY at.created_at, at.id
LIMIT ?`, runID, limit)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	leasedUntil := time.Now().UTC().Add(leaseFor)
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `UPDATE availability_tasks SET state='leased', attempts=attempts+1, lease_owner=?, leased_until=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`, owner, sqliteTime(leasedUntil), id); err != nil {
			return nil, err
		}
	}
	out := make([]LeasedAvailabilityTask, 0, len(ids))
	for _, id := range ids {
		var item LeasedAvailabilityTask
		item.LeasedTask = LeasedTask{ID: id, Table: "availability_tasks", State: "leased", LeaseOwner: owner, LeasedUntil: leasedUntil}
		if err := tx.QueryRowContext(ctx, `SELECT attempts, domain, method, COALESCE(provider,'') FROM availability_tasks WHERE run_id=? AND id=?`, runID, id).Scan(&item.Attempts, &item.Domain, &item.Method, &item.Provider); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, rows.Err()
}

func (s Store) DeferAvailabilityTask(ctx context.Context, runID string, id int64, retryAt time.Time, reason string) error {
	if retryAt.IsZero() {
		retryAt = time.Now().UTC().Add(time.Hour)
	}
	return retrySQLiteLocked(ctx, func() error {
		return s.deferAvailabilityTaskOnce(ctx, runID, id, retryAt, reason)
	})
}

func (s Store) deferAvailabilityTaskOnce(ctx context.Context, runID string, id int64, retryAt time.Time, reason string) error {
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, `UPDATE availability_tasks
SET state='pending',
    attempts=CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
    next_attempt_at=?,
    lease_owner=NULL,
    leased_until=NULL,
    last_error=?,
    updated_at=CURRENT_TIMESTAMP
WHERE run_id=? AND id=?`, sqliteTime(retryAt), reason, runID, id)
	return err
}

func (s Store) RecentEvents(ctx context.Context, runID string, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 20
	}
	db, err := s.openRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT severity, COALESCE(component,''), COALESCE(message,''), created_at FROM events WHERE run_id=? ORDER BY created_at DESC, id DESC LIMIT ?`, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []Event
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.Severity, &event.Component, &event.Message, &event.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s Store) storeCacheAvailability(ctx context.Context, record AvailabilityRecord) error {
	db, err := openSQLite(filepath.Join(s.DataDir, "cache", CacheDBName))
	if err != nil {
		return err
	}
	defer db.Close()
	normalized, err := normalization.NormalizeDomain(record.Domain, nil, nil)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO domains(domain,ascii_domain,unicode_domain,tld,last_status,last_confidence,tld_policy_category,classification_reason,last_checked_at,next_check_at,score,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
ON CONFLICT(domain) DO UPDATE SET
  last_seen_at=CURRENT_TIMESTAMP,
  last_status=excluded.last_status,
  last_confidence=excluded.last_confidence,
  tld_policy_category=excluded.tld_policy_category,
  classification_reason=excluded.classification_reason,
  last_checked_at=excluded.last_checked_at,
  next_check_at=excluded.next_check_at,
  score=excluded.score,
  updated_at=CURRENT_TIMESTAMP`,
		normalized.RegistrableDomain, normalized.ASCII, normalized.Unicode, normalized.TLD, string(record.Status), string(record.Confidence), record.TLDPolicyCategory, record.Reason, record.CheckedAt.Format(time.RFC3339), record.ExpiresAt.Format(time.RFC3339), scoreCandidate(record)); err != nil {
		return err
	}
	summary, _ := json.Marshal(map[string]any{"reason": record.Reason, "provider": record.Provider, "premium": record.Premium, "priceCents": record.PriceCents, "currency": record.Currency})
	if _, err := tx.ExecContext(ctx, `INSERT INTO availability_checks(domain,method,provider,status,confidence,summary_json,checked_at,expires_at) VALUES(?,?,?,?,?,?,?,?)`,
		normalized.RegistrableDomain, "classifier", record.Provider, string(record.Status), string(record.Confidence), string(summary), record.CheckedAt.Format(time.RFC3339), record.ExpiresAt.Format(time.RFC3339)); err != nil {
		return err
	}
	if record.Provider != "" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO registrar_cache(domain,provider,status,available,premium,price_cents,currency,summary_json,checked_at,expires_at)
VALUES(?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(domain,provider) DO UPDATE SET
  status=excluded.status,
  available=excluded.available,
  premium=excluded.premium,
  price_cents=excluded.price_cents,
  currency=excluded.currency,
  summary_json=excluded.summary_json,
  checked_at=excluded.checked_at,
  expires_at=excluded.expires_at`,
			normalized.RegistrableDomain, record.Provider, string(record.Status), boolInt(record.Status == availability.AvailableRegistrarConfirmed), boolInt(record.Premium), record.PriceCents, record.Currency, string(summary), record.CheckedAt.Format(time.RFC3339), record.ExpiresAt.Format(time.RFC3339)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s Store) RunDir(runID string) (string, error) {
	runDir := filepath.Join(s.DataDir, "runs", runID)
	if _, err := os.Stat(filepath.Join(runDir, RunDBName)); err != nil {
		return "", err
	}
	return runDir, nil
}

func (s Store) openRun(ctx context.Context, runID string) (*sql.DB, error) {
	runDir, err := s.RunDir(runID)
	if err != nil {
		return nil, err
	}
	if err := cleanupTemporaryArtifacts(runDir); err != nil {
		return nil, err
	}
	db, err := openSQLite(filepath.Join(runDir, RunDBName))
	if err != nil {
		return nil, err
	}
	if err := ensureRunSchemaCompatibility(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	if err := recoverLeases(ctx, db, runID); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func cleanupTemporaryArtifacts(runDir string) error {
	artifactsDir := filepath.Join(runDir, "artifacts")
	if _, err := os.Stat(artifactsDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return filepath.WalkDir(artifactsDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tmp") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if time.Since(info.ModTime()) < 5*time.Minute {
			return nil
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	})
}

func openSQLite(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=30000",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}

func retrySQLiteLocked(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < 20; attempt++ {
		err = fn()
		if err == nil || !isSQLiteLocked(err) {
			return err
		}
		delay := time.Duration(attempt+1) * 25 * time.Millisecond
		if delay > 250*time.Millisecond {
			delay = 250 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func isSQLiteLocked(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") ||
		strings.Contains(message, "sqlite_busy") ||
		strings.Contains(message, "sqlite_locked")
}

func applySchema(ctx context.Context, db *sql.DB, schemaFile, name string) error {
	if err := runMigrations(ctx, db, []migration{{Version: 1, Name: name, SchemaFile: schemaFile}}); err != nil {
		return err
	}
	if schemaFile == "run_state_schema.sql" {
		if err := ensureRunSchemaCompatibility(ctx, db); err != nil {
			return err
		}
	}
	return nil
}

type migration struct {
	Version    int
	Name       string
	SchemaFile string
}

func runMigrations(ctx context.Context, db *sql.DB, migrations []migration) error {
	for _, item := range migrations {
		applied, err := migrationApplied(ctx, db, item.Version)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		sqlText, err := readSchema(item.SchemaFile)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, sqlText); err != nil {
			tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO schema_migrations(version,name) VALUES(?,?)`, item.Version, item.Name); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func migrationApplied(ctx context.Context, db *sql.DB, version int) (bool, error) {
	var exists int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM schema_migrations WHERE version=?`, version).Scan(&exists)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "no such table") {
		return false, nil
	}
	return false, err
}

func ensureRunSchemaCompatibility(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS observation_batches (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  batch_key TEXT NOT NULL,
  artifact_path TEXT,
  artifact_type TEXT,
  row_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (run_id, batch_key)
)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cdx_discovery_pages (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  source_domain TEXT NOT NULL,
  crawl_id TEXT NOT NULL,
  pattern TEXT NOT NULL,
  page INTEGER NOT NULL,
  page_size INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending','running','succeeded','failed_retryable','failed_final')) DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  record_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (run_id, source_domain, crawl_id, pattern, page)
)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_cdx_pages_state ON cdx_discovery_pages(run_id, state)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS candidate_source_pages (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  domain TEXT NOT NULL,
  source_domain TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('wat','warc')),
  crawl_id TEXT NOT NULL,
  source_url TEXT NOT NULL,
  first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (run_id, domain, source_domain, mode, crawl_id, source_url),
  FOREIGN KEY (run_id, domain) REFERENCES candidate_domains(run_id, domain) ON DELETE CASCADE
)`); err != nil {
		return err
	}
	columns := map[string]string{
		"registrar_provider": "TEXT",
		"premium":            "INTEGER NOT NULL DEFAULT 0 CHECK (premium IN (0,1))",
		"price_cents":        "INTEGER",
		"currency":           "TEXT",
	}
	for name, definition := range columns {
		exists, err := columnExists(ctx, db, "candidate_domains", name)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE candidate_domains ADD COLUMN %s %s`, name, definition)); err != nil {
			return err
		}
	}
	if exists, err := columnExists(ctx, db, "availability_tasks", "next_attempt_at"); err != nil {
		return err
	} else if !exists {
		if _, err := db.ExecContext(ctx, `ALTER TABLE availability_tasks ADD COLUMN next_attempt_at TEXT`); err != nil {
			return err
		}
	}
	return nil
}

func columnExists(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if strings.EqualFold(name, column) {
			return true, nil
		}
	}
	return false, rows.Err()
}

func seedTLDPolicy(ctx context.Context, db *sql.DB, cfg config.Config) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, tld := range cfg.Availability.RestrictedTLDs {
		tld = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(tld)), ".")
		if tld == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO tld_support(tld,policy_category,rdap_supported,registrar_supported,policy_json,updated_at)
VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)
ON CONFLICT(tld) DO UPDATE SET
  policy_category=excluded.policy_category,
  rdap_supported=excluded.rdap_supported,
  registrar_supported=excluded.registrar_supported,
  policy_json=excluded.policy_json,
  updated_at=CURRENT_TIMESTAMP`,
			tld, "restricted", 0, 0, `{"source":"config.restrictedTlds"}`); err != nil {
			return err
		}
	}
	for _, tld := range cfg.Availability.BlockedTLDs {
		tld = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(tld)), ".")
		if tld == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO tld_support(tld,policy_category,rdap_supported,registrar_supported,policy_json,updated_at)
VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)
ON CONFLICT(tld) DO UPDATE SET
  policy_category=excluded.policy_category,
  rdap_supported=excluded.rdap_supported,
  registrar_supported=excluded.registrar_supported,
  policy_json=excluded.policy_json,
  updated_at=CURRENT_TIMESTAMP`,
			tld, "unsupported_unknown", 0, 0, `{"source":"config.blockedTlds"}`); err != nil {
			return err
		}
	}
	for _, tld := range []string{"arpa", "localhost", "example", "invalid", "test"} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO tld_support(tld,policy_category,rdap_supported,registrar_supported,policy_json,updated_at)
VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)
ON CONFLICT(tld) DO UPDATE SET
  policy_category=excluded.policy_category,
  rdap_supported=excluded.rdap_supported,
  registrar_supported=excluded.registrar_supported,
  policy_json=excluded.policy_json,
  updated_at=CURRENT_TIMESTAMP`,
			tld, "reserved_infrastructure", 0, 0, `{"source":"built_in_reserved"}`); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func seedIgnoreRules(ctx context.Context, db *sql.DB, cfg config.Config) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, domain := range cfg.Availability.BlockedDomains {
		domain = strings.ToLower(strings.TrimSpace(domain))
		if domain == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO ignore_rules(rule_type,pattern,reason,enabled) VALUES(?,?,?,1)
ON CONFLICT(rule_type,pattern) DO UPDATE SET reason=excluded.reason, enabled=1`,
			"domain", domain, "config.blockedDomains"); err != nil {
			return err
		}
	}
	for _, tld := range cfg.Availability.BlockedTLDs {
		tld = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(tld)), ".")
		if tld == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO ignore_rules(rule_type,pattern,reason,enabled) VALUES(?,?,?,1)
ON CONFLICT(rule_type,pattern) DO UPDATE SET reason=excluded.reason, enabled=1`,
			"tld", tld, "config.blockedTlds"); err != nil {
			return err
		}
	}
	for _, suffix := range cfg.Availability.BlockedSuffixes {
		suffix = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(suffix)), ".")
		if suffix == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO ignore_rules(rule_type,pattern,reason,enabled) VALUES(?,?,?,1)
ON CONFLICT(rule_type,pattern) DO UPDATE SET reason=excluded.reason, enabled=1`,
			"suffix", suffix, "config.blockedSuffixes"); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func readSchema(name string) (string, error) {
	wd, err := os.Getwd()
	if err == nil {
		for {
			path := filepath.Join(wd, "schemas", name)
			data, err := os.ReadFile(path)
			if err == nil {
				return string(data), nil
			}
			parent := filepath.Dir(wd)
			if parent == wd {
				break
			}
			wd = parent
		}
	}
	sqlText, embedErr := schemadata.ReadSQL(name)
	if embedErr != nil {
		if err != nil {
			return "", fmt.Errorf("schema file not found: %s: cwd error: %w; embedded error: %v", name, err, embedErr)
		}
		return "", fmt.Errorf("schema file not found: %s: %w", name, embedErr)
	}
	return sqlText, nil
}

func recoverLeases(ctx context.Context, db *sql.DB, runID string) error {
	totalRecovered := int64(0)
	if result, err := db.ExecContext(ctx, `UPDATE cdx_discovery_pages SET state='failed_retryable', updated_at=CURRENT_TIMESTAMP WHERE run_id=? AND state='running' AND updated_at < datetime('now','-30 minutes')`, runID); err == nil {
		affected, _ := result.RowsAffected()
		totalRecovered += affected
	} else if !strings.Contains(strings.ToLower(err.Error()), "no such table") {
		return err
	}
	for _, table := range []string{"warc_tasks", "wat_tasks", "graph_tasks", "availability_tasks"} {
		stmt := fmt.Sprintf(`UPDATE %s SET state='pending', lease_owner=NULL, leased_until=NULL, updated_at=CURRENT_TIMESTAMP WHERE run_id=? AND state IN ('leased','running') AND (leased_until IS NULL OR leased_until < datetime('now','-30 minutes'))`, table)
		result, err := db.ExecContext(ctx, stmt, runID)
		if err != nil {
			return err
		}
		affected, _ := result.RowsAffected()
		totalRecovered += affected
	}
	if totalRecovered > 0 {
		_, err := db.ExecContext(ctx, `INSERT INTO events(run_id,event_type,severity,component,message,payload_json) VALUES(?,?,?,?,?,?)`,
			runID, "leases_recovered", "warn", "recovery", "expired task leases recovered", fmt.Sprintf(`{"count":%d}`, totalRecovered))
		return err
	}
	return nil
}

func recoveryState(ctx context.Context, db *sql.DB, runID string) string {
	var count int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=? AND event_type='leases_recovered'`, runID).Scan(&count)
	if count > 0 {
		return "recovered"
	}
	return "enabled"
}

func recalculateProgress(ctx context.Context, db *sql.DB, runID string, now time.Time) error {
	discoveryComplete := countRows(ctx, db, `SELECT COUNT(*) FROM cdx_records WHERE run_id=?`, runID)
	discoveryTotal := max(discoveryComplete, countRows(ctx, db, `SELECT COUNT(*) FROM run_source_domains rs CROSS JOIN run_crawl_selections rc WHERE rs.run_id=? AND rc.run_id=?`, runID, runID))
	discoveryFailed := countRows(ctx, db, `SELECT COUNT(*) FROM cdx_discovery_pages WHERE run_id=? AND state IN ('failed_retryable','failed_final')`, runID)
	if err := updateProgressStage(ctx, db, runID, "discovery", discoveryComplete, discoveryTotal, discoveryFailed, now); err != nil {
		return err
	}
	extractionComplete := countRows(ctx, db, `SELECT COUNT(*) FROM warc_tasks WHERE run_id=? AND state='succeeded' AND length > 0 AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=warc_tasks.run_id AND mode='warc' AND enabled=1)`, runID) +
		countRows(ctx, db, `SELECT COUNT(*) FROM wat_tasks WHERE run_id=? AND state='succeeded' AND wat_filename <> 'pending-wat-sidecar' AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=wat_tasks.run_id AND mode='wat' AND enabled=1)`, runID) +
		countRows(ctx, db, `SELECT COUNT(*) FROM graph_tasks WHERE run_id=? AND state='succeeded' AND file_path <> 'pending-graph-release' AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=graph_tasks.run_id AND mode='web_graph' AND enabled=1)`, runID)
	extractionTotal := countRows(ctx, db, `SELECT COUNT(*) FROM warc_tasks WHERE run_id=? AND length > 0 AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=warc_tasks.run_id AND mode='warc' AND enabled=1)`, runID) +
		countRows(ctx, db, `SELECT COUNT(*) FROM wat_tasks WHERE run_id=? AND wat_filename <> 'pending-wat-sidecar' AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=wat_tasks.run_id AND mode='wat' AND enabled=1)`, runID) +
		countRows(ctx, db, `SELECT COUNT(*) FROM graph_tasks WHERE run_id=? AND file_path <> 'pending-graph-release' AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=graph_tasks.run_id AND mode='web_graph' AND enabled=1)`, runID)
	extractionFailed := countRows(ctx, db, `SELECT COUNT(*) FROM warc_tasks WHERE run_id=? AND state IN ('failed_retryable','failed_final') AND length > 0 AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=warc_tasks.run_id AND mode='warc' AND enabled=1)`, runID) +
		countRows(ctx, db, `SELECT COUNT(*) FROM wat_tasks WHERE run_id=? AND state IN ('failed_retryable','failed_final') AND wat_filename <> 'pending-wat-sidecar' AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=wat_tasks.run_id AND mode='wat' AND enabled=1)`, runID) +
		countRows(ctx, db, `SELECT COUNT(*) FROM graph_tasks WHERE run_id=? AND state IN ('failed_retryable','failed_final') AND file_path <> 'pending-graph-release' AND EXISTS (SELECT 1 FROM extraction_modes WHERE run_id=graph_tasks.run_id AND mode='web_graph' AND enabled=1)`, runID)
	if err := updateProgressStage(ctx, db, runID, "extraction", extractionComplete, extractionTotal, extractionFailed, now); err != nil {
		return err
	}
	domainTotal := countRows(ctx, db, `SELECT COUNT(*) FROM candidate_domains WHERE run_id=?`, runID)
	domainComplete := countRows(ctx, db, `SELECT COUNT(*) FROM candidate_domains WHERE run_id=? AND availability_status IS NOT NULL`, runID)
	if err := updateProgressStage(ctx, db, runID, "domain_checks", domainComplete, domainTotal, 0, now); err != nil {
		return err
	}
	exportComplete := countRows(ctx, db, `SELECT COUNT(*) FROM artifact_files WHERE run_id=? AND artifact_type='export' AND status='sealed'`, runID)
	exportTotal := 1
	if exportComplete > 0 {
		exportTotal = exportComplete
	}
	return updateProgressStage(ctx, db, runID, "export", exportComplete, exportTotal, 0, now)
}

func updateProgressStage(ctx context.Context, db *sql.DB, runID, stage string, complete, total, failed int, now time.Time) error {
	var oldComplete int
	var oldRate float64
	var updatedAtRaw string
	_ = db.QueryRowContext(ctx, `SELECT completed_units, recent_rate, updated_at FROM progress_counters WHERE run_id=? AND stage=?`, runID, stage).Scan(&oldComplete, &oldRate, &updatedAtRaw)
	updatedAt, _ := time.Parse(time.RFC3339, updatedAtRaw)
	if updatedAt.IsZero() {
		updatedAt = now
	}
	elapsed := now.Sub(updatedAt).Seconds()
	rate := oldRate
	if elapsed >= 10 {
		delta := complete - oldComplete
		if delta < 0 {
			delta = 0
		}
		rate = progress.EMA(oldRate, float64(delta)/elapsed, 0.35)
	}
	remaining := total - complete
	if remaining < 0 {
		remaining = 0
	}
	etaSeconds := 0
	confidence := "low"
	if rate > 0 {
		etaSeconds = int(float64(remaining) / rate)
		confidence = "medium"
		if total > 0 {
			confidence = "high"
		}
	}
	_, err := db.ExecContext(ctx, `INSERT INTO progress_counters(run_id,stage,completed_units,total_known_units,failed_units,recent_rate,eta_seconds,confidence,updated_at)
VALUES(?,?,?,?,?,?,?,?,?)
ON CONFLICT(run_id,stage) DO UPDATE SET
  completed_units=excluded.completed_units,
  total_known_units=excluded.total_known_units,
  failed_units=excluded.failed_units,
  recent_rate=excluded.recent_rate,
  eta_seconds=excluded.eta_seconds,
  confidence=excluded.confidence,
  updated_at=excluded.updated_at`,
		runID, stage, complete, total, failed, rate, etaSeconds, confidence, now.Format(time.RFC3339))
	return err
}

func countRows(ctx context.Context, db *sql.DB, query string, args ...any) int {
	var count int
	_ = db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count
}

func validTaskTable(table string) bool {
	switch table {
	case "warc_tasks", "wat_tasks", "graph_tasks", "availability_tasks":
		return true
	default:
		return false
	}
}

func isTerminalStatus(status string) bool {
	switch status {
	case "completed", "cancelled", "failed":
		return true
	default:
		return false
	}
}

func recordTaskFailure(ctx context.Context, tx *sql.Tx, runID, table string, id int64, message string, retryable bool) error {
	if message == "" {
		message = "task failed"
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO errors(run_id,component,task_table,task_id,error_code,error_message,retryable) VALUES(?,?,?,?,?,?,?)`,
		runID, componentForTaskTable(table), table, id, "task_failed", message, boolInt(retryable)); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO events(run_id,event_type,severity,component,message,payload_json) VALUES(?,?,?,?,?,?)`,
		runID, "task_failed", "error", componentForTaskTable(table), message, fmt.Sprintf(`{"taskTable":%q,"taskId":%d,"retryable":%t}`, table, id, retryable))
	return err
}

func componentForTaskTable(table string) string {
	switch table {
	case "warc_tasks", "wat_tasks":
		return "extraction"
	case "graph_tasks":
		return "graph"
	case "availability_tasks":
		return "availability"
	default:
		return "storage"
	}
}

func upsertCandidate(ctx context.Context, tx *sql.Tx, runID, domain, mode string, cfg config.Config, graphEdges, outboundURLs, sourcePages int) (bool, error) {
	normalized, err := normalization.NormalizeDomain(domain, cfg.Availability.BlockedDomains, blockedSuffixes(cfg))
	if err != nil {
		if isIgnoredCandidateError(err) {
			return false, nil
		}
		return false, err
	}
	graphSeen, watSeen, warcSeen := modeBits(mode)
	tldPolicy := policyCategory(normalized.RegistrableDomain, cfg)
	_, err = tx.ExecContext(ctx, `INSERT INTO candidate_domains(run_id,domain,tld,first_seen_mode,seen_in_graph,seen_in_wat,seen_in_warc,source_page_count,graph_edge_count,outbound_url_count,tld_policy_category,score,last_seen_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
ON CONFLICT(run_id,domain) DO UPDATE SET
  seen_in_graph=MAX(candidate_domains.seen_in_graph, excluded.seen_in_graph),
  seen_in_wat=MAX(candidate_domains.seen_in_wat, excluded.seen_in_wat),
  seen_in_warc=MAX(candidate_domains.seen_in_warc, excluded.seen_in_warc),
  source_page_count=candidate_domains.source_page_count+excluded.source_page_count,
  graph_edge_count=candidate_domains.graph_edge_count+excluded.graph_edge_count,
  outbound_url_count=candidate_domains.outbound_url_count+excluded.outbound_url_count,
  tld_policy_category=excluded.tld_policy_category,
  score=candidate_domains.score+excluded.score,
  last_seen_at=CURRENT_TIMESTAMP`,
		runID, normalized.RegistrableDomain, normalized.TLD, mode, graphSeen, watSeen, warcSeen, sourcePages, graphEdges, outboundURLs, tldPolicy, evidenceScore(mode, graphEdges, outboundURLs))
	if err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO availability_tasks(run_id,domain,method,provider,max_attempts) VALUES(?,?,?,?,?)`, runID, normalized.RegistrableDomain, "classifier", "", taskMaxAttempts(cfg))
	return true, err
}

func recordSourcePage(ctx context.Context, tx *sql.Tx, runID, domain, sourceDomain, mode, crawlID, sourceURL string) error {
	if sourceURL == "" {
		return nil
	}
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO candidate_source_pages(run_id,domain,source_domain,mode,crawl_id,source_url) VALUES(?,?,?,?,?,?)`,
		runID, domain, sourceDomain, mode, crawlID, sourceURL)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE candidate_domains SET source_page_count=source_page_count+1 WHERE run_id=? AND domain=?`, runID, domain)
	return err
}

func isIgnoredCandidateError(err error) bool {
	message := err.Error()
	return strings.Contains(message, "blocked domain:") || strings.Contains(message, "blocked suffix:")
}

func blockedSuffixes(cfg config.Config) []string {
	out := make([]string, 0, len(cfg.Availability.BlockedTLDs)+len(cfg.Availability.BlockedSuffixes))
	out = append(out, cfg.Availability.BlockedTLDs...)
	out = append(out, cfg.Availability.BlockedSuffixes...)
	return out
}

func modeBits(mode string) (graphSeen, watSeen, warcSeen int) {
	switch mode {
	case "web_graph":
		return 1, 0, 0
	case "wat":
		return 0, 1, 0
	case "warc":
		return 0, 0, 1
	default:
		return 0, 0, 0
	}
}

func policyCategory(domain string, cfg config.Config) string {
	tld := domain
	if i := strings.LastIndexByte(domain, '.'); i >= 0 {
		tld = domain[i+1:]
	}
	for _, restricted := range cfg.Availability.RestrictedTLDs {
		if strings.EqualFold(tld, strings.TrimPrefix(restricted, ".")) {
			return "restricted"
		}
	}
	for _, blocked := range cfg.Availability.BlockedTLDs {
		if strings.EqualFold(tld, strings.TrimPrefix(blocked, ".")) {
			return "unsupported_unknown"
		}
	}
	switch tld {
	case "arpa":
		return "reserved_infrastructure"
	default:
		return "open_retail"
	}
}

func evidenceScore(mode string, graphEdges, outboundURLs int) int {
	score := outboundURLs + graphEdges
	switch mode {
	case "warc":
		score += 30
	case "wat":
		score += 20
	case "web_graph":
		score += 10
	}
	return score
}

func scoreCandidate(record AvailabilityRecord) int {
	score := 0
	switch record.Status {
	case availability.AvailableRegistrarConfirmed:
		score += 1000
	case availability.AvailableLikely:
		score += 700
	case availability.PendingDelete, availability.RedemptionPeriod:
		score += 500
	case availability.Premium:
		score += 100
	case availability.RegisteredActive, availability.RegisteredNoDNS:
		score -= 500
	case availability.RestrictedTLD, availability.Reserved, availability.UnsupportedTLD, availability.Ignored:
		score -= 1000
	}
	switch record.Confidence {
	case availability.ConfidenceHighest:
		score += 200
	case availability.ConfidenceHigh:
		score += 100
	case availability.ConfidenceMedium:
		score += 50
	}
	if record.Premium {
		score -= 200
	}
	return score
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func nullablePositive(value int) any {
	if value <= 0 {
		return nil
	}
	return value
}

func nullEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func priceSummary(premium bool, priceCents int, currency string) string {
	if !premium && priceCents <= 0 {
		return ""
	}
	label := "standard"
	if premium {
		label = "premium"
	}
	if priceCents <= 0 {
		return label
	}
	if currency == "" {
		currency = "USD"
	}
	return fmt.Sprintf("%s %s %.2f", label, currency, float64(priceCents)/100)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func parseInt(raw string) int {
	value, _ := strconv.Atoi(strings.TrimSpace(raw))
	return value
}

func sqliteTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}

func shortID() string {
	id := strings.ReplaceAll(uuid.NewString(), "-", "")
	if len(id) > 10 {
		return id[:10]
	}
	return id
}

func parseSQLiteTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return parsed, nil
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05", raw); err == nil {
		return parsed, nil
	}
	return time.Parse(time.RFC3339Nano, raw)
}

func parseGraphSourceHash(raw string) ([]string, string) {
	sourcesPart, vertexPath, _ := strings.Cut(raw, "|vertex=")
	var sources []string
	for _, source := range strings.Split(sourcesPart, ",") {
		source = strings.TrimSpace(source)
		if source != "" {
			sources = append(sources, source)
		}
	}
	return sources, vertexPath
}

func slug(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else if r == '.' || r == '-' {
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

func isMonth(s string) bool {
	return len(s) == 7 && s[4] == '-'
}

func sourceSetHash(sources []normalization.Result) string {
	names := make([]string, 0, len(sources))
	for _, source := range sources {
		names = append(names, source.RegistrableDomain)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

func taskMaxAttempts(cfg config.Config) int {
	if cfg.Runtime.MaxRetryAttempts < 0 {
		return 1
	}
	return cfg.Runtime.MaxRetryAttempts + 1
}

func runTaskMaxAttempts(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var raw string
	if err := tx.QueryRowContext(ctx, `SELECT config_json FROM runs WHERE id=?`, runID).Scan(&raw); err != nil {
		return 0, err
	}
	var cfg config.Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return 0, err
	}
	return taskMaxAttempts(cfg), nil
}

func initialTotal(stage string, req RunRequest) int {
	switch stage {
	case "discovery":
		return len(req.Sources) * len(req.Crawls)
	case "extraction":
		return len(req.Sources) * len(req.Crawls) * len(req.Modes)
	case "domain_checks":
		return 1
	case "export":
		return 1
	default:
		return 0
	}
}

func titleStage(stage string) string {
	switch stage {
	case "discovery":
		return string(progress.Discovery)
	case "extraction":
		return string(progress.Extraction)
	case "domain_checks":
		return string(progress.DomainChecks)
	case "export":
		return string(progress.Export)
	default:
		return stage
	}
}

func aggregateETA(stages []progress.Stage) time.Duration {
	var total time.Duration
	for _, stage := range stages {
		total += stage.ETA
	}
	return total
}

func cacheHits(ctx context.Context, db *sql.DB, runID string) int {
	var hits int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_domains WHERE run_id=? AND cache_hit=1`, runID).Scan(&hits)
	return hits
}

func modesFromBits(graph, wat, warc int) string {
	var modes []string
	if graph == 1 {
		modes = append(modes, "web_graph")
	}
	if wat == 1 {
		modes = append(modes, "wat")
	}
	if warc == 1 {
		modes = append(modes, "warc")
	}
	if len(modes) == 0 {
		return "none"
	}
	return strings.Join(modes, ",")
}
