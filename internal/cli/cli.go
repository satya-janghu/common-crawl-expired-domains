package cli

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"expired-domain-finder/internal/artifact"
	"expired-domain-finder/internal/availability"
	"expired-domain-finder/internal/commoncrawl"
	"expired-domain-finder/internal/config"
	"expired-domain-finder/internal/graph"
	"expired-domain-finder/internal/normalization"
	"expired-domain-finder/internal/progress"
	"expired-domain-finder/internal/storage"
	"expired-domain-finder/internal/tui"
	"expired-domain-finder/internal/workflow"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

const usage = `Expired Domain Finder

Usage:
  edf init --data-dir <path>
  edf config show
  edf find --source <domain> --crawl <id-or-month> --mode <web_graph|wat|warc> [--export-format csv|json|parquet] [--export-output <path>]
  edf run start --source <domain> --crawl <id-or-month> --mode <web_graph|wat|warc> [--no-tui] [--create-only] [--cdx-limit n] [--max-runtime 5m]
  edf run status <run-id>
  edf run watch <run-id>
  edf run pause <run-id>
  edf run resume <run-id>
  edf run stop <run-id>
  edf results <run-id>
  edf export <run-id> --format <csv|json|parquet> --output <path>
`

var errWorkBudgetReached = errors.New("work budget reached")

func Execute(args []string, stdout, stderr io.Writer) int {
	return ExecuteWithInput(args, stdout, stderr, nil)
}

func ExecuteWithInput(args []string, stdout, stderr io.Writer, stdin io.Reader) int {
	opts, args, err := parseGlobal(args)
	if err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return 2
	}
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprint(stdout, usage)
		return 0
	}

	switch args[0] {
	case "init":
		return runInit(args[1:], stdout, stderr, opts)
	case "config":
		return runConfig(args[1:], stdout, stderr, opts)
	case "find":
		return runFind(args[1:], stdout, stderr, opts, stdin)
	case "run":
		return runRun(args[1:], stdout, stderr, opts, stdin)
	case "results":
		return runResults(args[1:], stdout, stderr, opts)
	case "export":
		return runExport(args[1:], stdout, stderr, opts)
	default:
		fmt.Fprintf(stderr, "unknown command: %s\n\n%s", args[0], usage)
		return 2
	}
}

type globalOptions struct {
	configPath string
	dataDir    string
	output     string
}

type runLimits struct {
	CDXRecords         int
	WARCTasks          int
	WATTasks           int
	GraphTasks         int
	AvailabilityChecks int
	MaxRuntime         time.Duration
}

type runCommandOptions struct {
	Sources             repeatedFlag
	Crawls              repeatedFlag
	Modes               repeatedFlag
	NoTUI               bool
	CreateOnly          bool
	Quiet               bool
	ProgressInterval    time.Duration
	Limits              runLimits
	MaxWARCRecordBytes  int64
	MaxWARCDecodedBytes int64
	MaxWATObjectBytes   int64
	MaxGraphObjectBytes int64
	GraphVertices       string
	GraphEdges          string
	ExportFormat        string
	ExportOutput        string
	NoExport            bool
}

func parseGlobal(args []string) (globalOptions, []string, error) {
	opts := globalOptions{output: "text"}
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--config":
			i++
			if i >= len(args) {
				return opts, nil, fmt.Errorf("--config requires a value")
			}
			opts.configPath = args[i]
		case "--data-dir":
			i++
			if i >= len(args) {
				return opts, nil, fmt.Errorf("--data-dir requires a value")
			}
			opts.dataDir = args[i]
		case "--output":
			i++
			if i >= len(args) {
				return opts, nil, fmt.Errorf("--output requires a value")
			}
			if args[i] != "text" && args[i] != "json" {
				return opts, nil, fmt.Errorf("--output must be text or json")
			}
			opts.output = args[i]
		default:
			out = append(out, args[i:]...)
			return opts, out, nil
		}
	}
	return opts, out, nil
}

func loadConfig(opts globalOptions) (config.Config, error) {
	path := opts.configPath
	if path == "" && opts.dataDir != "" {
		path = config.ConfigPath(opts.dataDir)
	}
	cfg, err := config.Load(path)
	if err != nil {
		return config.Config{}, err
	}
	if opts.dataDir != "" {
		cfg.DataDirectory = opts.dataDir
	}
	return cfg, nil
}

func runInit(args []string, stdout, stderr io.Writer, opts globalOptions) int {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dataDir := fs.String("data-dir", opts.dataDir, "base data directory")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dataDir == "" {
		*dataDir = config.DefaultDataDir()
	}

	cfg := config.Default()
	cfg.DataDirectory = *dataDir
	if err := storage.New(cfg.DataDirectory).Init(context.Background(), cfg); err != nil {
		fmt.Fprintf(stderr, "init storage: %v\n", err)
		return 1
	}
	if opts.output == "json" {
		return writeJSON(stdout, map[string]string{"dataDirectory": cfg.DataDirectory, "config": config.ConfigPath(cfg.DataDirectory)})
	}
	fmt.Fprintf(stdout, "initialized data directory: %s\n", cfg.DataDirectory)
	fmt.Fprintf(stdout, "config=%s\n", config.ConfigPath(cfg.DataDirectory))
	return 0
}

func runConfig(args []string, stdout, stderr io.Writer, opts globalOptions) int {
	if len(args) != 1 || args[0] != "show" {
		fmt.Fprint(stderr, "usage: edf config show\n")
		return 2
	}

	cfg, err := loadConfig(opts)
	if err != nil {
		fmt.Fprintf(stderr, "load config: %v\n", err)
		return 1
	}
	return writeJSON(stdout, cfg.Redacted())
}

func runRun(args []string, stdout, stderr io.Writer, opts globalOptions, stdin io.Reader) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, "usage: edf run <start|status|watch|pause|resume|stop>\n")
		return 2
	}

	switch args[0] {
	case "start":
		return runStart(args[1:], stdout, stderr, opts)
	case "status":
		if len(args) == 1 {
			return runList(stdout, stderr, opts)
		}
		return runIDCommand("status", args[1:], stdout, stderr, opts)
	case "watch":
		return runWatch(args[1:], stdout, stderr, opts, stdin)
	case "pause", "resume", "stop":
		if args[0] == "resume" {
			return runResume(args[1:], stdout, stderr, opts)
		}
		return runIDCommand(args[0], args[1:], stdout, stderr, opts)
	default:
		fmt.Fprintf(stderr, "unknown run command: %s\n", args[0])
		return 2
	}
}

func runWatch(args []string, stdout, stderr io.Writer, opts globalOptions, stdin io.Reader) int {
	runID := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		runID = args[0]
		args = args[1:]
	}
	fs := flag.NewFlagSet("run watch", flag.ContinueOnError)
	fs.SetOutput(stderr)
	ticks := fs.Int("ticks", 0, "number of refreshes before exiting; 0 watches until terminal run state")
	interval := fs.Duration("interval", 10*time.Second, "watch refresh interval")
	control := fs.String("control", "", "apply a TUI control once: p|pause, p|resume, s|stop, q|quit")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if runID == "" && fs.NArg() == 1 {
		runID = fs.Arg(0)
	}
	if runID == "" || fs.NArg() > 1 {
		fmt.Fprint(stderr, "usage: edf run watch <run-id> [--ticks n] [--interval 10s]\n")
		return 2
	}
	if *interval <= 0 {
		fmt.Fprint(stderr, "watch interval must be positive\n")
		return 2
	}
	cfg, err := loadConfig(opts)
	if err != nil {
		fmt.Fprintf(stderr, "load config: %v\n", err)
		return 1
	}
	if *interval != 10*time.Second && *ticks == 0 {
		// Production progress is recalculated every 10 seconds; shorter intervals are useful only for bounded smoke checks.
		*interval = 10 * time.Second
	}
	store := storage.New(cfg.DataDirectory)
	if *control != "" {
		snapshot, err := store.Snapshot(context.Background(), runID)
		if err != nil {
			fmt.Fprintf(stderr, "load run: %v\n", err)
			return 1
		}
		action := tui.ControlForKey(*control, snapshot.Status)
		switch action {
		case tui.ControlPause:
			if err := store.SetRunStatus(context.Background(), runID, "pause_requested"); err != nil {
				fmt.Fprintf(stderr, "pause run: %v\n", err)
				return 1
			}
			fmt.Fprintf(stdout, "pause requested for run %s\n", runID)
			return 0
		case tui.ControlResume:
			if err := store.SetRunStatus(context.Background(), runID, "resume_requested"); err != nil {
				fmt.Fprintf(stderr, "resume run: %v\n", err)
				return 1
			}
			fmt.Fprintf(stdout, "resume requested for run %s\n", runID)
			return 0
		case tui.ControlStop:
			if err := store.SetRunStatus(context.Background(), runID, "cancelling"); err != nil {
				fmt.Fprintf(stderr, "stop run: %v\n", err)
				return 1
			}
			fmt.Fprintf(stdout, "stop requested for run %s\n", runID)
			return 0
		case tui.ControlQuit:
			return 0
		default:
			fmt.Fprintf(stderr, "unsupported watch control: %s\n", *control)
			return 2
		}
	}
	controlCh := watchControlChannel(stdin)
	count := 0
	for {
		select {
		case key, ok := <-controlCh:
			if ok {
				if done, code := applyWatchControl(context.Background(), store, runID, key, stdout, stderr); done {
					return code
				}
			}
		default:
		}
		snapshot, err := store.Snapshot(context.Background(), runID)
		if err != nil {
			fmt.Fprintf(stderr, "load run: %v\n", err)
			return 1
		}
		events, _ := store.RecentEvents(context.Background(), runID, 20)
		candidates, _ := store.Results(context.Background(), runID)
		model := tui.NewModelDetailed(snapshot, renderEvents(events), renderCandidatePreview(candidates, 10))
		if count > 0 {
			fmt.Fprint(stdout, "\n")
		}
		fmt.Fprint(stdout, model.RenderPlain())
		count++
		if *ticks > 0 && count >= *ticks {
			return 0
		}
		if isTerminalRunStatus(snapshot.Status) {
			return 0
		}
		timer := time.NewTimer(*interval)
		select {
		case key, ok := <-controlCh:
			timer.Stop()
			if ok {
				if done, code := applyWatchControl(context.Background(), store, runID, key, stdout, stderr); done {
					return code
				}
			}
		case <-timer.C:
		}
	}
}

func runFind(args []string, stdout, stderr io.Writer, opts globalOptions, stdin io.Reader) int {
	runOpts, err := parseFindOptions(args, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return 2
	}
	cfg, err := loadConfig(opts)
	if err != nil {
		fmt.Fprintf(stderr, "load config: %v\n", err)
		return 1
	}
	applyCommonCrawlOverrides(&cfg, runOpts.MaxWARCRecordBytes, runOpts.MaxWARCDecodedBytes, runOpts.MaxWATObjectBytes, runOpts.MaxGraphObjectBytes)
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "config overrides: %v\n", err)
		return 2
	}
	normalized, err := normalization.Deduplicate(runOpts.Sources, cfg.Availability.BlockedDomains, blockedSuffixes(cfg))
	if err != nil {
		fmt.Fprintf(stderr, "normalize source: %v\n", err)
		return 2
	}
	store := storage.New(cfg.DataDirectory)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: normalized, Crawls: runOpts.Crawls, Modes: runOpts.Modes, Config: cfg, InitialStatus: "running"})
	if err != nil {
		fmt.Fprintf(stderr, "create run: %v\n", err)
		return 1
	}
	if opts.output != "json" {
		printRunStartSummary(stdout, run, normalized, runOpts.Crawls, runOpts.Modes, false, false)
	}
	controlCh := watchControlChannel(stdin)
	code := executeForegroundRun(run.ID, sourceDomains(normalized), runOpts.Crawls, runOpts.Modes, cfg, store, runOpts, stdout, stderr, opts.output == "json", false, controlCh)
	if code != 0 {
		return code
	}
	snapshot, err := store.Snapshot(context.Background(), run.ID)
	if err != nil {
		fmt.Fprintf(stderr, "load run: %v\n", err)
		return 1
	}
	if snapshot.Status != "completed" && snapshot.Status != "recoverable" {
		if opts.output == "json" {
			return writeJSON(stdout, snapshot)
		}
		fmt.Fprint(stdout, snapshot.RenderLines())
		return 0
	}
	if snapshot.Status == "recoverable" {
		if opts.output != "json" {
			fmt.Fprint(stdout, "warning=partial_results reason=\"retryable Common Crawl discovery failures remain; resume the run to fill gaps\"\n")
			fmt.Fprintf(stdout, "resume_command=%q\n", resumeCommand(cfg.DataDirectory, run.ID))
		}
	}
	rows, err := store.Results(context.Background(), run.ID)
	if err != nil {
		fmt.Fprintf(stderr, "load results: %v\n", err)
		return 1
	}
	var exportPath string
	if !runOpts.NoExport && (snapshot.Status == "completed" || len(rows) > 0) {
		exportPath = runOpts.ExportOutput
		if exportPath == "" {
			exportPath = filepath.Join(run.Dir, "artifacts", "exports", "results."+runOpts.ExportFormat)
		}
		exportRows, manifest, err := writeAndRecordExport(context.Background(), store, run.ID, exportPath, runOpts.ExportFormat)
		if err != nil {
			fmt.Fprintf(stderr, "export results: %v\n", err)
			return 1
		}
		if opts.output != "json" {
			fmt.Fprintf(stdout, "export=%s rows=%d format=%s\n", manifest.Path, len(exportRows), runOpts.ExportFormat)
		}
	} else if !runOpts.NoExport && snapshot.Status == "recoverable" && opts.output != "json" {
		fmt.Fprint(stdout, "export=skipped rows=0 reason=\"recoverable run has no candidate results yet\"\n")
	}
	if opts.output == "json" {
		return writeJSON(stdout, map[string]any{"run": snapshot, "export": exportPath, "results": rows})
	}
	fmt.Fprint(stdout, renderResultsTable(rows))
	return 0
}

func runStart(args []string, stdout, stderr io.Writer, opts globalOptions) int {
	runOpts, err := parseRunStartOptions(args, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return 2
	}
	cfg, err := loadConfig(opts)
	if err != nil {
		fmt.Fprintf(stderr, "load config: %v\n", err)
		return 1
	}
	applyCommonCrawlOverrides(&cfg, runOpts.MaxWARCRecordBytes, runOpts.MaxWARCDecodedBytes, runOpts.MaxWATObjectBytes, runOpts.MaxGraphObjectBytes)
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "config overrides: %v\n", err)
		return 2
	}
	normalized, err := normalization.Deduplicate(runOpts.Sources, cfg.Availability.BlockedDomains, blockedSuffixes(cfg))
	if err != nil {
		fmt.Fprintf(stderr, "normalize source: %v\n", err)
		return 2
	}

	initialStatus := "running"
	if runOpts.CreateOnly {
		initialStatus = "created"
	}
	store := storage.New(cfg.DataDirectory)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: normalized, Crawls: runOpts.Crawls, Modes: runOpts.Modes, Config: cfg, InitialStatus: initialStatus})
	if err != nil {
		fmt.Fprintf(stderr, "create run: %v\n", err)
		return 1
	}
	if opts.output == "json" && runOpts.CreateOnly {
		return writeJSON(stdout, run)
	}
	if opts.output != "json" {
		printRunStartSummary(stdout, run, normalized, runOpts.Crawls, runOpts.Modes, runOpts.NoTUI, runOpts.CreateOnly)
		snapshot, _ := store.Snapshot(context.Background(), run.ID)
		fmt.Fprint(stdout, snapshot.RenderLines())
	}
	if !runOpts.CreateOnly {
		return executeForegroundRun(run.ID, sourceDomains(normalized), runOpts.Crawls, runOpts.Modes, cfg, store, runOpts, stdout, stderr, opts.output == "json", true, nil)
	}
	return 0
}

func parseRunStartOptions(args []string, stderr io.Writer) (runCommandOptions, error) {
	fs := flag.NewFlagSet("run start", flag.ContinueOnError)
	fs.SetOutput(stderr)
	runOpts := runCommandOptions{ProgressInterval: 10 * time.Second}
	fs.BoolVar(&runOpts.NoTUI, "no-tui", false, "disable TUI for interactive terminal sessions")
	fs.BoolVar(&runOpts.CreateOnly, "create-only", false, "create run state without executing workers")
	_ = fs.Bool("execute", true, "deprecated: run start executes in the foreground by default")
	addRunExecutionFlags(fs, &runOpts)
	fs.Var(&runOpts.Sources, "source", "source domain or URL; repeat up to 10 times")
	fs.Var(&runOpts.Crawls, "crawl", "crawl ID or month; repeat for multiple selections")
	fs.Var(&runOpts.Modes, "mode", "extraction mode: web_graph, wat, or warc")
	if err := fs.Parse(args); err != nil {
		return runCommandOptions{}, err
	}
	return validateRunOptions(runOpts, "run start")
}

func parseFindOptions(args []string, stderr io.Writer) (runCommandOptions, error) {
	fs := flag.NewFlagSet("find", flag.ContinueOnError)
	fs.SetOutput(stderr)
	runOpts := runCommandOptions{ProgressInterval: 10 * time.Second, ExportFormat: "csv"}
	fs.BoolVar(&runOpts.Quiet, "quiet", false, "suppress periodic progress output")
	fs.DurationVar(&runOpts.ProgressInterval, "progress-interval", 10*time.Second, "progress refresh interval; must be 10s")
	fs.StringVar(&runOpts.ExportFormat, "export-format", "csv", "export format: csv, json, parquet")
	fs.StringVar(&runOpts.ExportOutput, "export-output", "", "export output path")
	fs.BoolVar(&runOpts.NoExport, "no-export", false, "skip the default result export")
	fs.BoolVar(&runOpts.NoTUI, "no-tui", false, "deprecated for find; inline progress is used")
	addRunExecutionFlags(fs, &runOpts)
	fs.Var(&runOpts.Sources, "source", "source domain or URL; repeat up to 10 times")
	fs.Var(&runOpts.Crawls, "crawl", "crawl ID or month; repeat for multiple selections")
	fs.Var(&runOpts.Modes, "mode", "extraction mode: web_graph, wat, or warc")
	if err := fs.Parse(args); err != nil {
		return runCommandOptions{}, err
	}
	if runOpts.ProgressInterval != 10*time.Second {
		return runCommandOptions{}, fmt.Errorf("--progress-interval must be 10s")
	}
	if !validExportFormat(runOpts.ExportFormat) {
		return runCommandOptions{}, fmt.Errorf("unsupported export format: %s", runOpts.ExportFormat)
	}
	if runOpts.NoExport && runOpts.ExportOutput != "" {
		return runCommandOptions{}, fmt.Errorf("--export-output cannot be used with --no-export")
	}
	return validateRunOptions(runOpts, "find")
}

func addRunExecutionFlags(fs *flag.FlagSet, runOpts *runCommandOptions) {
	fs.IntVar(&runOpts.Limits.CDXRecords, "cdx-limit", 1000, "maximum CDX records per source/crawl")
	fs.IntVar(&runOpts.Limits.WARCTasks, "warc-task-limit", 0, "maximum WARC tasks to process in this command; 0 means all")
	fs.IntVar(&runOpts.Limits.WATTasks, "wat-task-limit", 0, "maximum WAT tasks to process in this command; 0 means all")
	fs.IntVar(&runOpts.Limits.GraphTasks, "graph-task-limit", 0, "maximum graph tasks to process in this command; 0 means all")
	fs.IntVar(&runOpts.Limits.AvailabilityChecks, "availability-limit", 0, "maximum availability tasks to process in this command; 0 means all")
	fs.DurationVar(&runOpts.Limits.MaxRuntime, "max-runtime", 0, "maximum foreground runtime for this command; 0 means no limit")
	fs.Int64Var(&runOpts.MaxWARCRecordBytes, "max-warc-record-bytes", 0, "override commonCrawl.maxWarcRecordBytes for this command")
	fs.Int64Var(&runOpts.MaxWARCDecodedBytes, "max-warc-decoded-bytes", 0, "override commonCrawl.maxWarcDecodedBytes for this command")
	fs.Int64Var(&runOpts.MaxWATObjectBytes, "max-wat-object-bytes", 0, "override commonCrawl.maxWatObjectBytes for this command")
	fs.Int64Var(&runOpts.MaxGraphObjectBytes, "max-graph-object-bytes", 0, "override commonCrawl.maxGraphObjectBytes for this command")
	fs.StringVar(&runOpts.GraphVertices, "graph-vertices", "", "graph vertex object path for web_graph execution")
	fs.StringVar(&runOpts.GraphEdges, "graph-edges", "", "graph edge object path for web_graph execution")
}

func validateRunOptions(runOpts runCommandOptions, command string) (runCommandOptions, error) {
	if len(runOpts.Sources) == 0 || len(runOpts.Sources) > 10 {
		return runCommandOptions{}, fmt.Errorf("%s requires 1-10 --source values", command)
	}
	if len(runOpts.Crawls) == 0 {
		return runCommandOptions{}, fmt.Errorf("%s requires at least one --crawl value", command)
	}
	if err := validateModes(runOpts.Modes); err != nil {
		if strings.Contains(err.Error(), "run start") {
			return runCommandOptions{}, fmt.Errorf("%s requires at least one --mode value", command)
		}
		return runCommandOptions{}, err
	}
	if err := validateRunLimitFlags(runOpts.Limits.CDXRecords, runOpts.Limits.WARCTasks, runOpts.Limits.WATTasks, runOpts.Limits.GraphTasks, runOpts.Limits.AvailabilityChecks, runOpts.Limits.MaxRuntime); err != nil {
		return runCommandOptions{}, err
	}
	return runOpts, nil
}

func printRunStartSummary(stdout io.Writer, run storage.Run, normalized []normalization.Result, crawls, modes []string, noTUI, createOnly bool) {
	fmt.Fprintf(stdout, "run_id=%s\n", run.ID)
	fmt.Fprintf(stdout, "run_dir=%s\n", run.Dir)
	fmt.Fprintf(stdout, "sources=%s\n", strings.Join(sourceDomains(normalized), ","))
	fmt.Fprintf(stdout, "crawls=%s\n", strings.Join(crawls, ","))
	fmt.Fprintf(stdout, "modes=%s\n", strings.Join(modes, ","))
	if noTUI {
		fmt.Fprint(stdout, "tui=disabled\n")
	}
	if createOnly {
		fmt.Fprint(stdout, "execution=create-only\n")
	} else {
		fmt.Fprint(stdout, "execution=foreground\n")
	}
}

func executeForegroundRun(runID string, sources, crawls, modes []string, cfg config.Config, store storage.Store, runOpts runCommandOptions, stdout, stderr io.Writer, jsonOutput, printFinalSnapshot bool, controlCh <-chan string) int {
	runner := workflow.Runner{
		Store:       store,
		Config:      cfg,
		CommonCrawl: commonCrawlClient(cfg),
		RDAP:        availability.RDAPClient{UserAgent: cfg.CommonCrawl.UserAgent},
		Registrar:   registrarFromEnv(cfg),
		WorkerID:    "cli",
	}
	stopProgressFn := func() {}
	if !jsonOutput && !runOpts.Quiet {
		stopProgressFn = startProgressPrinter(store, runID, stdout, runOpts.ProgressInterval)
	}
	var stopProgressOnce sync.Once
	stopProgress := func() {
		stopProgressOnce.Do(stopProgressFn)
	}
	stopControls := startRunControlApplier(store, runID, controlCh, stdout, stderr, stopProgress)
	ctx, cancel := runContext(runOpts.Limits.MaxRuntime)
	err := executeRunPlan(ctx, runner, runID, sources, crawls, modes, runOpts.Limits, runOpts.GraphVertices, runOpts.GraphEdges)
	cancel()
	stopControls()
	stopProgress()
	if err != nil {
		if errors.Is(err, errWorkBudgetReached) {
			_ = store.SetRunStatus(context.Background(), runID, "paused")
			if !jsonOutput {
				fmt.Fprintf(stdout, "execution=paused reason=%q\n", err.Error())
			}
			finalSnapshot, _ := store.Snapshot(context.Background(), runID)
			if jsonOutput && printFinalSnapshot {
				return writeJSON(stdout, finalSnapshot)
			}
			if !jsonOutput && printFinalSnapshot {
				fmt.Fprint(stdout, finalSnapshot.RenderLines())
			}
			return 0
		}
		if isControlledStop(err) {
			fmt.Fprintf(stderr, "execute stopped: %v\n", err)
			return 1
		}
		_ = store.SetRunStatus(context.Background(), runID, "failed")
		fmt.Fprintf(stderr, "execute run: %v\n", err)
		return 1
	}
	if code, handled := finishIncompleteRunIfNeeded(store, runID, stdout, stderr, jsonOutput, printFinalSnapshot); handled {
		return code
	}
	if err := store.SetRunStatus(context.Background(), runID, "completed"); err != nil {
		fmt.Fprintf(stderr, "complete run: %v\n", err)
		return 1
	}
	finalSnapshot, _ := store.Snapshot(context.Background(), runID)
	if jsonOutput && printFinalSnapshot {
		return writeJSON(stdout, finalSnapshot)
	}
	if !jsonOutput && printFinalSnapshot {
		fmt.Fprint(stdout, finalSnapshot.RenderLines())
	}
	return 0
}

func finishIncompleteRunIfNeeded(store storage.Store, runID string, stdout, stderr io.Writer, jsonOutput, printFinalSnapshot bool) (int, bool) {
	blockers, err := store.CompletionBlockers(context.Background(), runID)
	if err != nil {
		fmt.Fprintf(stderr, "check completion: %v\n", err)
		return 1, true
	}
	if len(blockers) == 0 {
		return 0, false
	}
	status, err := store.RunStatus(context.Background(), runID)
	if err != nil {
		fmt.Fprintf(stderr, "load run status: %v\n", err)
		return 1, true
	}
	finalStatus := "paused"
	reason := "incomplete work remains"
	if status == "cancelling" || status == "cancelled" || hasCancelledBlocker(blockers) {
		finalStatus = "cancelled"
		reason = "run cancelled before all work completed"
	} else if onlyCDXRetryableBlockers(blockers) {
		finalStatus = "recoverable"
		reason = "partial Common Crawl discovery"
	}
	if err := store.SetRunStatus(context.Background(), runID, finalStatus); err != nil {
		fmt.Fprintf(stderr, "finalize incomplete run: %v\n", err)
		return 1, true
	}
	if !jsonOutput {
		fmt.Fprintf(stdout, "execution=%s reason=%q blockers=%s\n", finalStatus, reason, formatBlockers(blockers))
	}
	snapshot, _ := store.Snapshot(context.Background(), runID)
	if jsonOutput && printFinalSnapshot {
		return writeJSON(stdout, snapshot), true
	}
	if !jsonOutput && printFinalSnapshot {
		fmt.Fprint(stdout, snapshot.RenderLines())
	}
	return 0, true
}

func hasCancelledBlocker(blockers map[string]int) bool {
	for name, count := range blockers {
		if count > 0 && strings.Contains(name, "cancelled") {
			return true
		}
	}
	return false
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

func resumeCommand(dataDir, runID string) string {
	return fmt.Sprintf("go run ./cmd/edf --data-dir %s run resume %s --cdx-limit 0 --warc-task-limit 0 --wat-task-limit 0 --availability-limit 0 --max-runtime 0", dataDir, runID)
}

func formatBlockers(blockers map[string]int) string {
	if len(blockers) == 0 {
		return ""
	}
	keys := make([]string, 0, len(blockers))
	for key := range blockers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", key, blockers[key]))
	}
	return strings.Join(parts, ",")
}

func startRunControlApplier(store storage.Store, runID string, controlCh <-chan string, stdout, stderr io.Writer, stopMonitoring func()) func() {
	if controlCh == nil {
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case key, ok := <-controlCh:
				if !ok {
					return
				}
				switch strings.ToLower(strings.TrimSpace(key)) {
				case "q", "quit":
					stopMonitoring()
					return
				}
				finished, _ := applyWatchControl(context.Background(), store, runID, key, stdout, stderr)
				if finished {
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func runIDCommand(name string, args []string, stdout, stderr io.Writer, opts globalOptions) int {
	if len(args) != 1 {
		fmt.Fprintf(stderr, "usage: edf run %s <run-id>\n", name)
		return 2
	}
	cfg, err := loadConfig(opts)
	if err != nil {
		fmt.Fprintf(stderr, "load config: %v\n", err)
		return 1
	}
	store := storage.New(cfg.DataDirectory)
	if name == "status" {
		snapshot, err := store.Snapshot(context.Background(), args[0])
		if err != nil {
			fmt.Fprintf(stderr, "load run: %v\n", err)
			return 1
		}
		if opts.output == "json" {
			return writeJSON(stdout, snapshot)
		}
		fmt.Fprint(stdout, snapshot.RenderLines())
		return 0
	}
	status := map[string]string{"pause": "pause_requested", "resume": "resume_requested", "stop": "cancelling"}[name]
	if err := store.SetRunStatus(context.Background(), args[0], status); err != nil {
		fmt.Fprintf(stderr, "%s run: %v\n", name, err)
		return 1
	}
	fmt.Fprintf(stdout, "%s requested for run %s\n", name, args[0])
	return 0
}

func runList(stdout, stderr io.Writer, opts globalOptions) int {
	cfg, err := loadConfig(opts)
	if err != nil {
		fmt.Fprintf(stderr, "load config: %v\n", err)
		return 1
	}
	runs, err := storage.New(cfg.DataDirectory).ListRuns(context.Background())
	if err != nil {
		fmt.Fprintf(stderr, "list runs: %v\n", err)
		return 1
	}
	if opts.output == "json" {
		return writeJSON(stdout, runs)
	}
	if len(runs) == 0 {
		fmt.Fprint(stdout, "no runs found\n")
		return 0
	}
	fmt.Fprint(stdout, "Run ID\tStatus\tCreated\tDirectory\n")
	for _, run := range runs {
		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\n", run.ID, run.Status, run.CreatedAt.Format(time.RFC3339), run.Dir)
	}
	return 0
}

func runResume(args []string, stdout, stderr io.Writer, opts globalOptions) int {
	runID := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		runID = args[0]
		args = args[1:]
	}
	fs := flag.NewFlagSet("run resume", flag.ContinueOnError)
	fs.SetOutput(stderr)
	_ = fs.Bool("execute", true, "deprecated: run resume executes in the foreground by default")
	cdxLimit := fs.Int("cdx-limit", 1000, "maximum CDX records or tasks for resumed execution")
	warcTaskLimit := fs.Int("warc-task-limit", 0, "maximum WARC tasks to process in this command; 0 means all")
	watTaskLimit := fs.Int("wat-task-limit", 0, "maximum WAT tasks to process in this command; 0 means all")
	graphTaskLimit := fs.Int("graph-task-limit", 0, "maximum graph tasks to process in this command; 0 means all")
	availabilityLimit := fs.Int("availability-limit", 0, "maximum availability tasks to process in this command; 0 means all")
	maxRuntime := fs.Duration("max-runtime", 0, "maximum foreground runtime for this command; 0 means no limit")
	maxWARCRecordBytes := fs.Int64("max-warc-record-bytes", 0, "override commonCrawl.maxWarcRecordBytes for this command")
	maxWARCDecodedBytes := fs.Int64("max-warc-decoded-bytes", 0, "override commonCrawl.maxWarcDecodedBytes for this command")
	maxWATObjectBytes := fs.Int64("max-wat-object-bytes", 0, "override commonCrawl.maxWatObjectBytes for this command")
	maxGraphObjectBytes := fs.Int64("max-graph-object-bytes", 0, "override commonCrawl.maxGraphObjectBytes for this command")
	graphVertices := fs.String("graph-vertices", "", "graph vertex object path for web_graph execution")
	graphEdges := fs.String("graph-edges", "", "graph edge object path for web_graph execution")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if runID == "" && fs.NArg() == 1 {
		runID = fs.Arg(0)
	}
	if runID == "" || fs.NArg() > 1 {
		fmt.Fprint(stderr, "usage: edf run resume <run-id>\n")
		return 2
	}
	if err := validateRunLimitFlags(*cdxLimit, *warcTaskLimit, *watTaskLimit, *graphTaskLimit, *availabilityLimit, *maxRuntime); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return 2
	}
	cfg, err := loadConfig(opts)
	if err != nil {
		fmt.Fprintf(stderr, "load config: %v\n", err)
		return 1
	}
	store := storage.New(cfg.DataDirectory)
	if err := store.SetRunStatus(context.Background(), runID, "resume_requested"); err != nil {
		fmt.Fprintf(stderr, "resume run: %v\n", err)
		return 1
	}
	plan, err := store.RunPlan(context.Background(), runID)
	if err != nil {
		fmt.Fprintf(stderr, "load run plan: %v\n", err)
		return 1
	}
	if opts.dataDir != "" {
		plan.Config.DataDirectory = opts.dataDir
	}
	applyCommonCrawlOverrides(&plan.Config, *maxWARCRecordBytes, *maxWARCDecodedBytes, *maxWATObjectBytes, *maxGraphObjectBytes)
	if err := plan.Config.Validate(); err != nil {
		fmt.Fprintf(stderr, "config overrides: %v\n", err)
		return 2
	}
	runner := workflow.Runner{
		Store:       store,
		Config:      plan.Config,
		CommonCrawl: commonCrawlClient(plan.Config),
		RDAP:        availability.RDAPClient{UserAgent: plan.Config.CommonCrawl.UserAgent},
		Registrar:   registrarFromEnv(plan.Config),
		WorkerID:    "cli-resume",
	}
	stopProgress := func() {}
	if opts.output != "json" {
		stopProgress = startProgressPrinter(store, runID, stdout, 10*time.Second)
	}
	ctx, cancel := runContext(*maxRuntime)
	err = executeRunPlan(ctx, runner, runID, plan.Sources, plan.Crawls, plan.Modes, runLimits{
		CDXRecords:         *cdxLimit,
		WARCTasks:          *warcTaskLimit,
		WATTasks:           *watTaskLimit,
		GraphTasks:         *graphTaskLimit,
		AvailabilityChecks: *availabilityLimit,
		MaxRuntime:         *maxRuntime,
	}, *graphVertices, *graphEdges)
	cancel()
	stopProgress()
	if err != nil {
		if errors.Is(err, errWorkBudgetReached) {
			_ = store.SetRunStatus(context.Background(), runID, "paused")
			fmt.Fprintf(stdout, "execution=paused reason=%q\n", err.Error())
			snapshot, _ := store.Snapshot(context.Background(), runID)
			if opts.output == "json" {
				return writeJSON(stdout, snapshot)
			}
			fmt.Fprint(stdout, snapshot.RenderLines())
			return 0
		}
		if isControlledStop(err) {
			fmt.Fprintf(stderr, "resume stopped: %v\n", err)
			return 1
		}
		_ = store.SetRunStatus(context.Background(), runID, "failed")
		fmt.Fprintf(stderr, "resume execute: %v\n", err)
		return 1
	}
	if code, handled := finishIncompleteRunIfNeeded(store, runID, stdout, stderr, opts.output == "json", true); handled {
		return code
	}
	if err := store.SetRunStatus(context.Background(), runID, "completed"); err != nil {
		fmt.Fprintf(stderr, "complete run: %v\n", err)
		return 1
	}
	snapshot, err := store.Snapshot(context.Background(), runID)
	if err != nil {
		fmt.Fprintf(stderr, "load run: %v\n", err)
		return 1
	}
	if opts.output == "json" {
		return writeJSON(stdout, snapshot)
	}
	fmt.Fprint(stdout, snapshot.RenderLines())
	return 0
}

func runResults(args []string, stdout, stderr io.Writer, opts globalOptions) int {
	if len(args) != 1 {
		fmt.Fprint(stderr, "usage: edf results <run-id>\n")
		return 2
	}
	cfg, err := loadConfig(opts)
	if err != nil {
		fmt.Fprintf(stderr, "load config: %v\n", err)
		return 1
	}
	rows, err := storage.New(cfg.DataDirectory).Results(context.Background(), args[0])
	if err != nil {
		fmt.Fprintf(stderr, "load results: %v\n", err)
		return 1
	}
	if opts.output == "json" {
		return writeJSON(stdout, rows)
	}
	fmt.Fprint(stdout, renderResultsTable(rows))
	return 0
}

func runExport(args []string, stdout, stderr io.Writer, opts globalOptions) int {
	runID := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		runID = args[0]
		args = args[1:]
	}
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.SetOutput(stderr)
	format := fs.String("format", "csv", "export format: csv, json, parquet")
	output := fs.String("output", "", "output path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if runID == "" && fs.NArg() == 1 {
		runID = fs.Arg(0)
	}
	if runID == "" || fs.NArg() > 1 {
		fmt.Fprint(stderr, "usage: edf export <run-id> --format <csv|json|parquet> --output <path>\n")
		return 2
	}
	if !validExportFormat(*format) {
		fmt.Fprintf(stderr, "unsupported export format: %s\n", *format)
		return 2
	}
	if *output == "" {
		fmt.Fprint(stderr, "export requires --output\n")
		return 2
	}
	cfg, err := loadConfig(opts)
	if err != nil {
		fmt.Fprintf(stderr, "load config: %v\n", err)
		return 1
	}
	rows, _, err := writeAndRecordExport(context.Background(), storage.New(cfg.DataDirectory), runID, *output, *format)
	if err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "exported %d candidates for run %s as %s to %s\n", len(rows), runID, *format, *output)
	return 0
}

type repeatedFlag []string

func (r *repeatedFlag) String() string {
	return strings.Join(*r, ",")
}

func (r *repeatedFlag) Set(value string) error {
	*r = append(*r, value)
	return nil
}

func validateModes(modes []string) error {
	if len(modes) == 0 {
		return fmt.Errorf("run start requires at least one --mode value")
	}
	seen := map[string]bool{}
	for _, mode := range modes {
		switch mode {
		case "web_graph", "wat", "warc":
			seen[mode] = true
		default:
			return fmt.Errorf("unsupported mode: %s", mode)
		}
	}
	return nil
}

func validateRunLimitFlags(cdxLimit, warcLimit, watLimit, graphLimit, availabilityLimit int, maxRuntime time.Duration) error {
	for name, value := range map[string]int{
		"cdx-limit":          cdxLimit,
		"warc-task-limit":    warcLimit,
		"wat-task-limit":     watLimit,
		"graph-task-limit":   graphLimit,
		"availability-limit": availabilityLimit,
	} {
		if value < 0 {
			return fmt.Errorf("--%s must be zero or positive", name)
		}
	}
	if maxRuntime < 0 {
		return fmt.Errorf("--max-runtime must be zero or positive")
	}
	return nil
}

func applyCommonCrawlOverrides(cfg *config.Config, maxWARCRecordBytes, maxWARCDecodedBytes, maxWATObjectBytes, maxGraphObjectBytes int64) {
	if maxWARCRecordBytes > 0 {
		cfg.CommonCrawl.MaxWARCRecordBytes = maxWARCRecordBytes
	}
	if maxWARCDecodedBytes > 0 {
		cfg.CommonCrawl.MaxWARCDecodedBytes = maxWARCDecodedBytes
	}
	if maxWATObjectBytes > 0 {
		cfg.CommonCrawl.MaxWATObjectBytes = maxWATObjectBytes
	}
	if maxGraphObjectBytes > 0 {
		cfg.CommonCrawl.MaxGraphObjectBytes = maxGraphObjectBytes
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

func runContext(maxRuntime time.Duration) (context.Context, context.CancelFunc) {
	if maxRuntime <= 0 {
		return context.WithCancel(context.Background())
	}
	return context.WithTimeout(context.Background(), maxRuntime)
}

func validExportFormat(format string) bool {
	switch format {
	case "csv", "json", "parquet":
		return true
	default:
		return false
	}
}

func blockedSuffixes(cfg config.Config) []string {
	out := make([]string, 0, len(cfg.Availability.BlockedTLDs)+len(cfg.Availability.BlockedSuffixes))
	out = append(out, cfg.Availability.BlockedTLDs...)
	out = append(out, cfg.Availability.BlockedSuffixes...)
	return out
}

func isControlledStop(err error) bool {
	return errors.Is(err, storage.ErrRunPaused) || errors.Is(err, storage.ErrRunCancelled) || errors.Is(err, storage.ErrRunCompleted)
}

func isTerminalRunStatus(status string) bool {
	switch status {
	case "completed", "cancelled", "failed", "recoverable":
		return true
	default:
		return false
	}
}

func startProgressPrinter(store storage.Store, runID string, stdout io.Writer, interval time.Duration) func() {
	ctx, cancel := context.WithCancel(context.Background())
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
					continue
				}
				fmt.Fprint(stdout, "\n")
				fmt.Fprint(stdout, snapshot.RenderLines())
				if isTerminalRunStatus(snapshot.Status) {
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func executeRunPlan(ctx context.Context, runner workflow.Runner, runID string, sources, crawls, modes []string, limits runLimits, graphVertices, graphEdges string) error {
	if needsCDX(modes) {
		if err := runner.DiscoverCDX(ctx, runID, sources, crawls, limits.CDXRecords); err != nil {
			return fmt.Errorf("discovery: %w", err)
		}
	}
	for _, mode := range modes {
		switch mode {
		case "warc":
			if err := drainRunnableTasksWithLimit(ctx, runner.Store, runID, "warc_tasks", limits.WARCTasks, taskBatchLimit(limits.WARCTasks, runner.Config.Concurrency.WARC), func(batchLimit int) error {
				return runner.ProcessWARC(ctx, runID, batchLimit)
			}); err != nil {
				return fmt.Errorf("warc: %w", err)
			}
		case "wat":
			if err := drainRunnableTasksWithLimit(ctx, runner.Store, runID, "wat_tasks", limits.WATTasks, taskBatchLimit(limits.WATTasks, runner.Config.Concurrency.WAT), func(batchLimit int) error {
				return runner.ProcessWAT(ctx, runID, batchLimit)
			}); err != nil {
				return fmt.Errorf("wat: %w", err)
			}
		case "web_graph":
			releases := graphReleasesForSelections(crawls, runner.Config.CommonCrawl.GraphReleases)
			if graphVertices != "" || graphEdges != "" {
				if graphVertices == "" || graphEdges == "" {
					return fmt.Errorf("web_graph requires both --graph-vertices and --graph-edges")
				}
				releases = releases[:0]
				for _, crawl := range crawls {
					releases = append(releases, config.GraphRelease{ID: crawl, VertexPath: graphVertices, EdgePath: graphEdges})
				}
			}
			if len(releases) == 0 {
				loaded, err := graph.LoadGraphInfo(ctx, runner.Config.CommonCrawl.IndexBaseURL, runner.Config.CommonCrawl.UserAgent, runner.CommonCrawl.HTTPClient)
				if err != nil {
					return fmt.Errorf("web_graph graphinfo discovery: %w", err)
				}
				if release, ok := graph.ResolveRelease(crawls, loaded); ok {
					releases = append(releases, config.GraphRelease{ID: release.ID, CrawlIDs: release.Crawls, VertexPath: release.VertexPath, EdgePath: release.EdgePath})
				}
			}
			if len(releases) == 0 {
				return fmt.Errorf("web_graph could not resolve a domain graph release matching %s; configure commonCrawl.graphReleases or pass --graph-vertices/--graph-edges", strings.Join(crawls, ","))
			}
			for _, release := range releases {
				if err := runner.Store.AddGraphReleaseTask(ctx, runID, release.ID, release.VertexPath, release.EdgePath, sources); err != nil {
					return fmt.Errorf("graph setup: %w", err)
				}
			}
			if err := drainRunnableTasksWithLimit(ctx, runner.Store, runID, "graph_tasks", limits.GraphTasks, taskBatchLimit(limits.GraphTasks, runner.Config.Concurrency.Graph), func(batchLimit int) error {
				return runner.ProcessGraph(ctx, runID, batchLimit)
			}); err != nil {
				return fmt.Errorf("graph: %w", err)
			}
		}
	}
	if err := drainRunnableTasksWithLimit(ctx, runner.Store, runID, "availability_tasks", limits.AvailabilityChecks, taskBatchLimit(limits.AvailabilityChecks, runner.Config.Concurrency.DNS), func(batchLimit int) error {
		return runner.CheckAvailability(ctx, runID, batchLimit)
	}); err != nil {
		return fmt.Errorf("availability: %w", err)
	}
	return nil
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

func drainRunnableTasks(ctx context.Context, store storage.Store, runID, table string, process func() error) error {
	return drainRunnableTasksWithLimit(ctx, store, runID, table, 0, 1, func(int) error {
		return process()
	})
}

func drainRunnableTasksWithLimit(ctx context.Context, store storage.Store, runID, table string, totalLimit, batchLimit int, process func(batchLimit int) error) error {
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
			return errWorkBudgetReached
		}
		remaining := batchLimit
		if totalLimit > 0 && totalLimit-processed < remaining {
			remaining = totalLimit - processed
		}
		if remaining <= 0 {
			return nil
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

func needsCDX(modes []string) bool {
	for _, mode := range modes {
		if mode == "warc" || mode == "wat" {
			return true
		}
	}
	return false
}

func graphReleasesForSelections(selections []string, releases []config.GraphRelease) []config.GraphRelease {
	if len(selections) == 0 || len(releases) == 0 {
		return nil
	}
	selected := map[string]bool{}
	for _, selection := range selections {
		selection = strings.TrimSpace(selection)
		if selection == "" {
			continue
		}
		selected[selection] = true
		if strings.HasPrefix(selection, "CC-MAIN-") {
			parts := strings.Split(selection, "-")
			if len(parts) >= 4 && len(parts[2]) == 4 {
				// Keep exact crawl IDs primary; month aliases come from explicit config.
				selected[selection] = true
			}
		}
	}
	seen := map[string]bool{}
	var out []config.GraphRelease
	for _, release := range releases {
		if release.ID == "" || release.VertexPath == "" || release.EdgePath == "" {
			continue
		}
		match := selected[release.ID]
		for _, month := range release.Months {
			match = match || selected[strings.TrimSpace(month)]
		}
		for _, crawlID := range release.CrawlIDs {
			match = match || selected[strings.TrimSpace(crawlID)]
		}
		if !match || seen[release.ID] {
			continue
		}
		seen[release.ID] = true
		out = append(out, release)
	}
	return out
}

func sourceDomains(sources []normalization.Result) []string {
	out := make([]string, 0, len(sources))
	for _, source := range sources {
		out = append(out, source.RegistrableDomain)
	}
	return out
}

func renderEvents(events []storage.Event) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		component := event.Component
		if component == "" {
			component = "system"
		}
		out = append(out, fmt.Sprintf("%s [%s] %s", event.Severity, component, event.Message))
	}
	return out
}

func renderCandidatePreview(candidates []storage.Candidate, limit int) []tui.CandidatePreview {
	if limit <= 0 || limit > len(candidates) {
		limit = len(candidates)
	}
	out := make([]tui.CandidatePreview, 0, limit)
	for _, candidate := range candidates[:limit] {
		out = append(out, tui.CandidatePreview{
			Domain:     candidate.Domain,
			Status:     candidate.Status,
			Confidence: candidate.Confidence,
			Modes:      candidate.EvidenceModes,
			Evidence:   candidate.EvidenceSample,
		})
	}
	return out
}

func watchControlChannel(stdin io.Reader) <-chan string {
	if stdin == nil {
		return nil
	}
	ch := make(chan string)
	go func() {
		defer close(ch)
		scanner := bufio.NewScanner(stdin)
		for scanner.Scan() {
			key := strings.TrimSpace(scanner.Text())
			if key == "" {
				continue
			}
			ch <- key
		}
	}()
	return ch
}

func applyWatchControl(ctx context.Context, store storage.Store, runID, key string, stdout, stderr io.Writer) (bool, int) {
	snapshot, err := store.Snapshot(ctx, runID)
	if err != nil {
		fmt.Fprintf(stderr, "load run: %v\n", err)
		return true, 1
	}
	action := tui.ControlForKey(key, snapshot.Status)
	switch action {
	case tui.ControlPause:
		if err := store.SetRunStatus(ctx, runID, "pause_requested"); err != nil {
			fmt.Fprintf(stderr, "pause run: %v\n", err)
			return true, 1
		}
		fmt.Fprintf(stdout, "pause requested for run %s\n", runID)
		return false, 0
	case tui.ControlResume:
		if err := store.SetRunStatus(ctx, runID, "resume_requested"); err != nil {
			fmt.Fprintf(stderr, "resume run: %v\n", err)
			return true, 1
		}
		fmt.Fprintf(stdout, "resume requested for run %s\n", runID)
		return false, 0
	case tui.ControlStop:
		if err := store.SetRunStatus(ctx, runID, "cancelling"); err != nil {
			fmt.Fprintf(stderr, "stop run: %v\n", err)
			return true, 1
		}
		fmt.Fprintf(stdout, "stop requested for run %s\n", runID)
		return false, 0
	case tui.ControlQuit:
		return true, 0
	default:
		fmt.Fprintf(stderr, "unsupported watch control: %s\n", key)
		return true, 2
	}
}

func writeJSON(stdout io.Writer, value any) int {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fmt.Fprintf(stdout, `{"error":%q}`+"\n", err.Error())
		return 1
	}
	fmt.Fprintf(stdout, "%s\n", encoded)
	return 0
}

func renderResultsTable(rows []storage.Candidate) string {
	var builder strings.Builder
	builder.WriteString("Domain\tStatus\tConfidence\tTLD Policy\tSource Pages\tModes\tEvidence\tRegistrar\tPrice\tManual Verification\n")
	for _, row := range rows {
		fmt.Fprintf(&builder, "%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\n", row.Domain, row.Status, row.Confidence, row.TLDPolicyCategory, row.SourcePageCount, row.EvidenceModes, row.EvidenceSample, row.RegistrarProvider, row.PriceSummary, row.ManualVerification)
	}
	return builder.String()
}

func writeAndRecordExport(ctx context.Context, store storage.Store, runID, output, format string) ([]storage.Candidate, artifact.Manifest, error) {
	rows, err := store.Results(ctx, runID)
	if err != nil {
		return nil, artifact.Manifest{}, fmt.Errorf("load results: %w", err)
	}
	if dir := filepath.Dir(output); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, artifact.Manifest{}, fmt.Errorf("create export directory: %w", err)
		}
	}
	if err := writeExport(output, format, rows); err != nil {
		return nil, artifact.Manifest{}, fmt.Errorf("write export: %w", err)
	}
	compression := ""
	if format == "parquet" {
		compression = "zstd"
	}
	manifest, err := artifact.ManifestFile(output, format, compression, len(rows))
	if err != nil {
		return nil, artifact.Manifest{}, fmt.Errorf("manifest export: %w", err)
	}
	if err := store.RecordArtifact(ctx, runID, "export", manifest); err != nil {
		return nil, artifact.Manifest{}, fmt.Errorf("record export: %w", err)
	}
	return rows, manifest, nil
}

func writeExport(path, format string, rows []storage.Candidate) error {
	switch format {
	case "json":
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		defer file.Close()
		enc := json.NewEncoder(file)
		enc.SetIndent("", "  ")
		return enc.Encode(rows)
	case "csv":
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
	case "parquet":
		return parquet.WriteFile(path, parquetRows(rows), parquet.Compression(&zstd.Codec{}))
	default:
		return fmt.Errorf("unsupported export format: %s", format)
	}
}

var _ = progress.PlaceholderSnapshot

func registrarFromEnv(cfg config.Config) availability.RegistrarProvider {
	if cfg.Availability.RegistrarProvider == nil || *cfg.Availability.RegistrarProvider != "porkbun" {
		return nil
	}
	apiKey := os.Getenv("PORKBUN_API_KEY")
	secretKey := os.Getenv("PORKBUN_SECRET_KEY")
	if cfg.Availability.RegistrarAPIKeyRef != nil {
		apiKey = secretFromRef(*cfg.Availability.RegistrarAPIKeyRef)
	}
	if cfg.Availability.RegistrarSecretRef != nil {
		secretKey = secretFromRef(*cfg.Availability.RegistrarSecretRef)
	}
	if apiKey == "" || secretKey == "" {
		return nil
	}
	return availability.PorkbunProvider{APIKey: apiKey, SecretKey: secretKey}
}

func secretFromRef(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	if name, ok := strings.CutPrefix(ref, "env:"); ok {
		return os.Getenv(name)
	}
	return os.Getenv(ref)
}

type parquetCandidate struct {
	Domain                     string `parquet:"domain"`
	AvailabilityStatus         string `parquet:"availability_status"`
	Confidence                 string `parquet:"confidence"`
	TLDPolicyCategory          string `parquet:"tld_policy_category"`
	SourcePageCount            int    `parquet:"source_page_count"`
	GraphEdgeCount             int    `parquet:"graph_edge_count"`
	OutboundURLCount           int    `parquet:"outbound_url_count"`
	EvidenceModes              string `parquet:"evidence_modes"`
	EvidenceSample             string `parquet:"evidence_sample"`
	RegistrarProvider          string `parquet:"registrar_provider"`
	PriceSummary               string `parquet:"price_summary"`
	Score                      int    `parquet:"score"`
	ManualVerificationRequired string `parquet:"manual_verification_required"`
}

func parquetRows(rows []storage.Candidate) []parquetCandidate {
	out := make([]parquetCandidate, 0, len(rows))
	for _, row := range rows {
		out = append(out, parquetCandidate{
			Domain:                     row.Domain,
			AvailabilityStatus:         row.Status,
			Confidence:                 row.Confidence,
			TLDPolicyCategory:          row.TLDPolicyCategory,
			SourcePageCount:            row.SourcePageCount,
			GraphEdgeCount:             row.GraphEdgeCount,
			OutboundURLCount:           row.OutboundURLCount,
			EvidenceModes:              row.EvidenceModes,
			EvidenceSample:             row.EvidenceSample,
			RegistrarProvider:          row.RegistrarProvider,
			PriceSummary:               row.PriceSummary,
			Score:                      row.Score,
			ManualVerificationRequired: row.ManualVerification,
		})
	}
	return out
}
