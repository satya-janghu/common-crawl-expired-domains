//go:build linux && cgo

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"expired-domain-finder/internal/config"
	"expired-domain-finder/internal/guiapp"
	"expired-domain-finder/internal/progress"
	"expired-domain-finder/internal/storage"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/widget"
)

func main() {
	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	engine := guiapp.NewEngine(cfg)
	if len(os.Args) > 1 && os.Args[1] == "--run-once" {
		os.Exit(runOnce(os.Args[2:], engine))
	}
	a := app.NewWithID("expired-domain-finder")
	w := a.NewWindow("Expired Domain Finder")
	w.Resize(fyne.NewSize(1120, 720))

	domain := widget.NewEntry()
	domain.SetPlaceHolder("example.com")
	year := widget.NewSelect(yearOptions(), nil)
	year.SetSelected(strconv.Itoa(time.Now().Year()))
	month := widget.NewSelect(monthOptions(), nil)
	month.SetSelected(fmt.Sprintf("%02d", int(time.Now().Month())))

	runID := widget.NewLabel("Run: none")
	crawlID := widget.NewLabel("Crawl: unresolved")
	status := widget.NewLabel("Status: idle")
	headline := widget.NewLabel("No active run")
	headline.TextStyle = fyne.TextStyle{Bold: true}
	candidateSummary := widget.NewLabel("No candidates discovered yet")
	candidateSummary.Wrapping = fyne.TextWrapWord
	progress := widget.NewProgressBar()
	stages := widget.NewMultiLineEntry()
	stages.Disable()
	candidates := widget.NewMultiLineEntry()
	candidates.Disable()
	runDetails := widget.NewMultiLineEntry()
	runDetails.Disable()
	alerts := widget.NewMultiLineEntry()
	alerts.Disable()
	output := widget.NewLabel("Output: none")
	output.Wrapping = fyne.TextWrapWord
	warnings := widget.NewLabel("")
	warnings.Wrapping = fyne.TextWrapWord
	metricLabels := make([]*widget.Label, 6)
	metricCards := make([]fyne.CanvasObject, 0, len(metricLabels))
	for i, metric := range guiapp.BuildMonitorView(progressSnapshotIdle(), nil, "").Metrics() {
		value := widget.NewLabel(metric.Value)
		value.TextStyle = fyne.TextStyle{Bold: true}
		metricLabels[i] = value
		metricCards = append(metricCards, widget.NewCard(metric.Label, "", value))
	}

	start := widget.NewButton("Start", nil)
	cancel := widget.NewButton("Cancel", nil)
	resume := widget.NewButton("Resume", nil)
	cancel.Disable()
	resume.Disable()
	resume.Hide()

	var mu sync.Mutex
	var cancelRun context.CancelFunc
	var lastRunID string

	setRunning := func(running bool) {
		fyne.Do(func() {
			if running {
				start.Disable()
				cancel.Enable()
				resume.Disable()
			} else {
				start.Enable()
				cancel.Disable()
			}
		})
	}

	applyUpdate := func(update guiapp.Update) {
		var view guiapp.MonitorView
		if update.Snapshot.RunID != "" {
			rows, _ := engine.Results(context.Background(), update.Snapshot.RunID)
			view = guiapp.BuildMonitorView(update.Snapshot, rows, "")
		}
		fyne.Do(func() {
			if update.Message != "" {
				status.SetText("Status: " + update.Message)
			}
			if update.CrawlID != "" {
				crawlID.SetText("Crawl: " + update.CrawlID)
			}
			if update.Snapshot.RunID == "" {
				return
			}
			runID.SetText("Run: " + update.Snapshot.RunID)
			status.SetText("Status: " + view.Status)
			headline.SetText(view.Headline)
			for i, metric := range view.Metrics() {
				if i < len(metricLabels) {
					metricLabels[i].SetText(metric.Value)
				}
			}
			candidateSummary.SetText(view.CandidateSummaryText())
			progress.SetValue(float64(view.OverallPct) / 100)
			stages.SetText(view.StageMonitorText())
			candidates.SetText(view.CandidateMonitorText())
			runDetails.SetText(view.RunDetailsText(strings.TrimPrefix(crawlID.Text, "Crawl: "), strings.TrimPrefix(output.Text, "Output: ")))
			alerts.SetText(view.AlertText())
			warnings.SetText(strings.Join(view.Alerts, "\n"))
		})
	}

	run := func(resumeExisting bool) {
		mu.Lock()
		if cancelRun != nil {
			mu.Unlock()
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancelRun = cancel
		mu.Unlock()
		setRunning(true)
		updates := make(chan guiapp.Update, 8)
		go func() {
			for update := range updates {
				applyUpdate(update)
			}
		}()
		go func() {
			defer func() {
				close(updates)
				mu.Lock()
				cancelRun = nil
				mu.Unlock()
				setRunning(false)
			}()
			var result guiapp.RunResult
			var err error
			if resumeExisting {
				result, err = engine.Resume(ctx, lastRunID, updates)
			} else {
				y, _ := strconv.Atoi(year.Selected)
				m, _ := strconv.Atoi(month.Selected)
				result, err = engine.Start(ctx, guiapp.RunRequest{Domain: domain.Text, Year: y, Month: time.Month(m)}, updates)
			}
			fyne.Do(func() {
				if result.RunID != "" {
					lastRunID = result.RunID
					runID.SetText("Run: " + result.RunID)
				}
				if result.CrawlID != "" {
					crawlID.SetText("Crawl: " + result.CrawlID)
				}
				if result.ExportPath != "" {
					output.SetText(fmt.Sprintf("Output: %s (%d rows)", result.ExportPath, result.ExportRows))
				}
				warnings.SetText(result.Warning)
				if result.Recoverable {
					resume.Show()
					resume.Enable()
				} else {
					resume.Disable()
					resume.Hide()
				}
				if err != nil {
					status.SetText("Status: error")
					dialog.ShowError(err, w)
					return
				}
				status.SetText("Status: " + result.Status)
			})
		}()
	}

	start.OnTapped = func() {
		if strings.TrimSpace(domain.Text) == "" {
			dialog.ShowInformation("Domain required", "Enter one source domain before starting.", w)
			return
		}
		run(false)
	}
	cancel.OnTapped = func() {
		mu.Lock()
		cancel := cancelRun
		id := lastRunID
		mu.Unlock()
		if id != "" {
			_ = engine.Cancel(context.Background(), id)
		}
		if cancel != nil {
			cancel()
		}
	}
	resume.OnTapped = func() {
		if lastRunID == "" {
			runs, err := storage.New(cfg.DataDirectory).ListRuns(context.Background())
			if err == nil && len(runs) > 0 {
				lastRunID = runs[0].ID
			}
		}
		if lastRunID == "" {
			dialog.ShowInformation("No run", "No resumable run is selected.", w)
			return
		}
		run(true)
	}

	selector := widget.NewCard("Run Control", "Source and crawl window",
		container.NewVBox(
			widget.NewLabel("Domain"), domain,
			container.NewGridWithColumns(2, container.NewVBox(widget.NewLabel("Year"), year), container.NewVBox(widget.NewLabel("Month"), month)),
			container.NewGridWithColumns(3, start, cancel, resume),
		),
	)
	runCard := widget.NewCard("Run Details", "", runDetails)
	sidebar := container.NewVBox(selector, runCard, widget.NewCard("Export", "", output))
	overview := widget.NewCard("Operations Monitor", "", container.NewVBox(headline, candidateSummary, progress, container.NewGridWithColumns(3, metricCards...)))
	stageCard := widget.NewCard("Stage Monitor", "Throughput, ETA, and failed units", container.NewScroll(stages))
	candidateCard := widget.NewCard("Candidate Intelligence", "Top domains found so far", container.NewScroll(candidates))
	alertCard := widget.NewCard("Alerts", "", alerts)
	mainPane := container.NewBorder(overview, alertCard, nil, nil, container.NewHSplit(stageCard, candidateCard))
	content := container.NewHSplit(sidebar, mainPane)
	content.SetOffset(0.28)
	w.SetContent(content)
	w.ShowAndRun()
}

func progressSnapshotIdle() progress.Snapshot {
	return progress.Snapshot{Status: "idle", RecoveryState: "enabled"}
}

func runOnce(args []string, engine guiapp.Engine) int {
	fs := flag.NewFlagSet("edf-gui --run-once", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	domain := fs.String("domain", "", "source domain")
	year := fs.Int("year", time.Now().Year(), "crawl selection year")
	month := fs.Int("month", int(time.Now().Month()), "crawl selection month")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*domain) == "" {
		fmt.Fprintln(os.Stderr, "--domain is required")
		return 2
	}
	updates := make(chan guiapp.Update, 16)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for update := range updates {
			if update.CrawlID != "" {
				fmt.Printf("crawl=%s\n", update.CrawlID)
			}
			if update.Message != "" {
				fmt.Printf("message=%s\n", update.Message)
			}
			if update.Snapshot.RunID != "" {
				fmt.Print(update.Snapshot.RenderLines())
			}
		}
	}()
	result, err := engine.Start(context.Background(), guiapp.RunRequest{Domain: *domain, Year: *year, Month: time.Month(*month)}, updates)
	close(updates)
	<-done
	if err != nil {
		fmt.Fprintf(os.Stderr, "run failed: %v\n", err)
		return 1
	}
	fmt.Printf("run_id=%s\n", result.RunID)
	fmt.Printf("crawl=%s\n", result.CrawlID)
	fmt.Printf("status=%s\n", result.Status)
	fmt.Printf("export=%s rows=%d\n", result.ExportPath, result.ExportRows)
	if result.Warning != "" {
		fmt.Printf("warning=%s\n", result.Warning)
	}
	if result.Status != "completed" {
		return 1
	}
	return 0
}

func yearOptions() []string {
	now := time.Now().Year()
	out := make([]string, 0, now-2007)
	for y := now; y >= 2008; y-- {
		out = append(out, strconv.Itoa(y))
	}
	return out
}

func monthOptions() []string {
	out := make([]string, 0, 12)
	for m := 1; m <= 12; m++ {
		out = append(out, fmt.Sprintf("%02d", m))
	}
	return out
}
