package guiapp

import (
	"strings"
	"testing"
	"time"

	"expired-domain-finder/internal/availability"
	"expired-domain-finder/internal/progress"
	"expired-domain-finder/internal/storage"
)

func TestBuildMonitorViewSummarizesProgressCandidatesAndActions(t *testing.T) {
	snapshot := progress.Snapshot{
		RunID:         "run-1",
		Status:        "running",
		OverallPct:    47,
		ETA:           95 * time.Second,
		Elapsed:       3*time.Minute + 4*time.Second,
		CacheHits:     12,
		RecoveryState: "enabled",
		Stages: []progress.Stage{
			{Name: progress.Discovery, Complete: 10, Total: 10, Rate: 2, Confidence: "high"},
			{Name: progress.Extraction, Complete: 20, Total: 100, Failed: 1, Rate: 0.5, ETA: 2 * time.Minute, Confidence: "medium"},
		},
	}
	candidates := []storage.Candidate{
		{Domain: "strong.example", Status: string(availability.AvailableRegistrarConfirmed), Confidence: string(availability.ConfidenceHighest), EvidenceModes: "warc+wat", EvidenceSample: "https://source/a", Score: 900, ManualVerification: "yes"},
		{Domain: "likely.example", Status: string(availability.AvailableLikely), Confidence: string(availability.ConfidenceMedium), EvidenceModes: "warc", Score: 700, ManualVerification: "yes"},
		{Domain: "registered.example", Status: string(availability.RegisteredActive), Confidence: string(availability.ConfidenceHigh), EvidenceModes: "wat", Score: 300},
		{Domain: "restricted.example", Status: string(availability.RestrictedTLD), Confidence: string(availability.ConfidenceHigh), EvidenceModes: "warc", Score: 200},
		{Domain: "review.example", Status: string(availability.Ambiguous), Confidence: string(availability.ConfidenceLow), Score: 100},
	}

	view := BuildMonitorView(snapshot, candidates, "network warning")

	if view.Headline != "Crawl in progress: 47%" || view.PrimaryAction != "Cancel" || !view.CanCancel {
		t.Fatalf("unexpected status/action fields: %+v", view)
	}
	if view.CandidateSummary.Available != 2 || view.CandidateSummary.Registered != 1 || view.CandidateSummary.Restricted != 1 || view.CandidateSummary.Ambiguous != 1 {
		t.Fatalf("candidate summary = %+v", view.CandidateSummary)
	}
	if view.CandidateSummary.BestConfidence != string(availability.ConfidenceHighest) || view.CandidateSummary.BestEvidence != "warc" {
		t.Fatalf("candidate quality summary = %+v", view.CandidateSummary)
	}
	if len(view.StageRows) != 2 || view.StageRows[0].State != "done" || view.StageRows[1].State != "attention" || view.StageRows[1].Percent != 20 {
		t.Fatalf("stage rows = %+v", view.StageRows)
	}
	if len(view.Alerts) != 2 || !strings.Contains(strings.Join(view.Alerts, " "), "Extraction has 1 failed") {
		t.Fatalf("alerts = %+v", view.Alerts)
	}
	if len(view.TopCandidates) != 5 || view.TopCandidates[0].Domain != "strong.example" || !view.TopCandidates[0].NeedsCheck {
		t.Fatalf("top candidates = %+v", view.TopCandidates)
	}
	if view.ExportExpectation != "2 export-ready candidates" {
		t.Fatalf("export expectation = %q", view.ExportExpectation)
	}
	if len(view.Metrics()) != 6 {
		t.Fatalf("metrics = %+v", view.Metrics())
	}
	if !strings.Contains(view.RunDetailsText("CC-MAIN-2026-04", "/tmp/out.csv"), "Export: /tmp/out.csv") {
		t.Fatalf("run details = %q", view.RunDetailsText("CC-MAIN-2026-04", "/tmp/out.csv"))
	}
	if !strings.Contains(view.StageMonitorText(), "Extraction") || !strings.Contains(view.StageMonitorText(), "failed 1") {
		t.Fatalf("stage text = %q", view.StageMonitorText())
	}
	if !strings.Contains(view.CandidateMonitorText(), "strong.example") || !strings.Contains(view.CandidateMonitorText(), "manual-check") {
		t.Fatalf("candidate text = %q", view.CandidateMonitorText())
	}
	if !strings.Contains(view.AlertText(), "network warning") {
		t.Fatalf("alert text = %q", view.AlertText())
	}
	if !strings.Contains(view.CandidateSummaryText(), "2 available") {
		t.Fatalf("summary text = %q", view.CandidateSummaryText())
	}
}

func TestBuildMonitorViewHandlesEmptyAndRecoverableStates(t *testing.T) {
	view := BuildMonitorView(progress.Snapshot{
		Status:        "recoverable",
		OverallPct:    120,
		RecoveryState: "recovered",
		Stages:        []progress.Stage{{Name: progress.Export}},
	}, nil, "")

	if view.OverallPct != 100 || view.Headline != "Run needs attention" {
		t.Fatalf("view = %+v", view)
	}
	if !view.CanResume || view.PrimaryAction != "Resume" || view.SecondaryAction != "Export current results" {
		t.Fatalf("actions = primary %q secondary %q resume=%v", view.PrimaryAction, view.SecondaryAction, view.CanResume)
	}
	if view.CandidateSummary.AvailabilityMix != "No candidates discovered yet" || view.ExportExpectation != "No export-ready candidates yet" {
		t.Fatalf("empty summary = %+v export=%q", view.CandidateSummary, view.ExportExpectation)
	}
	if len(view.Alerts) != 2 {
		t.Fatalf("alerts = %+v", view.Alerts)
	}
	if view.StageMonitorText() == "" || view.CandidateMonitorText() == "" || view.AlertText() == "" {
		t.Fatalf("empty state text missing: stage=%q candidates=%q alerts=%q", view.StageMonitorText(), view.CandidateMonitorText(), view.AlertText())
	}
}

func TestTopCandidateRowsSortsByScoreThenDomainAndHonorsLimit(t *testing.T) {
	rows := topCandidateRows([]storage.Candidate{
		{Domain: "b.example", Score: 10},
		{Domain: "a.example", Score: 10},
		{Domain: "c.example", Score: 20},
	}, 2)

	if len(rows) != 2 || rows[0].Domain != "c.example" || rows[1].Domain != "a.example" {
		t.Fatalf("rows = %+v", rows)
	}
}

func TestHeadlineAndActionStates(t *testing.T) {
	cases := []struct {
		status    string
		headline  string
		primary   string
		secondary string
		resume    bool
		cancel    bool
	}{
		{status: "completed", headline: "Run complete", primary: "Open export", secondary: "Start new run"},
		{status: "failed", headline: "Run failed", primary: "Inspect issue", secondary: "Resume", resume: true},
		{status: "cancelled", headline: "Run cancelled", primary: "Start"},
		{status: "paused", headline: "Run needs attention", primary: "Resume", secondary: "Export current results", resume: true},
		{status: "created", headline: "Created", primary: "Cancel", cancel: true},
		{status: "", headline: "No active run", primary: "Start"},
	}
	for _, tc := range cases {
		view := BuildMonitorView(progress.Snapshot{Status: tc.status}, nil, "alert")
		if view.Headline != tc.headline || view.PrimaryAction != tc.primary || view.SecondaryAction != tc.secondary || view.CanResume != tc.resume || view.CanCancel != tc.cancel {
			t.Fatalf("status %q view = %+v", tc.status, view)
		}
	}
}

func TestFormattingHelpers(t *testing.T) {
	cases := map[time.Duration]string{
		0:                             "unknown",
		17 * time.Second:              "17s",
		2*time.Minute + 3*time.Second: "2m 03s",
		3*time.Hour + 4*time.Minute:   "3h 04m",
	}
	for value, want := range cases {
		if got := durationLabel(value); got != want {
			t.Fatalf("durationLabel(%s) = %q, want %q", value, got, want)
		}
	}
	if got := rateLabel(0); got != "no recent throughput" {
		t.Fatalf("rateLabel(0) = %q", got)
	}
	if got := rateLabel(0.5); got != "30.00/min" {
		t.Fatalf("rateLabel(0.5) = %q", got)
	}
	if got := rateLabel(2.25); got != "2.2/s" {
		t.Fatalf("rateLabel(2.25) = %q", got)
	}
}
