package guiapp

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"expired-domain-finder/internal/availability"
	"expired-domain-finder/internal/progress"
	"expired-domain-finder/internal/storage"
)

type MonitorView struct {
	RunID             string
	Status            string
	Headline          string
	OverallPct        int
	ETA               string
	Elapsed           string
	CacheHits         int
	RecoveryState     string
	StageRows         []StageRow
	CandidateSummary  CandidateSummary
	TopCandidates     []CandidateRow
	Alerts            []string
	PrimaryAction     string
	SecondaryAction   string
	CanResume         bool
	CanCancel         bool
	ExportExpectation string
}

type Metric struct {
	Label string
	Value string
}

type StageRow struct {
	Name       string
	Complete   int
	Total      int
	Failed     int
	Percent    int
	Rate       string
	ETA        string
	Confidence string
	State      string
}

type CandidateSummary struct {
	Total           int
	Available       int
	Restricted      int
	Registered      int
	Ambiguous       int
	ManualReview    int
	BestConfidence  string
	BestEvidence    string
	AvailabilityMix string
}

type CandidateRow struct {
	Domain     string
	Status     string
	Confidence string
	Evidence   string
	Score      int
	NeedsCheck bool
}

func BuildMonitorView(snapshot progress.Snapshot, candidates []storage.Candidate, warning string) MonitorView {
	view := MonitorView{
		RunID:         snapshot.RunID,
		Status:        snapshot.Status,
		OverallPct:    clampPercent(snapshot.OverallPct),
		ETA:           durationLabel(snapshot.ETA),
		Elapsed:       durationLabel(snapshot.Elapsed),
		CacheHits:     snapshot.CacheHits,
		RecoveryState: snapshot.RecoveryState,
		Alerts:        alertMessages(snapshot, warning),
	}
	view.Headline = headlineFor(snapshot)
	view.StageRows = stageRows(snapshot.Stages)
	view.CandidateSummary = summarizeCandidates(candidates)
	view.TopCandidates = topCandidateRows(candidates, 8)
	view.PrimaryAction, view.SecondaryAction, view.CanResume, view.CanCancel = actionsFor(snapshot.Status, len(view.Alerts) > 0)
	if view.CandidateSummary.Available > 0 {
		view.ExportExpectation = fmt.Sprintf("%d export-ready candidates", view.CandidateSummary.Available)
	} else {
		view.ExportExpectation = "No export-ready candidates yet"
	}
	return view
}

func (v MonitorView) Metrics() []Metric {
	return []Metric{
		{Label: "Progress", Value: fmt.Sprintf("%d%%", v.OverallPct)},
		{Label: "ETA", Value: v.ETA},
		{Label: "Elapsed", Value: v.Elapsed},
		{Label: "Cache Hits", Value: fmt.Sprint(v.CacheHits)},
		{Label: "Candidates", Value: fmt.Sprint(v.CandidateSummary.Total)},
		{Label: "Export Ready", Value: fmt.Sprint(v.CandidateSummary.Available)},
	}
}

func (v MonitorView) RunDetailsText(crawlID, exportPath string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Status: %s\n", emptyFallback(v.Status, "idle"))
	fmt.Fprintf(&b, "Run: %s\n", emptyFallback(v.RunID, "none"))
	fmt.Fprintf(&b, "Crawl: %s\n", emptyFallback(crawlID, "unresolved"))
	fmt.Fprintf(&b, "Recovery: %s\n", emptyFallback(v.RecoveryState, "enabled"))
	if exportPath != "" {
		fmt.Fprintf(&b, "Export: %s\n", exportPath)
	} else {
		fmt.Fprintf(&b, "Export: %s\n", v.ExportExpectation)
	}
	return strings.TrimRight(b.String(), "\n")
}

func (v MonitorView) StageMonitorText() string {
	if len(v.StageRows) == 0 {
		return "No stage activity yet."
	}
	var b strings.Builder
	for _, stage := range v.StageRows {
		failed := ""
		if stage.Failed > 0 {
			failed = fmt.Sprintf("  failed %d", stage.Failed)
		}
		fmt.Fprintf(&b, "%-14s %3d%%  %d/%d  %-9s  %s  eta %s%s\n", stage.Name, stage.Percent, stage.Complete, stage.Total, stage.State, stage.Rate, stage.ETA, failed)
	}
	return strings.TrimRight(b.String(), "\n")
}

func (v MonitorView) CandidateMonitorText() string {
	if len(v.TopCandidates) == 0 {
		return "No candidates have been classified yet."
	}
	var b strings.Builder
	for _, row := range v.TopCandidates {
		check := ""
		if row.NeedsCheck {
			check = "  manual-check"
		}
		fmt.Fprintf(&b, "%-32s %-28s %-7s score %d%s\n", row.Domain, row.Status, row.Confidence, row.Score, check)
	}
	return strings.TrimRight(b.String(), "\n")
}

func (v MonitorView) AlertText() string {
	if len(v.Alerts) == 0 {
		return "No alerts."
	}
	return strings.Join(v.Alerts, "\n")
}

func (v MonitorView) CandidateSummaryText() string {
	return fmt.Sprintf("%s | best confidence %s | evidence %s", v.CandidateSummary.AvailabilityMix, v.CandidateSummary.BestConfidence, v.CandidateSummary.BestEvidence)
}

func stageRows(stages []progress.Stage) []StageRow {
	rows := make([]StageRow, 0, len(stages))
	for _, stage := range stages {
		percent := 0
		if stage.Total > 0 {
			percent = clampPercent(stage.Complete * 100 / stage.Total)
		}
		rows = append(rows, StageRow{
			Name:       string(stage.Name),
			Complete:   stage.Complete,
			Total:      stage.Total,
			Failed:     stage.Failed,
			Percent:    percent,
			Rate:       rateLabel(stage.Rate),
			ETA:        durationLabel(stage.ETA),
			Confidence: stage.Confidence,
			State:      stageState(stage),
		})
	}
	return rows
}

func emptyFallback(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func summarizeCandidates(candidates []storage.Candidate) CandidateSummary {
	summary := CandidateSummary{Total: len(candidates), BestConfidence: "none", BestEvidence: "none"}
	confidenceRank := map[string]int{"low": 1, "medium": 2, "high": 3, "highest": 4}
	bestRank := 0
	evidence := map[string]int{}
	for _, candidate := range candidates {
		switch candidate.Status {
		case string(availability.AvailableRegistrarConfirmed), string(availability.AvailableLikely):
			summary.Available++
		case string(availability.RestrictedTLD), string(availability.Reserved), string(availability.UnsupportedTLD), string(availability.Premium):
			summary.Restricted++
		case string(availability.RegisteredActive), string(availability.RegisteredNoDNS):
			summary.Registered++
		default:
			summary.Ambiguous++
		}
		if strings.EqualFold(candidate.ManualVerification, "yes") {
			summary.ManualReview++
		}
		if rank := confidenceRank[candidate.Confidence]; rank > bestRank {
			bestRank = rank
			summary.BestConfidence = candidate.Confidence
		}
		for _, mode := range strings.Split(candidate.EvidenceModes, "+") {
			mode = strings.TrimSpace(mode)
			if mode != "" {
				evidence[mode]++
			}
		}
	}
	if len(evidence) > 0 {
		type pair struct {
			mode  string
			count int
		}
		pairs := make([]pair, 0, len(evidence))
		for mode, count := range evidence {
			pairs = append(pairs, pair{mode: mode, count: count})
		}
		sort.Slice(pairs, func(i, j int) bool {
			if pairs[i].count == pairs[j].count {
				return pairs[i].mode < pairs[j].mode
			}
			return pairs[i].count > pairs[j].count
		})
		summary.BestEvidence = pairs[0].mode
	}
	if summary.Total > 0 {
		summary.AvailabilityMix = fmt.Sprintf("%d available, %d registered, %d restricted, %d review", summary.Available, summary.Registered, summary.Restricted, summary.Ambiguous)
	} else {
		summary.AvailabilityMix = "No candidates discovered yet"
	}
	return summary
}

func topCandidateRows(candidates []storage.Candidate, limit int) []CandidateRow {
	if limit <= 0 || len(candidates) == 0 {
		return nil
	}
	sorted := append([]storage.Candidate(nil), candidates...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Score == sorted[j].Score {
			return sorted[i].Domain < sorted[j].Domain
		}
		return sorted[i].Score > sorted[j].Score
	})
	if len(sorted) > limit {
		sorted = sorted[:limit]
	}
	rows := make([]CandidateRow, 0, len(sorted))
	for _, candidate := range sorted {
		rows = append(rows, CandidateRow{
			Domain:     candidate.Domain,
			Status:     candidate.Status,
			Confidence: candidate.Confidence,
			Evidence:   compactEvidence(candidate),
			Score:      candidate.Score,
			NeedsCheck: strings.EqualFold(candidate.ManualVerification, "yes"),
		})
	}
	return rows
}

func alertMessages(snapshot progress.Snapshot, warning string) []string {
	var alerts []string
	if warning != "" {
		alerts = append(alerts, warning)
	}
	if snapshot.RecoveryState == "recovered" {
		alerts = append(alerts, "Recovered expired leases from an earlier interrupted run.")
	}
	for _, stage := range snapshot.Stages {
		if stage.Failed > 0 {
			alerts = append(alerts, fmt.Sprintf("%s has %d failed units.", stage.Name, stage.Failed))
		}
	}
	if snapshot.Status == "recoverable" || snapshot.Status == "paused" {
		alerts = append(alerts, "Resume is available.")
	}
	return alerts
}

func headlineFor(snapshot progress.Snapshot) string {
	switch snapshot.Status {
	case "completed":
		return "Run complete"
	case "failed":
		return "Run failed"
	case "cancelled":
		return "Run cancelled"
	case "recoverable", "paused":
		return "Run needs attention"
	case "running":
		if snapshot.OverallPct > 0 {
			return fmt.Sprintf("Crawl in progress: %d%%", clampPercent(snapshot.OverallPct))
		}
		return "Preparing crawl"
	default:
		if snapshot.Status == "" {
			return "No active run"
		}
		return strings.Title(snapshot.Status)
	}
}

func actionsFor(status string, hasAlerts bool) (primary, secondary string, canResume, canCancel bool) {
	switch status {
	case "running", "created", "resume_requested":
		return "Cancel", "", false, true
	case "recoverable", "paused":
		return "Resume", "Export current results", true, false
	case "completed":
		return "Open export", "Start new run", false, false
	case "failed":
		if hasAlerts {
			return "Inspect issue", "Resume", true, false
		}
		return "Start new run", "", false, false
	default:
		return "Start", "", false, false
	}
}

func stageState(stage progress.Stage) string {
	if stage.Failed > 0 {
		return "attention"
	}
	if stage.Total > 0 && stage.Complete >= stage.Total {
		return "done"
	}
	if stage.Complete > 0 {
		return "active"
	}
	return "waiting"
}

func compactEvidence(candidate storage.Candidate) string {
	mode := candidate.EvidenceModes
	if mode == "" {
		mode = "evidence"
	}
	if candidate.EvidenceSample == "" {
		return mode
	}
	return mode + ": " + candidate.EvidenceSample
}

func durationLabel(value time.Duration) string {
	if value <= 0 {
		return "unknown"
	}
	if value < time.Minute {
		return fmt.Sprintf("%ds", int(value.Seconds()))
	}
	if value < time.Hour {
		return fmt.Sprintf("%dm %02ds", int(value.Minutes()), int(value.Seconds())%60)
	}
	return fmt.Sprintf("%dh %02dm", int(value.Hours()), int(value.Minutes())%60)
}

func rateLabel(rate float64) string {
	if rate <= 0 {
		return "no recent throughput"
	}
	if rate < 1 {
		return fmt.Sprintf("%.2f/min", rate*60)
	}
	return fmt.Sprintf("%.1f/s", rate)
}

func clampPercent(value int) int {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}
