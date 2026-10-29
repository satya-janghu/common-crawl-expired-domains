package progress

import (
	"fmt"
	"strings"
	"time"
)

type StageName string

const (
	Discovery    StageName = "Discovery"
	Extraction   StageName = "Extraction"
	DomainChecks StageName = "Domain Checks"
	Export       StageName = "Export"
)

type Stage struct {
	Name       StageName
	Complete   int
	Total      int
	Failed     int
	ETA        time.Duration
	Rate       float64
	Confidence string
}

type Snapshot struct {
	RunID         string
	Status        string
	OverallPct    int
	ETA           time.Duration
	Elapsed       time.Duration
	CacheHits     int
	RecoveryState string
	Stages        []Stage
}

func PlaceholderSnapshot(runID string) Snapshot {
	return Snapshot{
		RunID:         runID,
		Status:        "created",
		OverallPct:    0,
		ETA:           0,
		Elapsed:       0,
		CacheHits:     0,
		RecoveryState: "enabled",
		Stages: []Stage{
			{Name: Discovery},
			{Name: Extraction},
			{Name: DomainChecks},
			{Name: Export},
		},
	}
}

func (s Snapshot) RenderLines() string {
	var b strings.Builder
	fmt.Fprintf(&b, "run_id=%s status=%s progress=%d%% eta=%s elapsed=%s cache_hits=%d recovery=%s\n",
		s.RunID, s.Status, s.OverallPct, s.ETA, s.Elapsed, s.CacheHits, s.RecoveryState)
	for _, stage := range s.Stages {
		fmt.Fprintf(&b, "stage=%q complete=%d total=%d failed=%d eta=%s eta_confidence=%s rate=%.2f\n",
			stage.Name, stage.Complete, stage.Total, stage.Failed, stage.ETA, stage.Confidence, stage.Rate)
	}
	return b.String()
}

func EMA(previous, current, alpha float64) float64 {
	if previous == 0 {
		return current
	}
	return alpha*current + (1-alpha)*previous
}

type Counter struct {
	Stage              StageName
	Completed          int
	TotalKnown         int
	ProjectedRemaining int
	Failed             int
	RecentRate         float64
	LastCompleted      int
	LastUpdated        time.Time
}

func (c Counter) Recalculate(now time.Time) Counter {
	if c.LastUpdated.IsZero() {
		c.LastUpdated = now
		c.LastCompleted = c.Completed
		return c
	}
	elapsed := now.Sub(c.LastUpdated).Seconds()
	if elapsed <= 0 {
		return c
	}
	delta := c.Completed - c.LastCompleted
	if delta < 0 {
		delta = 0
	}
	c.RecentRate = EMA(c.RecentRate, float64(delta)/elapsed, 0.35)
	c.LastUpdated = now
	c.LastCompleted = c.Completed
	return c
}

func (c Counter) StageSnapshot() Stage {
	total := c.TotalKnown
	if c.ProjectedRemaining > 0 {
		total = c.Completed + c.ProjectedRemaining
		if total < c.TotalKnown {
			total = c.TotalKnown
		}
	}
	remaining := total - c.Completed
	if remaining < 0 {
		remaining = 0
	}
	eta := time.Duration(0)
	confidence := "low"
	if c.RecentRate > 0 {
		eta = time.Duration(float64(remaining)/c.RecentRate) * time.Second
		confidence = "medium"
		if c.TotalKnown > 0 && c.ProjectedRemaining == 0 {
			confidence = "high"
		}
	}
	return Stage{
		Name:       c.Stage,
		Complete:   c.Completed,
		Total:      total,
		Failed:     c.Failed,
		ETA:        eta,
		Rate:       c.RecentRate,
		Confidence: confidence,
	}
}
