package progress

import (
	"strings"
	"testing"
	"time"
)

func TestEMA(t *testing.T) {
	got := EMA(10, 20, 0.25)
	if got != 12.5 {
		t.Fatalf("EMA = %v, want 12.5", got)
	}
}

func TestPlaceholderSnapshotRendersStages(t *testing.T) {
	got := PlaceholderSnapshot("run-1").RenderLines()
	if !strings.Contains(got, `stage="Discovery"`) {
		t.Fatalf("snapshot missing Discovery stage: %s", got)
	}
	if !strings.Contains(got, "recovery=enabled") {
		t.Fatalf("snapshot missing recovery state: %s", got)
	}
	if !strings.Contains(got, "eta_confidence=") {
		t.Fatalf("snapshot missing ETA confidence: %s", got)
	}
}

func TestCounterRecalculatesETA(t *testing.T) {
	start := time.Unix(0, 0)
	counter := Counter{Stage: DomainChecks, Completed: 10, TotalKnown: 20, LastCompleted: 0, LastUpdated: start}
	counter = counter.Recalculate(start.Add(10 * time.Second))
	stage := counter.StageSnapshot()
	if stage.Rate != 1 {
		t.Fatalf("rate = %f, want 1", stage.Rate)
	}
	if stage.ETA != 10*time.Second {
		t.Fatalf("eta = %s, want 10s", stage.ETA)
	}
	if stage.Confidence != "high" {
		t.Fatalf("confidence = %s, want high", stage.Confidence)
	}
}
