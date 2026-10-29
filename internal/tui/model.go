package tui

import (
	"fmt"
	"strings"

	"expired-domain-finder/internal/progress"
)

type Model struct {
	Snapshot   progress.Snapshot
	Events     []string
	Candidates []CandidatePreview
}

type CandidatePreview struct {
	Domain     string
	Status     string
	Confidence string
	Modes      string
	Evidence   string
}

type ControlAction string

const (
	ControlNone   ControlAction = ""
	ControlPause  ControlAction = "pause"
	ControlResume ControlAction = "resume"
	ControlStop   ControlAction = "stop"
	ControlQuit   ControlAction = "quit"
)

func NewModel(snapshot progress.Snapshot) Model {
	return NewModelWithEvents(snapshot, nil)
}

func NewModelWithEvents(snapshot progress.Snapshot, events []string) Model {
	return NewModelDetailed(snapshot, events, nil)
}

func NewModelDetailed(snapshot progress.Snapshot, events []string, candidates []CandidatePreview) Model {
	if len(events) == 0 {
		events = []string{"no recent activity"}
	}
	return Model{
		Snapshot:   snapshot,
		Events:     events,
		Candidates: candidates,
	}
}

func (m Model) RenderPlain() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Expired Domain Finder\n\n")
	fmt.Fprintf(&b, "Run %s | %s | ETA %s | Cache hits %d | Recovery %s\n\n",
		m.Snapshot.RunID, m.Snapshot.Status, m.Snapshot.ETA, m.Snapshot.CacheHits, m.Snapshot.RecoveryState)
	fmt.Fprintf(&b, "Stages\n")
	for _, stage := range m.Snapshot.Stages {
		fmt.Fprintf(&b, "  %-13s %d/%d failed=%d\n", stage.Name, stage.Complete, stage.Total, stage.Failed)
	}
	fmt.Fprintf(&b, "\nActivity\n")
	for _, event := range m.Events {
		fmt.Fprintf(&b, "  %s\n", event)
	}
	fmt.Fprintf(&b, "\nCandidates\n")
	if len(m.Candidates) == 0 {
		fmt.Fprintf(&b, "  no candidates yet\n")
	}
	for _, candidate := range m.Candidates {
		evidence := candidate.Evidence
		if evidence == "" {
			evidence = "-"
		}
		fmt.Fprintf(&b, "  %-28s %-32s %-7s %-14s %s\n", candidate.Domain, candidate.Status, candidate.Confidence, candidate.Modes, evidence)
	}
	fmt.Fprintf(&b, "\nControls: p pause/resume | s stop | q quit\n")
	return b.String()
}

func ControlForKey(key, currentStatus string) ControlAction {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "p", "pause", "resume":
		switch currentStatus {
		case "pause_requested", "paused":
			return ControlResume
		default:
			return ControlPause
		}
	case "s", "stop":
		return ControlStop
	case "q", "quit":
		return ControlQuit
	default:
		return ControlNone
	}
}
