package tui

import (
	"strings"
	"testing"

	"expired-domain-finder/internal/progress"
)

func TestRenderPlain(t *testing.T) {
	model := NewModelDetailed(progress.PlaceholderSnapshot("run-1"), []string{"info [test] ready"}, []CandidatePreview{{Domain: "candidate.example", Status: "available_likely", Confidence: "medium", Modes: "warc", Evidence: "https://source.test/"}})
	got := model.RenderPlain()
	for _, want := range []string{"Expired Domain Finder", "Stages", "Activity", "Candidates", "candidate.example", "Controls"} {
		if !strings.Contains(got, want) {
			t.Fatalf("render missing %q: %s", want, got)
		}
	}
}

func TestControlForKey(t *testing.T) {
	tests := []struct {
		key    string
		status string
		want   ControlAction
	}{
		{key: "p", status: "running", want: ControlPause},
		{key: "p", status: "paused", want: ControlResume},
		{key: "s", status: "running", want: ControlStop},
		{key: "q", status: "running", want: ControlQuit},
		{key: "x", status: "running", want: ControlNone},
	}
	for _, tt := range tests {
		if got := ControlForKey(tt.key, tt.status); got != tt.want {
			t.Fatalf("ControlForKey(%q,%q)=%q want %q", tt.key, tt.status, got, tt.want)
		}
	}
}
