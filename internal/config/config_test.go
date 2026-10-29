package config

import (
	"path/filepath"
	"testing"
)

func TestDefaultConfigIncludesCLITUIRuntimeDefaults(t *testing.T) {
	cfg := Default()

	if !cfg.Display.TUIEnabledByDefault {
		t.Fatal("TUI should be enabled by default for interactive terminals")
	}
	if cfg.Display.DefaultOutputFormat != "text" {
		t.Fatalf("output format = %q, want text", cfg.Display.DefaultOutputFormat)
	}
	if cfg.Display.ProgressIntervalSecs != 10 {
		t.Fatalf("progress interval = %d, want 10", cfg.Display.ProgressIntervalSecs)
	}
	if cfg.Runtime.QueueSize <= 0 {
		t.Fatalf("queue size = %d, want positive", cfg.Runtime.QueueSize)
	}
	if cfg.Runtime.SQLiteBatchRows != 1000 {
		t.Fatalf("sqlite batch rows = %d, want 1000", cfg.Runtime.SQLiteBatchRows)
	}
}

func TestConfigRoundTripsGraphReleases(t *testing.T) {
	cfg := Default()
	cfg.DataDirectory = t.TempDir()
	cfg.CommonCrawl.GraphReleases = []GraphRelease{{
		ID:         "cc-domain-graph-2026-04",
		Months:     []string{"2026-04"},
		CrawlIDs:   []string{"CC-MAIN-2026-18"},
		VertexPath: "graphs/vertices.txt.zst",
		EdgePath:   "graphs/edges.txt.zst",
	}}
	path := filepath.Join(cfg.DataDirectory, "config.json")
	if err := Save(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.CommonCrawl.GraphReleases) != 1 || loaded.CommonCrawl.GraphReleases[0].ID != "cc-domain-graph-2026-04" {
		t.Fatalf("graph releases = %+v", loaded.CommonCrawl.GraphReleases)
	}
}

func TestConfigValidatesGraphReleases(t *testing.T) {
	cfg := Default()
	cfg.CommonCrawl.GraphReleases = []GraphRelease{{ID: "broken", Months: []string{"2026-04"}}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for graph release without paths")
	}
}

func TestRedactedRemovesRegistrarSecretRefs(t *testing.T) {
	apiRef := "env:API"
	secretRef := "env:SECRET"
	cfg := Default()
	cfg.Availability.RegistrarAPIKeyRef = &apiRef
	cfg.Availability.RegistrarSecretRef = &secretRef
	redacted := cfg.Redacted()
	if redacted.Availability.RegistrarAPIKeyRef != nil || redacted.Availability.RegistrarSecretRef != nil {
		t.Fatalf("redacted refs = %+v", redacted.Availability)
	}
}

func TestConfigRoundTripsBlockedSuffixes(t *testing.T) {
	cfg := Default()
	cfg.DataDirectory = t.TempDir()
	cfg.Availability.BlockedSuffixes = []string{"blocked.example"}
	path := filepath.Join(cfg.DataDirectory, "config.json")
	if err := Save(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Availability.BlockedSuffixes) != 1 || loaded.Availability.BlockedSuffixes[0] != "blocked.example" {
		t.Fatalf("blocked suffixes = %+v", loaded.Availability.BlockedSuffixes)
	}
}
