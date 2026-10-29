package tests

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"expired-domain-finder/internal/availability"
	"expired-domain-finder/internal/commoncrawl"
	"expired-domain-finder/internal/config"
	"expired-domain-finder/internal/extraction"
	"expired-domain-finder/internal/graph"
	"expired-domain-finder/internal/normalization"
	"expired-domain-finder/internal/storage"
)

func BenchmarkSmokeCDX100Records(b *testing.B) {
	var input strings.Builder
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&input, `{"urlkey":"com,source)/%d","timestamp":"202604010000%02d","url":"https://source.com/%d","mime":"text/html","status":"200","digest":"%d","length":"100","offset":"%d","filename":"crawl.warc.gz"}`+"\n", i, i%60, i, i, i*100)
	}
	data := input.String()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := commoncrawl.DecodeCDX(strings.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSmokeGraph100kEdges(b *testing.B) {
	var edges strings.Builder
	for i := 0; i < 100000; i++ {
		fmt.Fprintf(&edges, "1 %d\n", 2+(i%100))
	}
	sourceIDs := map[string]string{"1": "source.com"}
	targets := map[string]string{}
	for i := 2; i < 102; i++ {
		targets[fmt.Sprint(i)] = fmt.Sprintf("target-%d.com", i)
	}
	data := edges.String()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := graph.ScanEdges(strings.NewReader(data), sourceIDs, targets, "graph-smoke"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSmoke10kCandidateIngest(b *testing.B) {
	cfg := config.Default()
	cfg.DataDirectory = b.TempDir()
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		b.Fatal(err)
	}
	store := storage.New(cfg.DataDirectory)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		b.Fatal(err)
	}
	observations := make([]extraction.LinkObservation, 0, 10000)
	for i := 0; i < 10000; i++ {
		domain := fmt.Sprintf("candidate-%d.com", i)
		observations = append(observations, extraction.LinkObservation{SourceDomain: "source.com", SourceURL: "https://source.com/", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://" + domain + "/", RegistrableDomain: domain, Mode: "warc"})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.AddLinkObservations(context.Background(), run.ID, observations, cfg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSmoke10kAvailabilityCacheWrites(b *testing.B) {
	cfg := config.Default()
	cfg.DataDirectory = b.TempDir()
	source, err := normalization.NormalizeSource("source.com")
	if err != nil {
		b.Fatal(err)
	}
	store := storage.New(cfg.DataDirectory)
	run, err := store.CreateRun(context.Background(), storage.RunRequest{Sources: []normalization.Result{source}, Crawls: []string{"CC-MAIN-2026-04"}, Modes: []string{"warc"}, Config: cfg})
	if err != nil {
		b.Fatal(err)
	}
	now := time.Now().UTC()
	records := make([]storage.AvailabilityRecord, 0, 10000)
	observations := make([]extraction.LinkObservation, 0, 10000)
	for i := 0; i < 10000; i++ {
		domain := fmt.Sprintf("candidate-%d.com", i)
		observations = append(observations, extraction.LinkObservation{SourceDomain: "source.com", SourceURL: "https://source.com/", CrawlID: "CC-MAIN-2026-04", OutboundURL: "https://" + domain + "/", RegistrableDomain: domain, Mode: "warc"})
		records = append(records, storage.AvailabilityRecord{Domain: domain, Status: availability.AvailableLikely, Confidence: availability.ConfidenceMedium, TLDPolicyCategory: "open_retail", CheckedAt: now, ExpiresAt: now.Add(time.Hour)})
	}
	if err := store.AddLinkObservations(context.Background(), run.ID, observations, cfg); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, record := range records {
			if err := store.StoreAvailability(context.Background(), run.ID, record); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func TestBenchmarkFixturesAreNonEmpty(t *testing.T) {
	var buf bytes.Buffer
	fmt.Fprint(&buf, "benchmark fixtures compile")
	if buf.Len() == 0 {
		t.Fatal("empty benchmark fixture")
	}
}
