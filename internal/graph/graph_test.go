package graph

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestScanGraphFixtures(t *testing.T) {
	sourceIDs, err := ScanVertices(strings.NewReader("1 source.com\n2 target.net\n"), map[string]bool{"source.com": true})
	if err != nil {
		t.Fatal(err)
	}
	if sourceIDs["1"] != "source.com" {
		t.Fatalf("source IDs = %+v", sourceIDs)
	}
	observations, err := ScanEdges(strings.NewReader("1 2\n1 2\n2 1\n"), sourceIDs, map[string]string{"2": "target.net"}, "graph-2026-04")
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 1 || observations[0].EdgeCount != 2 {
		t.Fatalf("observations = %+v", observations)
	}
	sourceEdges, err := ScanSourceEdges(strings.NewReader("1 2\n1 2\n2 1\n"), sourceIDs)
	if err != nil {
		t.Fatal(err)
	}
	targets := TargetIDs(sourceEdges)
	resolved, err := ResolveVertices(strings.NewReader("1 source.com\n2 target.net\n3 unrelated.org\n"), targets)
	if err != nil {
		t.Fatal(err)
	}
	filtered := ObservationsFromSourceEdges(sourceEdges, resolved, "graph-2026-04")
	if len(filtered) != 1 || filtered[0].TargetDomain != "target.net" || filtered[0].EdgeCount != 2 {
		t.Fatalf("filtered observations = %+v", filtered)
	}
}

func TestScanGraphSkipsMalformedRecords(t *testing.T) {
	sourceIDs, err := ScanVertices(strings.NewReader("badline\n1 source.com\n2 not a domain\n3 invalid_domain\n"), map[string]bool{"source.com": true})
	if err != nil {
		t.Fatal(err)
	}
	if len(sourceIDs) != 1 || sourceIDs["1"] != "source.com" {
		t.Fatalf("source IDs = %+v", sourceIDs)
	}
	sourceEdges, err := ScanSourceEdges(strings.NewReader("badline\n1\n1 2\n1 2 extra\n"), sourceIDs)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := ResolveVertices(strings.NewReader("2 target.com\nnot-enough\n"), TargetIDs(sourceEdges))
	if err != nil {
		t.Fatal(err)
	}
	observations := ObservationsFromSourceEdges(sourceEdges, resolved, "graph-test")
	if len(observations) != 1 || observations[0].TargetDomain != "target.com" || observations[0].EdgeCount != 2 {
		t.Fatalf("observations = %+v", observations)
	}
}

func TestScanGraphCompressedFixtures(t *testing.T) {
	var gzbuf bytes.Buffer
	gz := gzip.NewWriter(&gzbuf)
	if _, err := gz.Write([]byte("1 source.com\n")); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	sourceIDs, err := ScanVertices(bytes.NewReader(gzbuf.Bytes()), map[string]bool{"source.com": true})
	if err != nil {
		t.Fatal(err)
	}
	if sourceIDs["1"] != "source.com" {
		t.Fatalf("source IDs = %+v", sourceIDs)
	}

	var zbuf bytes.Buffer
	zw, err := zstd.NewWriter(&zbuf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := zw.Write([]byte("1 2\n")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	observations, err := ScanEdges(bytes.NewReader(zbuf.Bytes()), sourceIDs, map[string]string{"2": "target.net"}, "graph-2026-04")
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 1 || observations[0].TargetDomain != "target.net" {
		t.Fatalf("observations = %+v", observations)
	}
}

func TestResolveReleaseSkipsHostOnlyOrMissingDomainPaths(t *testing.T) {
	releases := []Release{
		{ID: "host-only", Crawls: []string{"CC-MAIN-2026-17"}, VertexPath: "host-vertices", EdgePath: "host-edges"},
		{ID: "missing-edge", Crawls: []string{"CC-MAIN-2026-17"}, DomainNodes: 10, VertexPath: "domain-vertices"},
		{ID: "domain", Crawls: []string{"CC-MAIN-2026-17"}, DomainNodes: 10, VertexPath: "domain-vertices", EdgePath: "domain-edges"},
	}
	got, ok := ResolveRelease([]string{"CC-MAIN-2026-17"}, releases)
	if !ok {
		t.Fatal("expected domain release")
	}
	if got.ID != "domain" {
		t.Fatalf("release = %+v, want domain release", got)
	}
}
