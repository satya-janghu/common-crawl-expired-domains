package artifact

import (
	"os"
	"path/filepath"
	"testing"

	"expired-domain-finder/internal/extraction"
)

func TestWriteLinkObservationsParquetZSTD(t *testing.T) {
	path := filepath.Join(t.TempDir(), "links.parquet")
	manifest, err := WriteLinkObservations(path, []extraction.LinkObservation{{
		SourceDomain:      "source.com",
		SourceURL:         "https://source.com/",
		OutboundURL:       "https://target.com/",
		RegistrableDomain: "target.com",
		Mode:              "warc",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.RowCount != 1 || manifest.Format != "parquet" || manifest.Compression != "zstd" || manifest.SHA256 == "" {
		t.Fatalf("manifest = %+v", manifest)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 4 || string(data[:4]) != "PAR1" {
		t.Fatalf("not a parquet file: %q", data[:min(len(data), 4)])
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary file left behind: %v", err)
	}
	again, err := ManifestFile(path, "parquet", "zstd", 1)
	if err != nil {
		t.Fatal(err)
	}
	if again.SHA256 != manifest.SHA256 || again.ByteCount != manifest.ByteCount {
		t.Fatalf("manifest mismatch: %+v vs %+v", again, manifest)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
