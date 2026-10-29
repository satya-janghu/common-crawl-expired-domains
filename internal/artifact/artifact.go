package artifact

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"expired-domain-finder/internal/extraction"
	"expired-domain-finder/internal/graph"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

type Manifest struct {
	Path        string
	Format      string
	Compression string
	RowCount    int
	ByteCount   int64
	SHA256      string
}

type linkObservationRow struct {
	SourceDomain      string `parquet:"source_domain"`
	SourceURL         string `parquet:"source_url"`
	CrawlID           string `parquet:"crawl_id"`
	Timestamp         string `parquet:"timestamp"`
	WARCFile          string `parquet:"warc_file"`
	WARCOffset        int64  `parquet:"warc_offset"`
	WARCLength        int64  `parquet:"warc_length"`
	OutboundURL       string `parquet:"outbound_url"`
	AnchorText        string `parquet:"anchor_text"`
	Rel               string `parquet:"rel"`
	Snippet           string `parquet:"snippet"`
	RegistrableDomain string `parquet:"registrable_domain"`
	Mode              string `parquet:"mode"`
}

type graphObservationRow struct {
	SourceDomain string `parquet:"source_domain"`
	TargetDomain string `parquet:"target_domain"`
	ReleaseID    string `parquet:"release_id"`
	EdgeCount    int    `parquet:"edge_count"`
}

func WriteLinkObservations(path string, observations []extraction.LinkObservation) (Manifest, error) {
	rows := make([]linkObservationRow, 0, len(observations))
	for _, obs := range observations {
		rows = append(rows, linkObservationRow{
			SourceDomain:      obs.SourceDomain,
			SourceURL:         obs.SourceURL,
			CrawlID:           obs.CrawlID,
			Timestamp:         obs.Timestamp,
			WARCFile:          obs.WARCFile,
			WARCOffset:        obs.WARCOffset,
			WARCLength:        obs.WARCLength,
			OutboundURL:       obs.OutboundURL,
			AnchorText:        obs.AnchorText,
			Rel:               obs.Rel,
			Snippet:           obs.Snippet,
			RegistrableDomain: obs.RegistrableDomain,
			Mode:              obs.Mode,
		})
	}
	return writeParquet(path, rows, len(rows))
}

func WriteGraphObservations(path string, observations []graph.CandidateObservation) (Manifest, error) {
	rows := make([]graphObservationRow, 0, len(observations))
	for _, obs := range observations {
		rows = append(rows, graphObservationRow(obs))
	}
	return writeParquet(path, rows, len(rows))
}

func ManifestFile(path, format, compression string, rowCount int) (Manifest, error) {
	hash, size, err := hashFile(path)
	if err != nil {
		return Manifest{}, err
	}
	return Manifest{Path: path, Format: format, Compression: compression, RowCount: rowCount, ByteCount: size, SHA256: hash}, nil
}

func writeParquet[T any](path string, rows []T, rowCount int) (Manifest, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return Manifest{}, err
	}
	tmp := path + ".tmp"
	if err := parquet.WriteFile(tmp, rows, parquet.Compression(&zstd.Codec{})); err != nil {
		return Manifest{}, err
	}
	hash, size, err := hashFile(tmp)
	if err != nil {
		_ = os.Remove(tmp)
		return Manifest{}, err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return Manifest{}, err
	}
	return Manifest{Path: path, Format: "parquet", Compression: "zstd", RowCount: rowCount, ByteCount: size, SHA256: hash}, nil
}

func hashFile(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	h := sha256.New()
	size, err := io.Copy(h, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}
