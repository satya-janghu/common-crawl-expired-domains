package extraction

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/url"
	"strings"

	"expired-domain-finder/internal/normalization"
)

type WATContext struct {
	SourceDomain  string
	SourceDomains map[string]bool
	CrawlID       string
}

type WATScanOptions struct {
	MaxLineBytes  int
	MaxTotalBytes int64
	BatchRows     int
}

func ExtractWATLinks(r io.Reader, ctx WATContext) ([]LinkObservation, error) {
	var out []LinkObservation
	err := ScanWATBatches(r, ctx, WATScanOptions{}, func(_ int, observations []LinkObservation) error {
		out = append(out, observations...)
		return nil
	})
	return out, err
}

func ScanWATBatches(r io.Reader, ctx WATContext, opts WATScanOptions, emit func(batchIndex int, observations []LinkObservation) error) error {
	input, closeInput, err := maybeGzipReader(r)
	if err != nil {
		return err
	}
	if closeInput != nil {
		defer closeInput()
	}
	if opts.MaxLineBytes <= 0 {
		opts.MaxLineBytes = 10 * 1024 * 1024
	}
	if opts.BatchRows <= 0 {
		opts.BatchRows = 1000
	}
	if opts.MaxTotalBytes > 0 {
		input = io.LimitReader(input, opts.MaxTotalBytes+1)
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 0, 64*1024), opts.MaxLineBytes)
	var batch []LinkObservation
	batchIndex := 0
	var scanned int64
	for scanner.Scan() {
		scanned += int64(len(scanner.Bytes())) + 1
		if opts.MaxTotalBytes > 0 && scanned > opts.MaxTotalBytes {
			return io.ErrUnexpectedEOF
		}
		observations, err := extractWATLine(scanner.Bytes(), ctx)
		if err != nil {
			continue
		}
		batch = append(batch, observations...)
		if len(batch) >= opts.BatchRows {
			if err := emit(batchIndex, batch); err != nil {
				return err
			}
			batchIndex++
			batch = nil
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(batch) > 0 {
		if err := emit(batchIndex, batch); err != nil {
			return err
		}
	}
	return nil
}

func maybeGzipReader(r io.Reader) (io.Reader, func() error, error) {
	br := bufio.NewReader(r)
	header, err := br.Peek(2)
	if err != nil && err != io.EOF {
		return nil, nil, err
	}
	if len(header) == 2 && header[0] == 0x1f && header[1] == 0x8b {
		gz, err := gzip.NewReader(br)
		if err != nil {
			return nil, nil, err
		}
		return gz, gz.Close, nil
	}
	return br, nil, nil
}

func extractWATLine(line []byte, ctx WATContext) ([]LinkObservation, error) {
	var payload struct {
		Envelope struct {
			WARCHeaderMetadata struct {
				WARCTargetURI string `json:"WARC-Target-URI"`
				WARCDate      string `json:"WARC-Date"`
				WARCRecordID  string `json:"WARC-Record-ID"`
			} `json:"WARC-Header-Metadata"`
			PayloadMetadata struct {
				HTTPResponseMetadata struct {
					HTMLMetadata struct {
						Links []struct {
							Path string `json:"path"`
							URL  string `json:"url"`
							Text string `json:"text"`
							Rel  string `json:"rel"`
						} `json:"Links"`
					} `json:"HTML-Metadata"`
				} `json:"HTTP-Response-Metadata"`
			} `json:"Payload-Metadata"`
		} `json:"Envelope"`
	}
	if err := json.Unmarshal(line, &payload); err != nil {
		return nil, err
	}
	sourceURL := payload.Envelope.WARCHeaderMetadata.WARCTargetURI
	if sourceURL == "" {
		return nil, nil
	}
	sourceDomain := ctx.SourceDomain
	normalizedSource, err := normalization.NormalizeDomain(sourceURL, nil, nil)
	if err == nil {
		sourceDomain = normalizedSource.RegistrableDomain
	}
	if len(ctx.SourceDomains) > 0 {
		if err != nil || !ctx.SourceDomains[sourceDomain] {
			return nil, nil
		}
	}
	base, err := url.Parse(sourceURL)
	if err != nil {
		return nil, nil
	}
	var out []LinkObservation
	for _, link := range payload.Envelope.PayloadMetadata.HTTPResponseMetadata.HTMLMetadata.Links {
		if strings.ToLower(link.Path) != "a@/href" && link.Path != "" {
			continue
		}
		href := strings.TrimSpace(link.URL)
		if href == "" {
			continue
		}
		parsed, err := url.Parse(href)
		if err != nil {
			continue
		}
		resolved := base.ResolveReference(parsed)
		if resolved.Scheme != "http" && resolved.Scheme != "https" {
			continue
		}
		normalized, err := normalization.NormalizeDomain(resolved.String(), nil, nil)
		if err != nil {
			continue
		}
		out = append(out, LinkObservation{
			SourceDomain:      sourceDomain,
			SourceURL:         sourceURL,
			CrawlID:           ctx.CrawlID,
			Timestamp:         payload.Envelope.WARCHeaderMetadata.WARCDate,
			OutboundURL:       resolved.String(),
			AnchorText:        strings.TrimSpace(link.Text),
			Rel:               link.Rel,
			RegistrableDomain: normalized.RegistrableDomain,
			Mode:              "wat",
		})
	}
	return out, nil
}
