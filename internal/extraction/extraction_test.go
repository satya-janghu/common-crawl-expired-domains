package extraction

import (
	"bytes"
	"compress/gzip"
	"strconv"
	"strings"
	"testing"
)

func TestExtractHTMLLinksResolvesAndNormalizes(t *testing.T) {
	html := `<html><body><a href="/local">Local</a><a rel="nofollow" href="https://blog.example.co.uk/post">Read more</a><a href="mailto:test@example.com">Mail</a></body></html>`
	got, err := ExtractHTMLLinks(strings.NewReader(html), HTMLContext{SourceDomain: "source.com", SourceURL: "https://source.com/page", CrawlID: "CC-MAIN-2026-04"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("links = %d, want 2: %+v", len(got), got)
	}
	if got[1].RegistrableDomain != "example.co.uk" {
		t.Fatalf("registrable = %q, want example.co.uk", got[1].RegistrableDomain)
	}
	if got[1].Rel != "nofollow" || got[1].AnchorText != "Read more" {
		t.Fatalf("evidence fields missing: %+v", got[1])
	}
}

func TestExtractWARCHTML(t *testing.T) {
	payload := "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><a href=\"https://target.com/\">Target</a></html>"
	warc := "WARC/1.0\r\nWARC-Type: response\r\nContent-Length: " + strconv.Itoa(len(payload)) + "\r\n\r\n" + payload
	got, err := ExtractWARCHTML([]byte(warc), HTMLContext{SourceDomain: "source.com", SourceURL: "https://source.com/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].RegistrableDomain != "target.com" {
		t.Fatalf("links = %+v", got)
	}
}

func TestExtractWARCHTMLRejectsTruncatedBody(t *testing.T) {
	payload := "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><a href=\"https://target.com/\">Target</a>"
	warc := "WARC/1.0\r\nWARC-Type: response\r\nContent-Length: " + strconv.Itoa(len(payload)+100) + "\r\n\r\n" + payload
	_, err := ExtractWARCHTML([]byte(warc), HTMLContext{SourceDomain: "source.com", SourceURL: "https://source.com/"})
	if err == nil || !strings.Contains(err.Error(), "truncated WARC body") {
		t.Fatalf("err = %v, want truncated WARC body", err)
	}
}

func TestExtractWATLinks(t *testing.T) {
	line := `{"Envelope":{"WARC-Header-Metadata":{"WARC-Target-URI":"https://source.com/page","WARC-Date":"2026-04-01T00:00:00Z"},"Payload-Metadata":{"HTTP-Response-Metadata":{"HTML-Metadata":{"Links":[{"path":"A@/href","url":"/relative","text":"Relative"},{"path":"A@/href","url":"https://target.co.uk/x","text":"Target","rel":"nofollow"}]}}}}}`
	got, err := ExtractWATLinks(strings.NewReader(line+"\n"), WATContext{SourceDomain: "source.com", CrawlID: "CC-MAIN-2026-04"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("links = %+v", got)
	}
	if got[1].RegistrableDomain != "target.co.uk" || got[1].Mode != "wat" {
		t.Fatalf("target observation = %+v", got[1])
	}
}

func TestExtractWATLinksSkipsMalformedJSONL(t *testing.T) {
	valid := `{"Envelope":{"WARC-Header-Metadata":{"WARC-Target-URI":"https://source.com/page","WARC-Date":"2026-04-01T00:00:00Z"},"Payload-Metadata":{"HTTP-Response-Metadata":{"HTML-Metadata":{"Links":[{"path":"A@/href","url":"https://target.com/","text":"Target"}]}}}}}`
	got, err := ExtractWATLinks(strings.NewReader("{broken\n"+valid+"\n"), WATContext{SourceDomain: "source.com", CrawlID: "CC-MAIN-2026-04"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].RegistrableDomain != "target.com" {
		t.Fatalf("links = %+v", got)
	}
}

func TestExtractWATLinksSkipsUnnormalizableFilteredSource(t *testing.T) {
	line := `{"Envelope":{"WARC-Header-Metadata":{"WARC-Target-URI":"http://www_bad_domain.invalid_host/","WARC-Date":"2026-04-01T00:00:00Z"},"Payload-Metadata":{"HTTP-Response-Metadata":{"HTML-Metadata":{"Links":[{"path":"A@/href","url":"https://target.com/","text":"Target"}]}}}}}`
	got, err := ExtractWATLinks(strings.NewReader(line+"\n"), WATContext{SourceDomain: "source.com", SourceDomains: map[string]bool{"source.com": true}, CrawlID: "CC-MAIN-2026-04"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("links = %+v, want none for unnormalizable filtered source URL", got)
	}
}

func TestExtractWATLinksReadsGzipSidecar(t *testing.T) {
	line := `{"Envelope":{"WARC-Header-Metadata":{"WARC-Target-URI":"https://source.com/page","WARC-Date":"2026-04-01T00:00:00Z"},"Payload-Metadata":{"HTTP-Response-Metadata":{"HTML-Metadata":{"Links":[{"path":"A@/href","url":"https://target.com/","text":"Target"}]}}}}}`
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write([]byte(line + "\n")); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := ExtractWATLinks(bytes.NewReader(buf.Bytes()), WATContext{SourceDomain: "source.com", CrawlID: "CC-MAIN-2026-04"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].RegistrableDomain != "target.com" {
		t.Fatalf("links = %+v", got)
	}
}
