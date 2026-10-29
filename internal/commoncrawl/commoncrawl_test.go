package commoncrawl

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDecodeCDX(t *testing.T) {
	records, err := DecodeCDX(strings.NewReader(`{"urlkey":"com,example)/","timestamp":"20260401000000","url":"https://example.com/","mime":"text/html","status":"200","digest":"abc","length":"123","offset":"456","filename":"crawl.warc.gz"}` + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Filename != "crawl.warc.gz" || records[0].RawJSON == "" {
		t.Fatalf("records = %+v", records)
	}
}

func TestDecodeCDXSkipsMalformedLines(t *testing.T) {
	input := "not-json\n" +
		`{"urlkey":"com,example)/","timestamp":"20260401000000","url":"https://example.com/","mime":"text/html","status":"200","digest":"abc","length":"123","offset":"456","filename":"crawl.warc.gz"}` + "\n" +
		`{"urlkey":` + "\n"
	records, err := DecodeCDX(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].URL != "https://example.com/" {
		t.Fatalf("records = %+v", records)
	}
}

func TestWATPath(t *testing.T) {
	got, ok := WATPath("crawl-data/CC-MAIN-2026-04/segments/x/warc/file.warc.gz")
	if !ok {
		t.Fatal("expected WAT mapping")
	}
	if !strings.Contains(got, "/wat/") || !strings.HasSuffix(got, ".warc.wat.gz") {
		t.Fatalf("wat path = %s", got)
	}
}

func TestFetchRangeUsesHTTPRange(t *testing.T) {
	client := Client{
		DataBaseURL: "https://data.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Header.Get("Range") != "bytes=5-9" {
				t.Fatalf("Range = %q, want bytes=5-9", req.Header.Get("Range"))
			}
			return &http.Response{StatusCode: http.StatusPartialContent, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader([]byte("abcde")))}, nil
		})},
	}
	got, err := client.FetchRange(context.Background(), "path/file.warc.gz", 5, 5)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "abcde" {
		t.Fatalf("body = %q", string(got))
	}
}

func TestFetchObject(t *testing.T) {
	client := Client{
		DataBaseURL: "https://data.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != "/path/file.warc.wat.gz" {
				t.Fatalf("path = %s", req.URL.Path)
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader([]byte("wat")))}, nil
		})},
	}
	body, err := client.FetchObject(context.Background(), "path/file.warc.wat.gz", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "wat" {
		t.Fatalf("body = %q", string(got))
	}
}

func TestFetchObjectErrorsWhenMaxBytesExceeded(t *testing.T) {
	client := Client{
		DataBaseURL: "https://data.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("abcdef"))}, nil
		})},
	}
	body, err := client.FetchObject(context.Background(), "path/file.txt", 3)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err == nil || !strings.Contains(err.Error(), "exceeded max bytes") {
		t.Fatalf("got body=%q err=%v, want max bytes error", string(got), err)
	}
}

func TestLoadCatalogAndResolveMonth(t *testing.T) {
	client := Client{
		IndexBaseURL: "https://index.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != "/collinfo.json" {
				t.Fatalf("path = %s", req.URL.Path)
			}
			body := `[{"id":"CC-MAIN-2026-18","from":"2026-04-01T00:00:00Z","to":"2026-04-30T23:59:59Z"}]`
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		})},
	}
	catalog, err := client.LoadCatalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := ResolveSelectionsFromCatalog([]string{"2026-04"}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if len(resolved) != 1 || resolved[0].ResolvedID != "CC-MAIN-2026-18" {
		t.Fatalf("resolved = %+v", resolved)
	}
}

func TestQueryCDXFromRawIndex(t *testing.T) {
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write([]byte(`com,adweek)/story 20240529065225 {"url": "https://www.adweek.com/story/", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "abc", "length": "123", "offset": "456", "filename": "crawl-data/CC-MAIN-2024-22/segments/x/warc/file.warc.gz"}` + "\n"))
	_, _ = zw.Write([]byte(`com,other)/story 20240529065225 {"url": "https://other.com/story/", "mime": "text/html", "status": "200", "length": "123", "offset": "456", "filename": "other.warc.gz"}` + "\n"))
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	var emptyGz bytes.Buffer
	emptyWriter := gzip.NewWriter(&emptyGz)
	if err := emptyWriter.Close(); err != nil {
		t.Fatal(err)
	}
	client := Client{
		DataBaseURL: "https://data.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/cc-index/collections/CC-MAIN-2024-22/indexes/cluster.idx":
				body := "com,aaa)/ 20240501000000\tcdx-00000.gz\t0\t" + strconv.Itoa(emptyGz.Len()) + "\t1\n" +
					"com,adweek)/story 20240529065225\tcdx-00027.gz\t0\t" + strconv.Itoa(gz.Len()) + "\t2\n" +
					"com,zzz)/ 20240501000000\tcdx-00099.gz\t0\t10\t3\n"
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			case "/cc-index/collections/CC-MAIN-2024-22/indexes/cdx-00027.gz":
				if req.Header.Get("Range") != fmt.Sprintf("bytes=0-%d", gz.Len()-1) {
					t.Fatalf("Range = %q", req.Header.Get("Range"))
				}
				return &http.Response{StatusCode: http.StatusPartialContent, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(gz.Bytes()))}, nil
			case "/cc-index/collections/CC-MAIN-2024-22/indexes/cdx-00000.gz":
				return &http.Response{StatusCode: http.StatusPartialContent, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(emptyGz.Bytes()))}, nil
			default:
				t.Fatalf("unexpected path %s", req.URL.Path)
				return nil, nil
			}
		})},
	}
	records, err := client.QueryCDXFromRawIndex(context.Background(), "CC-MAIN-2024-22", "adweek.com/*")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].URLKey != "com,adweek)/story" || records[0].URL != "https://www.adweek.com/story/" {
		t.Fatalf("records = %+v", records)
	}
}

func TestParseCDXPageCountSupportsCurrentJSONShape(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want int
	}{
		{name: "legacy number", raw: "2", want: 2},
		{name: "current object", raw: `{"pages": 3, "pageSize": 1000, "blocks": 6}`, want: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCDXPageCount(tc.raw)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("pages = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestQueryCDXTimeoutReturnsError(t *testing.T) {
	client := Client{
		IndexBaseURL: "https://index.test",
		MaxRetries:   0,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		})},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	_, err := client.QueryCDX(ctx, "CC-MAIN-2026-18", "example.com", 10)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestQueryCDXRetriesRetryableResponses(t *testing.T) {
	var calls int32
	client := Client{
		IndexBaseURL: "https://index.test",
		MaxRetries:   1,
		RetryBackoff: 0,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if atomic.AddInt32(&calls, 1) == 1 {
				header := make(http.Header)
				header.Set("Retry-After", "0")
				return &http.Response{StatusCode: http.StatusTooManyRequests, Header: header, Body: io.NopCloser(strings.NewReader("rate limited"))}, nil
			}
			if req.URL.Query().Get("showNumPages") == "true" {
				if req.URL.Query().Get("url") == "*.example.com/*" {
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("0"))}, nil
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("1"))}, nil
			}
			body := `{"urlkey":"com,example)/","timestamp":"20260401000000","url":"https://example.com/","mime":"text/html","status":"200","digest":"abc","length":"123","offset":"456","filename":"crawl.warc.gz"}` + "\n"
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		})},
	}
	records, err := client.QueryCDX(context.Background(), "CC-MAIN-2026-18", "example.com", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %+v", records)
	}
	if calls != 4 {
		t.Fatalf("calls = %d, want 4", calls)
	}
}

func TestIsTransientErrorClassifiesCDXNetworkFailures(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		errors.New(`Get "https://index.commoncrawl.org": dial tcp 1.2.3.4:443: connect: connection refused`),
		errors.New("net/http: timeout awaiting response headers"),
		errors.New("common crawl retryable http 429"),
		errors.New("common crawl retryable http 503"),
	} {
		if !IsTransientError(err) {
			t.Fatalf("err %q was not classified transient", err)
		}
	}
	if IsTransientError(errors.New("common crawl cdx http 403")) {
		t.Fatal("403 should not be classified transient")
	}
}

func TestRetryDelayHonorsMaxBackoff(t *testing.T) {
	if got := retryDelay("", time.Second, 1500*time.Millisecond, 3); got != 1500*time.Millisecond {
		t.Fatalf("delay = %s, want 1.5s", got)
	}
	if got := retryDelay("10", time.Second, 2*time.Second, 0); got != 2*time.Second {
		t.Fatalf("retry-after delay = %s, want 2s cap", got)
	}
}

func TestQueryCDXQueriesApexAndWildcardPatterns(t *testing.T) {
	var patterns []string
	client := Client{
		IndexBaseURL: "https://index.test",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			pattern := req.URL.Query().Get("url")
			if req.URL.Query().Get("showNumPages") == "true" {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("1"))}, nil
			}
			patterns = append(patterns, pattern)
			body := `{"urlkey":"com,example)/","timestamp":"20260401000000","url":"https://example.com/","mime":"text/html","status":"200","digest":"abc","length":"123","offset":"456","filename":"crawl.warc.gz"}` + "\n"
			if len(patterns) == 2 {
				body = `{"urlkey":"com,example,www)/","timestamp":"20260401000001","url":"https://www.example.com/","mime":"text/html","status":"200","digest":"def","length":"124","offset":"457","filename":"crawl.warc.gz"}` + "\n"
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		})},
	}
	records, err := client.QueryCDX(context.Background(), "CC-MAIN-2026-18", "example.com", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("records = %+v", records)
	}
	if strings.Join(patterns, ",") != "example.com/*,*.example.com/*" {
		t.Fatalf("patterns = %+v", patterns)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
