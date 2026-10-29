package commoncrawl

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type CrawlSelection struct {
	Input      string
	ResolvedID string
}

type CatalogEntry struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Timegate string `json:"timegate"`
	API      string `json:"cdx-api"`
	From     string `json:"from"`
	To       string `json:"to"`
}

type CDXRecord struct {
	URLKey       string `json:"urlkey"`
	Timestamp    string `json:"timestamp"`
	URL          string `json:"url"`
	Mime         string `json:"mime"`
	MimeDetected string `json:"mime-detected"`
	Status       string `json:"status"`
	Digest       string `json:"digest"`
	Length       string `json:"length"`
	Offset       string `json:"offset"`
	Filename     string `json:"filename"`
	RawJSON      string `json:"-"`
}

type rawCDXBlock struct {
	Key    string
	File   string
	Offset int64
	Length int64
}

type Client struct {
	IndexBaseURL string
	DataBaseURL  string
	HTTPClient   *http.Client
	UserAgent    string
	MaxRetries   int
	RetryBackoff time.Duration
	MaxBackoff   time.Duration
}

type readCloser struct {
	io.Reader
	close func() error
}

func (r readCloser) Close() error {
	return r.close()
}

type maxBytesReader struct {
	r        io.Reader
	remain   int64
	limit    int64
	exceeded bool
}

func (r *maxBytesReader) Read(p []byte) (int, error) {
	if r.exceeded {
		return 0, fmt.Errorf("common crawl object exceeded max bytes %d", r.limit)
	}
	if r.remain <= 0 {
		var one [1]byte
		n, err := r.r.Read(one[:])
		if n > 0 {
			r.exceeded = true
			return 0, fmt.Errorf("common crawl object exceeded max bytes %d", r.limit)
		}
		return 0, err
	}
	if int64(len(p)) > r.remain {
		p = p[:r.remain]
	}
	n, err := r.r.Read(p)
	r.remain -= int64(n)
	return n, err
}

func NewClient(indexBaseURL, userAgent string) Client {
	return Client{
		IndexBaseURL: strings.TrimRight(indexBaseURL, "/"),
		DataBaseURL:  "https://data.commoncrawl.org",
		UserAgent:    userAgent,
		HTTPClient:   &http.Client{},
		MaxRetries:   7,
		RetryBackoff: 250 * time.Millisecond,
		MaxBackoff:   5 * time.Second,
	}
}

func ResolveSelections(inputs []string) []CrawlSelection {
	out := make([]CrawlSelection, 0, len(inputs))
	for _, input := range inputs {
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}
		out = append(out, CrawlSelection{Input: input, ResolvedID: input})
	}
	return out
}

func (c Client) LoadCatalog(ctx context.Context) ([]CatalogEntry, error) {
	base := strings.TrimRight(c.IndexBaseURL, "/")
	if base == "" {
		base = "https://index.commoncrawl.org"
	}
	resp, err := c.get(ctx, base+"/collinfo.json", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("common crawl catalog http %d", resp.StatusCode)
	}
	var entries []CatalogEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func ResolveSelectionsFromCatalog(inputs []string, catalog []CatalogEntry) ([]CrawlSelection, error) {
	var out []CrawlSelection
	for _, input := range inputs {
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}
		if strings.HasPrefix(input, "CC-MAIN-") {
			out = append(out, CrawlSelection{Input: input, ResolvedID: input})
			continue
		}
		matched := false
		for _, entry := range catalog {
			if entry.ID == input {
				out = append(out, CrawlSelection{Input: input, ResolvedID: entry.ID})
				matched = true
				break
			}
			if len(input) == 7 && strings.HasPrefix(entry.From, input) {
				out = append(out, CrawlSelection{Input: input, ResolvedID: entry.ID})
				matched = true
			}
		}
		if !matched {
			return nil, fmt.Errorf("crawl selection %q not found in catalog", input)
		}
	}
	return out, nil
}

func ResolveClosestOlderOrExactMonth(month string, catalog []CatalogEntry) (CrawlSelection, error) {
	month = strings.TrimSpace(month)
	selected, err := time.Parse("2006-01", month)
	if err != nil {
		return CrawlSelection{}, fmt.Errorf("selected crawl month must be YYYY-MM: %w", err)
	}
	cutoff := selected.AddDate(0, 1, 0)
	var best CatalogEntry
	var bestFrom time.Time
	for _, entry := range catalog {
		if strings.TrimSpace(entry.ID) == "" || strings.TrimSpace(entry.From) == "" {
			continue
		}
		from, err := parseCatalogDate(entry.From)
		if err != nil {
			continue
		}
		if !from.Before(cutoff) {
			continue
		}
		if best.ID == "" || from.After(bestFrom) {
			best = entry
			bestFrom = from
		}
	}
	if best.ID == "" {
		return CrawlSelection{}, fmt.Errorf("no Common Crawl catalog entry exists on or before %s", month)
	}
	return CrawlSelection{Input: month, ResolvedID: best.ID}, nil
}

func FallbackCatalog() []CatalogEntry {
	return []CatalogEntry{
		{ID: "CC-MAIN-2026-17", From: "2026-04-10T08:11:53", To: "2026-04-23T02:04:17"},
		{ID: "CC-MAIN-2025-51", From: "2025-12-01T00:00:00", To: "2025-12-31T23:59:59"},
		{ID: "CC-MAIN-2025-47", From: "2025-11-01T00:00:00", To: "2025-11-30T23:59:59"},
		{ID: "CC-MAIN-2025-43", From: "2025-10-01T00:00:00", To: "2025-10-31T23:59:59"},
		{ID: "CC-MAIN-2025-39", From: "2025-09-01T00:00:00", To: "2025-09-30T23:59:59"},
		{ID: "CC-MAIN-2025-33", From: "2025-08-01T00:00:00", To: "2025-08-31T23:59:59"},
		{ID: "CC-MAIN-2025-30", From: "2025-07-01T00:00:00", To: "2025-07-31T23:59:59"},
		{ID: "CC-MAIN-2025-26", From: "2025-06-01T00:00:00", To: "2025-06-30T23:59:59"},
		{ID: "CC-MAIN-2025-21", From: "2025-05-01T00:00:00", To: "2025-05-31T23:59:59"},
		{ID: "CC-MAIN-2025-18", From: "2025-04-01T00:00:00", To: "2025-04-30T23:59:59"},
		{ID: "CC-MAIN-2025-13", From: "2025-03-01T00:00:00", To: "2025-03-31T23:59:59"},
		{ID: "CC-MAIN-2025-08", From: "2025-02-01T00:00:00", To: "2025-02-28T23:59:59"},
		{ID: "CC-MAIN-2025-05", From: "2025-01-01T00:00:00", To: "2025-01-31T23:59:59"},
		{ID: "CC-MAIN-2024-51", From: "2024-12-01T16:20:23", To: "2024-12-15T00:22:57"},
		{ID: "CC-MAIN-2024-46", From: "2024-11-01T18:42:24", To: "2024-11-15T05:19:00"},
		{ID: "CC-MAIN-2024-42", From: "2024-10-03T09:40:20", To: "2024-10-16T09:12:31"},
		{ID: "CC-MAIN-2024-38", From: "2024-09-07T09:58:56", To: "2024-09-21T04:50:54"},
		{ID: "CC-MAIN-2024-33", From: "2024-08-02T23:45:08", To: "2024-08-16T06:08:12"},
		{ID: "CC-MAIN-2024-30", From: "2024-07-12T09:42:14", To: "2024-07-25T20:55:45"},
		{ID: "CC-MAIN-2024-26", From: "2024-06-12T14:04:24", To: "2024-06-25T23:28:02"},
		{ID: "CC-MAIN-2024-22", From: "2024-05-17T23:31:22", To: "2024-05-31T00:06:14"},
		{ID: "CC-MAIN-2024-18", From: "2024-04-12T10:13:54", To: "2024-04-25T16:02:16"},
		{ID: "CC-MAIN-2024-10", From: "2024-02-20T21:10:55", To: "2024-03-05T15:40:45"},
	}
}

func parseCatalogDate(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	for _, layout := range []string{"2006-01-02", "2006-01-02T15:04:05", "2006-01-02T15:04:05Z07:00", time.RFC3339} {
		if t, err := time.Parse(layout, value); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported Common Crawl catalog date %q", value)
}

func (c Client) QueryCDX(ctx context.Context, crawlID, domain string, limit int) ([]CDXRecord, error) {
	pageSize := 1000
	patterns := []string{domain + "/*", "*." + domain + "/*"}
	seen := map[string]bool{}
	out := make([]CDXRecord, 0)
	for _, pattern := range patterns {
		pageCount, err := c.CDXPageCount(ctx, crawlID, pattern, pageSize)
		if err != nil {
			return nil, err
		}
		for page := 0; page < pageCount; page++ {
			records, err := c.QueryCDXPage(ctx, crawlID, pattern, page, pageSize)
			if err != nil {
				return nil, err
			}
			if page < pageCount-1 && len(records) == 0 {
				return nil, fmt.Errorf("common crawl cdx page inconsistency for %s %s page %d: empty page before final page", crawlID, pattern, page)
			}
			for _, record := range records {
				if !ValidCDXRecord(record) {
					continue
				}
				key := record.URLKey + "\x00" + record.Timestamp + "\x00" + record.Filename + "\x00" + record.Offset + "\x00" + record.Length
				if seen[key] {
					continue
				}
				seen[key] = true
				out = append(out, record)
				if limit > 0 && len(out) > limit {
					return nil, fmt.Errorf("common crawl cdx discovery exceeded --cdx-limit=%d; increase the budget or narrow sources/crawls", limit)
				}
			}
		}
	}
	return out, nil
}

func (c Client) CDXPageCount(ctx context.Context, crawlID, pattern string, pageSize int) (int, error) {
	if pageSize <= 0 {
		pageSize = 1000
	}
	base := c.IndexBaseURL + "/" + url.PathEscape(crawlID) + "-index"
	values := url.Values{}
	values.Set("url", pattern)
	values.Set("showNumPages", "true")
	values.Add("filter", "status:200")
	values.Add("filter", "mime:text/html")
	values.Set("pageSize", fmt.Sprint(pageSize))
	resp, err := c.get(ctx, base+"?"+values.Encode(), nil)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("common crawl cdx http %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return 0, err
	}
	raw := strings.TrimSpace(string(body))
	if raw == "" {
		return 0, nil
	}
	pages, err := parseCDXPageCount(raw)
	if err != nil {
		return 0, err
	}
	return pages, nil
}

func parseCDXPageCount(raw string) (int, error) {
	pages, err := strconv.Atoi(raw)
	if err == nil && pages >= 0 {
		return pages, nil
	}
	var payload struct {
		Pages int `json:"pages"`
	}
	if json.Unmarshal([]byte(raw), &payload) == nil && payload.Pages >= 0 {
		return payload.Pages, nil
	}
	return 0, fmt.Errorf("common crawl cdx invalid page count %q", raw)
}

func (c Client) QueryCDXPage(ctx context.Context, crawlID, pattern string, page, pageSize int) ([]CDXRecord, error) {
	if page < 0 {
		return nil, fmt.Errorf("cdx page must be non-negative")
	}
	if pageSize <= 0 {
		pageSize = 1000
	}
	base := c.IndexBaseURL + "/" + url.PathEscape(crawlID) + "-index"
	values := url.Values{}
	values.Set("url", pattern)
	values.Set("output", "json")
	values.Add("filter", "status:200")
	values.Add("filter", "mime:text/html")
	values.Set("fl", "urlkey,timestamp,url,mime,mime-detected,status,digest,length,offset,filename")
	values.Set("page", fmt.Sprint(page))
	values.Set("pageSize", fmt.Sprint(pageSize))
	resp, err := c.get(ctx, base+"?"+values.Encode(), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("common crawl cdx http %d", resp.StatusCode)
	}
	return DecodeCDX(resp.Body)
}

func (c Client) QueryCDXFromRawIndex(ctx context.Context, crawlID, pattern string) ([]CDXRecord, error) {
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		var records []CDXRecord
		records, err = c.queryCDXFromRawIndexOnce(ctx, crawlID, pattern)
		if err == nil || !rawIndexRetryable(err) {
			return records, err
		}
		if sleepErr := sleepContext(ctx, time.Duration(attempt+1)*2*time.Second); sleepErr != nil {
			return nil, sleepErr
		}
	}
	return nil, err
}

func (c Client) queryCDXFromRawIndexOnce(ctx context.Context, crawlID, pattern string) ([]CDXRecord, error) {
	prefix, err := rawCDXPrefix(pattern)
	if err != nil {
		return nil, err
	}
	blocks, err := c.rawCDXBlocks(ctx, crawlID, prefix)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []CDXRecord
	for _, block := range blocks {
		records, err := c.fetchRawCDXBlock(ctx, crawlID, block)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			if !strings.HasPrefix(record.URLKey, prefix) || !ValidCDXRecord(record) {
				continue
			}
			key := record.URLKey + "\x00" + record.Timestamp + "\x00" + record.Filename + "\x00" + record.Offset + "\x00" + record.Length
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, record)
		}
	}
	return out, nil
}

func rawIndexRetryable(err error) bool {
	if IsTransientError(err) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "raw cdx cluster http 403") ||
		strings.Contains(strings.ToLower(err.Error()), "common crawl range http 403")
}

func (c Client) rawCDXBlocks(ctx context.Context, crawlID, prefix string) ([]rawCDXBlock, error) {
	base := strings.TrimRight(c.DataBaseURL, "/")
	if base == "" {
		base = "https://data.commoncrawl.org"
	}
	path := fmt.Sprintf("%s/cc-index/collections/%s/indexes/cluster.idx", base, url.PathEscape(crawlID))
	resp, err := c.get(ctx, path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("common crawl raw cdx cluster http %d", resp.StatusCode)
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	var blocks []rawCDXBlock
	var previous rawCDXBlock
	inRange := false
	for scanner.Scan() {
		block, ok := parseRawCDXBlock(scanner.Text())
		if !ok {
			continue
		}
		if strings.HasPrefix(block.Key, prefix) {
			if !inRange && previous.File != "" {
				blocks = append(blocks, previous)
			}
			blocks = append(blocks, block)
			inRange = true
		} else if inRange {
			break
		}
		previous = block
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return dedupeRawCDXBlocks(blocks), nil
}

func parseRawCDXBlock(line string) (rawCDXBlock, bool) {
	fields := strings.Split(line, "\t")
	if len(fields) < 4 {
		return rawCDXBlock{}, false
	}
	key := strings.Fields(fields[0])
	if len(key) == 0 {
		return rawCDXBlock{}, false
	}
	offset, err := strconv.ParseInt(strings.TrimSpace(fields[2]), 10, 64)
	if err != nil || offset < 0 {
		return rawCDXBlock{}, false
	}
	length, err := strconv.ParseInt(strings.TrimSpace(fields[3]), 10, 64)
	if err != nil || length <= 0 {
		return rawCDXBlock{}, false
	}
	return rawCDXBlock{Key: key[0], File: strings.TrimSpace(fields[1]), Offset: offset, Length: length}, true
}

func dedupeRawCDXBlocks(blocks []rawCDXBlock) []rawCDXBlock {
	seen := map[string]bool{}
	out := make([]rawCDXBlock, 0, len(blocks))
	for _, block := range blocks {
		key := fmt.Sprintf("%s:%d:%d", block.File, block.Offset, block.Length)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, block)
	}
	return out
}

func (c Client) fetchRawCDXBlock(ctx context.Context, crawlID string, block rawCDXBlock) ([]CDXRecord, error) {
	data, err := c.FetchRange(ctx, fmt.Sprintf("cc-index/collections/%s/indexes/%s", crawlID, block.File), block.Offset, block.Length)
	if err != nil {
		return nil, err
	}
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return DecodeRawCDX(reader)
}

func rawCDXPrefix(pattern string) (string, error) {
	pattern = strings.TrimSpace(pattern)
	wildcard := strings.HasPrefix(pattern, "*.")
	domain := strings.TrimPrefix(pattern, "*.")
	domain = strings.TrimSuffix(domain, "/*")
	domain = strings.TrimSuffix(domain, "/")
	if domain == "" || strings.Contains(domain, "*") {
		return "", fmt.Errorf("unsupported raw cdx pattern %q", pattern)
	}
	labels := strings.Split(domain, ".")
	for i, j := 0, len(labels)-1; i < j; i, j = i+1, j-1 {
		labels[i], labels[j] = labels[j], labels[i]
	}
	if wildcard {
		return strings.Join(labels, ",") + ",", nil
	}
	return strings.Join(labels, ",") + ")/", nil
}

func CDXPatternPrefix(pattern string) (string, error) {
	return rawCDXPrefix(pattern)
}

func (c Client) FetchRange(ctx context.Context, filename string, offset, length int64) ([]byte, error) {
	if filename == "" {
		return nil, fmt.Errorf("filename is required")
	}
	if offset < 0 || length <= 0 {
		return nil, fmt.Errorf("invalid range offset=%d length=%d", offset, length)
	}
	base := strings.TrimRight(c.DataBaseURL, "/")
	if base == "" {
		base = "https://data.commoncrawl.org"
	}
	resp, err := c.get(ctx, base+"/"+strings.TrimLeft(filename, "/"), map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("common crawl range http %d", resp.StatusCode)
	}
	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, resp.Body, length); err != nil {
		return nil, err
	}
	var extra [1]byte
	n, err := resp.Body.Read(extra[:])
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n > 0 {
		return nil, fmt.Errorf("common crawl range exceeded expected length %d", length)
	}
	return buf.Bytes(), nil
}

func (c Client) FetchObject(ctx context.Context, filename string, maxBytes int64) (io.ReadCloser, error) {
	if filename == "" {
		return nil, fmt.Errorf("filename is required")
	}
	base := strings.TrimRight(c.DataBaseURL, "/")
	if base == "" {
		base = "https://data.commoncrawl.org"
	}
	resp, err := c.get(ctx, base+"/"+strings.TrimLeft(filename, "/"), nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("common crawl object http %d", resp.StatusCode)
	}
	if maxBytes > 0 {
		return readCloser{Reader: &maxBytesReader{r: resp.Body, remain: maxBytes, limit: maxBytes}, close: resp.Body.Close}, nil
	}
	return resp.Body, nil
}

func DecodeCDX(r io.Reader) ([]CDXRecord, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	var out []CDXRecord
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record CDXRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		record.RawJSON = line
		out = append(out, record)
	}
	return out, scanner.Err()
}

func DecodeRawCDX(r io.Reader) ([]CDXRecord, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	var out []CDXRecord
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		first := strings.IndexByte(line, ' ')
		if first < 0 {
			continue
		}
		second := strings.IndexByte(line[first+1:], ' ')
		if second < 0 {
			continue
		}
		second += first + 1
		var record CDXRecord
		if err := json.Unmarshal([]byte(line[second+1:]), &record); err != nil {
			continue
		}
		record.URLKey = strings.TrimSpace(line[:first])
		record.Timestamp = strings.TrimSpace(line[first+1 : second])
		record.RawJSON = line
		out = append(out, record)
	}
	return out, scanner.Err()
}

func ValidCDXRecord(record CDXRecord) bool {
	if strings.TrimSpace(record.Filename) == "" {
		return false
	}
	if parsePositiveInt64(record.Offset) < 0 || parsePositiveInt64(record.Length) <= 0 {
		return false
	}
	if strings.TrimSpace(record.Status) != "200" {
		return false
	}
	mime := strings.ToLower(strings.TrimSpace(record.Mime))
	if mime == "" {
		mime = strings.ToLower(strings.TrimSpace(record.MimeDetected))
	}
	if mime == "" || !strings.Contains(mime, "html") {
		return false
	}
	return true
}

func parsePositiveInt64(raw string) int64 {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return -1
	}
	return value
}

func WATPath(warcPath string) (string, bool) {
	if warcPath == "" {
		return "", false
	}
	wat := strings.Replace(warcPath, "/warc/", "/wat/", 1)
	wat = strings.Replace(wat, "warc.gz", "warc.wat.gz", 1)
	return wat, wat != warcPath
}

func retryableHTTPError(resp *http.Response) error {
	retryAfter := resp.Header.Get("Retry-After")
	if retryAfter != "" {
		return fmt.Errorf("common crawl retryable http %d retry-after=%s", resp.StatusCode, retryAfter)
	}
	return fmt.Errorf("common crawl retryable http %d", resp.StatusCode)
}

func IsTransientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	text := strings.ToLower(err.Error())
	for _, marker := range []string{
		"client.timeout",
		"context deadline exceeded",
		"connection refused",
		"connection reset",
		"connection timed out",
		"temporary failure",
		"tls handshake timeout",
		"timeout awaiting response headers",
		"retryable http 429",
		"retryable http 5",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

func (c Client) get(ctx context.Context, url string, headers map[string]string) (*http.Response, error) {
	attempts := c.MaxRetries
	if attempts <= 0 {
		attempts = 7
	}
	backoff := c.RetryBackoff
	if backoff <= 0 {
		backoff = 250 * time.Millisecond
	}
	maxBackoff := c.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 5 * time.Second
	}
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	var lastErr error
	for attempt := 0; attempt <= attempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		if c.UserAgent != "" {
			req.Header.Set("User-Agent", c.UserAgent)
		}
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		resp, err := client.Do(req)
		if err == nil && !isRetryable(resp.StatusCode) {
			return resp, nil
		}
		if err == nil {
			lastErr = retryableHTTPError(resp)
			delay := retryDelay(resp.Header.Get("Retry-After"), backoff, maxBackoff, attempt)
			resp.Body.Close()
			if attempt == attempts {
				return nil, lastErr
			}
			if err := sleepContext(ctx, delay); err != nil {
				return nil, err
			}
			continue
		}
		lastErr = err
		if attempt == attempts {
			return nil, lastErr
		}
		if err := sleepContext(ctx, retryDelay("", backoff, maxBackoff, attempt)); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func isRetryable(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

func retryDelay(raw string, base, maxDelay time.Duration, attempt int) time.Duration {
	if raw != "" {
		if seconds, err := time.ParseDuration(raw + "s"); err == nil {
			return capDelay(seconds, maxDelay)
		}
		if when, err := http.ParseTime(raw); err == nil {
			if delay := time.Until(when); delay > 0 {
				return capDelay(delay, maxDelay)
			}
		}
	}
	delay := base
	for i := 0; i < attempt; i++ {
		delay *= 2
	}
	delay = capDelay(delay, maxDelay)
	if delay <= 0 {
		return 0
	}
	jitter := time.Duration(rand.Int63n(int64(delay/2) + 1))
	return capDelay(delay+jitter, maxDelay)
}

func capDelay(delay, maxDelay time.Duration) time.Duration {
	if maxDelay > 0 && delay > maxDelay {
		return maxDelay
	}
	return delay
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
