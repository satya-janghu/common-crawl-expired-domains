package extraction

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type WARCRecord struct {
	Headers map[string]string
	Body    []byte
}

func DecodeWARC(data []byte) (WARCRecord, error) {
	return DecodeWARCWithLimit(data, 0)
}

func DecodeWARCWithLimit(data []byte, maxDecodedBody int64) (WARCRecord, error) {
	reader := bytes.NewReader(data)
	var input io.Reader = reader
	if gz, err := gzip.NewReader(reader); err == nil {
		defer gz.Close()
		input = gz
	} else {
		reader.Seek(0, io.SeekStart)
	}
	br := bufio.NewReader(input)
	first, err := br.ReadString('\n')
	if err != nil {
		return WARCRecord{}, err
	}
	if !strings.HasPrefix(first, "WARC/") {
		return WARCRecord{}, fmt.Errorf("missing WARC version")
	}
	headers := map[string]string{}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return WARCRecord{}, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		headers[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
	length := int64(-1)
	if raw := headers["content-length"]; raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			length = parsed
		}
	}
	var body []byte
	if length >= 0 {
		if maxDecodedBody > 0 && length > maxDecodedBody {
			return WARCRecord{}, fmt.Errorf("oversized WARC body: %d bytes exceeds limit %d", length, maxDecodedBody)
		}
		body, err = io.ReadAll(io.LimitReader(br, length))
		if err == nil && int64(len(body)) < length {
			return WARCRecord{}, fmt.Errorf("truncated WARC body: got %d bytes, want %d", len(body), length)
		}
	} else {
		reader := io.Reader(br)
		if maxDecodedBody > 0 {
			reader = io.LimitReader(br, maxDecodedBody+1)
		}
		body, err = io.ReadAll(reader)
		if maxDecodedBody > 0 && int64(len(body)) > maxDecodedBody {
			return WARCRecord{}, fmt.Errorf("oversized WARC body exceeds limit %d", maxDecodedBody)
		}
	}
	if err != nil {
		return WARCRecord{}, err
	}
	return WARCRecord{Headers: headers, Body: body}, nil
}

func ExtractWARCHTML(data []byte, ctx HTMLContext) ([]LinkObservation, error) {
	return ExtractWARCHTMLWithLimit(data, ctx, 0)
}

func ExtractWARCHTMLWithLimit(data []byte, ctx HTMLContext, maxDecodedBody int64) ([]LinkObservation, error) {
	record, err := DecodeWARCWithLimit(data, maxDecodedBody)
	if err != nil {
		return nil, err
	}
	if record.Headers["warc-type"] != "response" {
		return nil, nil
	}
	body := record.Body
	if bytes.HasPrefix(body, []byte("HTTP/")) {
		resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(body)), nil)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		contentType := strings.ToLower(resp.Header.Get("Content-Type"))
		if contentType != "" && !strings.Contains(contentType, "html") {
			return nil, nil
		}
		return ExtractHTMLLinks(resp.Body, ctx)
	}
	return ExtractHTMLLinks(bytes.NewReader(body), ctx)
}
