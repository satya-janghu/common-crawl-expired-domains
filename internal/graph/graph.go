package graph

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	"expired-domain-finder/internal/normalization"

	"github.com/klauspost/compress/zstd"
)

type CandidateObservation struct {
	SourceDomain string `json:"sourceDomain"`
	TargetDomain string `json:"targetDomain"`
	ReleaseID    string `json:"releaseId"`
	EdgeCount    int    `json:"edgeCount"`
}

type Vertex struct {
	ID     string
	Domain string
}

type Edge struct {
	From  string
	To    string
	Count int
}

type SourceEdge struct {
	SourceDomain string
	TargetID     string
	Count        int
}

type Release struct {
	ID          string
	Crawls      []string
	Location    string
	VertexPath  string
	EdgePath    string
	DomainNodes int64
	DomainArcs  int64
}

type GraphInfoEntry struct {
	ID       string   `json:"id"`
	Crawls   []string `json:"crawls"`
	Location string   `json:"location"`
	Stats    struct {
		Domain struct {
			Nodes int64 `json:"nodes"`
			Arcs  int64 `json:"arcs"`
		} `json:"domain"`
	} `json:"stats"`
}

func ScanVertices(r io.Reader, sourceDomains map[string]bool) (map[string]string, error) {
	input, closeInput, err := maybeCompressedReader(r)
	if err != nil {
		return nil, err
	}
	if closeInput != nil {
		defer closeInput()
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	out := map[string]string{}
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		domain := normalizeGraphDomain(fields[1])
		normalized, err := normalization.NormalizeDomain(domain, nil, nil)
		if err != nil {
			continue
		}
		if sourceDomains[normalized.RegistrableDomain] {
			out[fields[0]] = normalized.RegistrableDomain
		}
	}
	return out, scanner.Err()
}

func ScanAllVertices(r io.Reader) (map[string]string, error) {
	input, closeInput, err := maybeCompressedReader(r)
	if err != nil {
		return nil, err
	}
	if closeInput != nil {
		defer closeInput()
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	out := map[string]string{}
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		domain := normalizeGraphDomain(fields[1])
		normalized, err := normalization.NormalizeDomain(domain, nil, nil)
		if err != nil {
			continue
		}
		out[fields[0]] = normalized.RegistrableDomain
	}
	return out, scanner.Err()
}

func ScanEdges(r io.Reader, sourceIDs map[string]string, targetResolver map[string]string, releaseID string) ([]CandidateObservation, error) {
	sourceEdges, err := ScanSourceEdges(r, sourceIDs)
	if err != nil {
		return nil, err
	}
	return ObservationsFromSourceEdges(sourceEdges, targetResolver, releaseID), nil
}

func ScanSourceEdges(r io.Reader, sourceIDs map[string]string) ([]SourceEdge, error) {
	input, closeInput, err := maybeCompressedReader(r)
	if err != nil {
		return nil, err
	}
	if closeInput != nil {
		defer closeInput()
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	seen := map[string]SourceEdge{}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		sourceDomain, ok := sourceIDs[fields[0]]
		if !ok {
			continue
		}
		key := sourceDomain + "\x00" + fields[1]
		obs := seen[key]
		obs.SourceDomain = sourceDomain
		obs.TargetID = fields[1]
		obs.Count++
		seen[key] = obs
	}
	out := make([]SourceEdge, 0, len(seen))
	for _, obs := range seen {
		out = append(out, obs)
	}
	return out, scanner.Err()
}

func TargetIDs(edges []SourceEdge) map[string]bool {
	out := make(map[string]bool, len(edges))
	for _, edge := range edges {
		out[edge.TargetID] = true
	}
	return out
}

func ResolveVertices(r io.Reader, targetIDs map[string]bool) (map[string]string, error) {
	input, closeInput, err := maybeCompressedReader(r)
	if err != nil {
		return nil, err
	}
	if closeInput != nil {
		defer closeInput()
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	out := map[string]string{}
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || !targetIDs[fields[0]] {
			continue
		}
		domain := normalizeGraphDomain(fields[1])
		normalized, err := normalization.NormalizeDomain(domain, nil, nil)
		if err != nil {
			continue
		}
		out[fields[0]] = normalized.RegistrableDomain
	}
	return out, scanner.Err()
}

func normalizeGraphDomain(raw string) string {
	raw = strings.Trim(strings.TrimSpace(raw), ".")
	if raw == "" {
		return raw
	}
	labels := strings.Split(raw, ".")
	if len(labels) < 2 || !looksLikeLeadingTLD(labels[0]) {
		return raw
	}
	for i, j := 0, len(labels)-1; i < j; i, j = i+1, j-1 {
		labels[i], labels[j] = labels[j], labels[i]
	}
	return strings.Join(labels, ".")
}

func looksLikeLeadingTLD(label string) bool {
	switch strings.ToLower(label) {
	case "com", "net", "org", "edu", "gov", "mil", "int", "io", "ai", "co", "uk", "de", "fr", "jp", "au", "ca", "us", "br", "in", "cn":
		return true
	default:
		return false
	}
}

func LoadGraphInfo(ctx context.Context, indexBaseURL, userAgent string, client *http.Client) ([]Release, error) {
	base := strings.TrimRight(indexBaseURL, "/")
	if base == "" {
		base = "https://index.commoncrawl.org"
	}
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/graphinfo.json", nil)
	if err != nil {
		return nil, err
	}
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("common crawl graphinfo http %d", resp.StatusCode)
	}
	var entries []GraphInfoEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, err
	}
	out := make([]Release, 0, len(entries))
	for _, entry := range entries {
		release := Release{
			ID:          entry.ID,
			Crawls:      entry.Crawls,
			Location:    entry.Location,
			DomainNodes: entry.Stats.Domain.Nodes,
			DomainArcs:  entry.Stats.Domain.Arcs,
		}
		release.VertexPath, release.EdgePath = StandardDomainPaths(release)
		out = append(out, release)
	}
	return out, nil
}

func StandardDomainPaths(release Release) (string, string) {
	location := strings.TrimRight(release.Location, "/")
	if strings.HasPrefix(location, "s3://commoncrawl/") {
		location = strings.TrimPrefix(location, "s3://commoncrawl/")
	} else if strings.HasPrefix(location, "https://data.commoncrawl.org/") {
		u, err := url.Parse(location)
		if err == nil {
			location = strings.TrimLeft(u.Path, "/")
		}
	}
	id := release.ID
	if id == "" {
		id = path.Base(location)
	}
	prefix := strings.TrimRight(location, "/") + "/domain/"
	return prefix + id + "-domain-vertices.txt.gz", prefix + id + "-domain-edges.txt.gz"
}

func ResolveRelease(selections []string, releases []Release) (Release, bool) {
	selected := map[string]bool{}
	for _, selection := range selections {
		selection = strings.TrimSpace(selection)
		if selection != "" {
			selected[selection] = true
		}
	}
	for _, release := range releases {
		if release.ID != "" && selected[release.ID] && release.DomainNodes > 0 && release.VertexPath != "" && release.EdgePath != "" {
			return release, true
		}
		for _, crawl := range release.Crawls {
			if selected[strings.TrimSpace(crawl)] && release.DomainNodes > 0 && release.VertexPath != "" && release.EdgePath != "" {
				return release, true
			}
		}
	}
	return Release{}, false
}

func ObservationsFromSourceEdges(edges []SourceEdge, targetResolver map[string]string, releaseID string) []CandidateObservation {
	out := make([]CandidateObservation, 0, len(edges))
	for _, edge := range edges {
		targetDomain, ok := targetResolver[edge.TargetID]
		if !ok || targetDomain == edge.SourceDomain {
			continue
		}
		out = append(out, CandidateObservation{
			SourceDomain: edge.SourceDomain,
			TargetDomain: targetDomain,
			ReleaseID:    releaseID,
			EdgeCount:    edge.Count,
		})
	}
	return out
}

func maybeCompressedReader(r io.Reader) (io.Reader, func() error, error) {
	br := bufio.NewReader(r)
	header, err := br.Peek(4)
	if err != nil && err != io.EOF {
		return nil, nil, err
	}
	if len(header) >= 2 && header[0] == 0x1f && header[1] == 0x8b {
		gz, err := gzip.NewReader(br)
		if err != nil {
			return nil, nil, err
		}
		return gz, gz.Close, nil
	}
	if len(header) == 4 && header[0] == 0x28 && header[1] == 0xb5 && header[2] == 0x2f && header[3] == 0xfd {
		zr, err := zstd.NewReader(br)
		if err != nil {
			return nil, nil, err
		}
		return zr, func() error {
			zr.Close()
			return nil
		}, nil
	}
	return br, nil, nil
}
