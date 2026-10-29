package extraction

import (
	"fmt"
	"io"
	"net/url"
	"strings"

	"expired-domain-finder/internal/normalization"

	"golang.org/x/net/html"
)

type LinkObservation struct {
	SourceDomain      string `json:"sourceDomain"`
	SourceURL         string `json:"sourceUrl"`
	CrawlID           string `json:"crawlId"`
	Timestamp         string `json:"timestamp"`
	WARCFile          string `json:"warcFile,omitempty"`
	WARCOffset        int64  `json:"warcOffset,omitempty"`
	WARCLength        int64  `json:"warcLength,omitempty"`
	OutboundURL       string `json:"outboundUrl"`
	AnchorText        string `json:"anchorText,omitempty"`
	Rel               string `json:"rel,omitempty"`
	Snippet           string `json:"snippet,omitempty"`
	RegistrableDomain string `json:"registrableDomain"`
	Mode              string `json:"mode"`
}

type HTMLContext struct {
	SourceDomain string
	SourceURL    string
	CrawlID      string
	Timestamp    string
	WARCFile     string
	WARCOffset   int64
	WARCLength   int64
}

func ExtractHTMLLinks(r io.Reader, ctx HTMLContext) ([]LinkObservation, error) {
	base, err := url.Parse(ctx.SourceURL)
	if err != nil {
		return nil, err
	}
	doc, err := html.Parse(r)
	if err != nil {
		return nil, err
	}
	var out []LinkObservation
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			if obs, ok := observationFromAnchor(n, base, ctx); ok {
				out = append(out, obs)
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(doc)
	return out, nil
}

func observationFromAnchor(n *html.Node, base *url.URL, ctx HTMLContext) (LinkObservation, bool) {
	var href, rel string
	for _, attr := range n.Attr {
		switch strings.ToLower(attr.Key) {
		case "href":
			href = strings.TrimSpace(attr.Val)
		case "rel":
			rel = attr.Val
		}
	}
	if href == "" || strings.HasPrefix(href, "#") || strings.HasPrefix(strings.ToLower(href), "javascript:") || strings.HasPrefix(strings.ToLower(href), "mailto:") {
		return LinkObservation{}, false
	}
	parsed, err := url.Parse(href)
	if err != nil {
		return LinkObservation{}, false
	}
	resolved := base.ResolveReference(parsed)
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return LinkObservation{}, false
	}
	normalized, err := normalization.NormalizeDomain(resolved.String(), nil, nil)
	if err != nil {
		return LinkObservation{}, false
	}
	return LinkObservation{
		SourceDomain:      ctx.SourceDomain,
		SourceURL:         ctx.SourceURL,
		CrawlID:           ctx.CrawlID,
		Timestamp:         ctx.Timestamp,
		WARCFile:          ctx.WARCFile,
		WARCOffset:        ctx.WARCOffset,
		WARCLength:        ctx.WARCLength,
		OutboundURL:       resolved.String(),
		AnchorText:        strings.TrimSpace(textContent(n)),
		Rel:               rel,
		Snippet:           snippet(n),
		RegistrableDomain: normalized.RegistrableDomain,
		Mode:              "warc",
	}, true
}

func textContent(n *html.Node) string {
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.TextNode {
			b.WriteString(node.Data)
			b.WriteByte(' ')
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(n)
	return strings.Join(strings.Fields(b.String()), " ")
}

func snippet(n *html.Node) string {
	text := textContent(n)
	if len(text) > 160 {
		return text[:160]
	}
	return text
}

func ValidateMode(mode string) error {
	switch mode {
	case "web_graph", "wat", "warc":
		return nil
	default:
		return fmt.Errorf("unsupported extraction mode: %s", mode)
	}
}
