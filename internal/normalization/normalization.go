package normalization

import (
	"fmt"
	"net"
	"net/url"
	"strings"

	"golang.org/x/net/idna"
	"golang.org/x/net/publicsuffix"
)

type Result struct {
	Original          string
	Host              string
	RegistrableDomain string
	ASCII             string
	Unicode           string
	TLD               string
}

func NormalizeSource(input string) (Result, error) {
	return NormalizeDomain(input, nil, nil)
}

func NormalizeDomain(input string, blockedDomains, blockedSuffixes []string) (Result, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return Result{}, fmt.Errorf("empty domain")
	}
	if !strings.Contains(trimmed, "://") {
		trimmed = "https://" + trimmed
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return Result{}, err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return Result{}, fmt.Errorf("unsupported scheme: %s", parsed.Scheme)
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host == "" {
		return Result{}, fmt.Errorf("missing host")
	}
	if ip := net.ParseIP(host); ip != nil {
		return Result{}, fmt.Errorf("ip addresses are not domains")
	}
	ascii, err := idna.Lookup.ToASCII(host)
	if err != nil {
		return Result{}, fmt.Errorf("idna ascii: %w", err)
	}
	unicode, err := idna.Lookup.ToUnicode(ascii)
	if err != nil {
		return Result{}, fmt.Errorf("idna unicode: %w", err)
	}
	effective, err := publicsuffix.EffectiveTLDPlusOne(ascii)
	if err != nil {
		return Result{}, fmt.Errorf("not a registrable domain: %w", err)
	}
	suffix, _ := publicsuffix.PublicSuffix(ascii)
	if effective == suffix {
		return Result{}, fmt.Errorf("public suffix is not registrable: %s", ascii)
	}
	for _, blocked := range blockedDomains {
		if strings.EqualFold(effective, strings.TrimSpace(blocked)) {
			return Result{}, fmt.Errorf("blocked domain: %s", effective)
		}
	}
	for _, blocked := range blockedSuffixes {
		blocked = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(blocked)), ".")
		if blocked != "" && (effective == blocked || strings.HasSuffix(effective, "."+blocked)) {
			return Result{}, fmt.Errorf("blocked suffix: %s", blocked)
		}
	}
	labels := strings.Split(effective, ".")
	return Result{
		Original:          input,
		Host:              ascii,
		RegistrableDomain: effective,
		ASCII:             effective,
		Unicode:           unicode,
		TLD:               labels[len(labels)-1],
	}, nil
}

func Deduplicate(inputs []string, blockedDomains, blockedSuffixes []string) ([]Result, error) {
	seen := map[string]bool{}
	out := make([]Result, 0, len(inputs))
	for _, input := range inputs {
		normalized, err := NormalizeDomain(input, blockedDomains, blockedSuffixes)
		if err != nil {
			return nil, err
		}
		if seen[normalized.RegistrableDomain] {
			continue
		}
		seen[normalized.RegistrableDomain] = true
		out = append(out, normalized)
	}
	return out, nil
}
