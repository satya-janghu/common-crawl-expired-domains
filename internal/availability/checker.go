package availability

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DNSResult struct {
	Domain  string
	Present bool
	Records map[string][]string
}

type DNSResolver interface {
	LookupNS(context.Context, string) ([]*net.NS, error)
	LookupHost(context.Context, string) ([]string, error)
	LookupMX(context.Context, string) ([]*net.MX, error)
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

type SOAResolver interface {
	LookupSOA(context.Context, string) ([]string, error)
}

type ConfigurableResolver struct {
	Resolver *net.Resolver
	Servers  []string
}

func NewDNSResolver(servers []string) ConfigurableResolver {
	cleaned := make([]string, 0, len(servers))
	for _, server := range servers {
		server = strings.TrimSpace(server)
		if server == "" {
			continue
		}
		cleaned = append(cleaned, dnsAddress(server))
	}
	resolver := &net.Resolver{}
	if len(cleaned) > 0 {
		resolver.PreferGo = true
		resolver.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dialer := net.Dialer{}
			return dialer.DialContext(ctx, "udp", cleaned[0])
		}
	}
	return ConfigurableResolver{Resolver: resolver, Servers: cleaned}
}

func (r ConfigurableResolver) LookupNS(ctx context.Context, domain string) ([]*net.NS, error) {
	return r.resolver().LookupNS(ctx, domain)
}

func (r ConfigurableResolver) LookupHost(ctx context.Context, domain string) ([]string, error) {
	return r.resolver().LookupHost(ctx, domain)
}

func (r ConfigurableResolver) LookupMX(ctx context.Context, domain string) ([]*net.MX, error) {
	return r.resolver().LookupMX(ctx, domain)
}

func (r ConfigurableResolver) LookupIPAddr(ctx context.Context, domain string) ([]net.IPAddr, error) {
	return r.resolver().LookupIPAddr(ctx, domain)
}

func (r ConfigurableResolver) LookupSOA(ctx context.Context, domain string) ([]string, error) {
	servers := r.Servers
	if len(servers) == 0 {
		servers = []string{"1.1.1.1:53"}
	}
	return lookupSOA(ctx, servers[0], domain)
}

func (r ConfigurableResolver) resolver() *net.Resolver {
	if r.Resolver != nil {
		return r.Resolver
	}
	return net.DefaultResolver
}

func CheckDNS(ctx context.Context, resolver DNSResolver, domain string) DNSResult {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	result := DNSResult{Domain: domain, Records: map[string][]string{}}
	if ns, err := resolver.LookupNS(ctx, domain); err == nil && len(ns) > 0 {
		result.Present = true
		for _, record := range ns {
			result.Records["NS"] = append(result.Records["NS"], record.Host)
		}
	}
	if hosts, err := resolver.LookupHost(ctx, domain); err == nil && len(hosts) > 0 {
		result.Present = true
		result.Records["A_AAAA"] = append(result.Records["A_AAAA"], hosts...)
	}
	if mx, err := resolver.LookupMX(ctx, domain); err == nil && len(mx) > 0 {
		result.Present = true
		for _, record := range mx {
			result.Records["MX"] = append(result.Records["MX"], record.Host)
		}
	}
	if soa, err := resolver.LookupIPAddr(ctx, domain); err == nil && len(soa) > 0 {
		result.Present = true
		for _, record := range soa {
			result.Records["ADDR"] = append(result.Records["ADDR"], record.String())
		}
	}
	if soaResolver, ok := resolver.(SOAResolver); ok {
		if soa, err := soaResolver.LookupSOA(ctx, domain); err == nil && len(soa) > 0 {
			result.Present = true
			result.Records["SOA"] = append(result.Records["SOA"], soa...)
		}
	}
	return result
}

func dnsAddress(server string) string {
	if strings.Contains(server, ":") {
		return server
	}
	return net.JoinHostPort(server, "53")
}

func lookupSOA(ctx context.Context, server, domain string) ([]string, error) {
	query, id, err := dnsQuery(domain, 6)
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(ctx, "udp", server)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}
	_ = conn.SetDeadline(deadline)
	if _, err := conn.Write(query); err != nil {
		return nil, err
	}
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return parseSOAResponse(buf[:n], id)
}

func dnsQuery(domain string, qtype uint16) ([]byte, uint16, error) {
	var idBytes [2]byte
	if _, err := io.ReadFull(rand.Reader, idBytes[:]); err != nil {
		return nil, 0, err
	}
	id := binary.BigEndian.Uint16(idBytes[:])
	msg := make([]byte, 12, 512)
	binary.BigEndian.PutUint16(msg[0:2], id)
	binary.BigEndian.PutUint16(msg[2:4], 0x0100)
	binary.BigEndian.PutUint16(msg[4:6], 1)
	for _, label := range strings.Split(strings.TrimSuffix(domain, "."), ".") {
		if label == "" || len(label) > 63 {
			return nil, 0, fmt.Errorf("invalid dns label in %q", domain)
		}
		msg = append(msg, byte(len(label)))
		msg = append(msg, label...)
	}
	msg = append(msg, 0, byte(qtype>>8), byte(qtype), 0, 1)
	return msg, id, nil
}

func parseSOAResponse(msg []byte, id uint16) ([]string, error) {
	if len(msg) < 12 {
		return nil, fmt.Errorf("short dns response")
	}
	if binary.BigEndian.Uint16(msg[0:2]) != id {
		return nil, fmt.Errorf("dns response id mismatch")
	}
	qd := int(binary.BigEndian.Uint16(msg[4:6]))
	an := int(binary.BigEndian.Uint16(msg[6:8]))
	offset := 12
	var err error
	for i := 0; i < qd; i++ {
		_, offset, err = parseDNSName(msg, offset)
		if err != nil {
			return nil, err
		}
		offset += 4
		if offset > len(msg) {
			return nil, fmt.Errorf("short dns question")
		}
	}
	var out []string
	for i := 0; i < an; i++ {
		_, offset, err = parseDNSName(msg, offset)
		if err != nil {
			return nil, err
		}
		if offset+10 > len(msg) {
			return nil, fmt.Errorf("short dns answer")
		}
		typ := binary.BigEndian.Uint16(msg[offset : offset+2])
		rdlen := int(binary.BigEndian.Uint16(msg[offset+8 : offset+10]))
		offset += 10
		if offset+rdlen > len(msg) {
			return nil, fmt.Errorf("short dns rdata")
		}
		if typ == 6 {
			mname, next, err := parseDNSName(msg, offset)
			if err != nil {
				return nil, err
			}
			rname, _, err := parseDNSName(msg, next)
			if err != nil {
				return nil, err
			}
			out = append(out, strings.TrimSuffix(mname, ".")+" "+strings.TrimSuffix(rname, "."))
		}
		offset += rdlen
	}
	return out, nil
}

func parseDNSName(msg []byte, offset int) (string, int, error) {
	var labels []string
	original := offset
	jumped := false
	for depth := 0; depth < 32; depth++ {
		if offset >= len(msg) {
			return "", 0, fmt.Errorf("short dns name")
		}
		length := int(msg[offset])
		if length&0xc0 == 0xc0 {
			if offset+1 >= len(msg) {
				return "", 0, fmt.Errorf("short dns pointer")
			}
			ptr := int(binary.BigEndian.Uint16(msg[offset:offset+2]) & 0x3fff)
			if !jumped {
				original = offset + 2
				jumped = true
			}
			offset = ptr
			continue
		}
		offset++
		if length == 0 {
			if jumped {
				return strings.Join(labels, ".") + ".", original, nil
			}
			return strings.Join(labels, ".") + ".", offset, nil
		}
		if length&0xc0 != 0 || offset+length > len(msg) {
			return "", 0, fmt.Errorf("invalid dns label")
		}
		labels = append(labels, string(msg[offset:offset+length]))
		offset += length
	}
	return "", 0, fmt.Errorf("dns name compression loop")
}

type RDAPResult struct {
	Domain     string
	Found      bool
	NotFound   bool
	Statuses   []string
	HTTPStatus int
}

type RDAPClient struct {
	BaseURL      string
	BootstrapURL string
	HTTPClient   *http.Client
	UserAgent    string
	MaxRetries   int
	RetryBackoff time.Duration
}

var rdapBootstrapCache sync.Map

func (c RDAPClient) Lookup(ctx context.Context, domain string) (RDAPResult, error) {
	base := strings.TrimRight(c.BaseURL, "/")
	if base == "" {
		endpoint, err := c.bootstrapEndpoint(ctx, domain)
		if err == nil && endpoint != "" {
			base = strings.TrimRight(endpoint, "/") + "/domain"
		}
	}
	if base == "" {
		base = "https://rdap.org/domain"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/"+domain, nil)
	if err != nil {
		return RDAPResult{}, err
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := doWithRetry(ctx, client, req, c.MaxRetries, c.RetryBackoff, "rdap")
	if err != nil {
		return RDAPResult{}, err
	}
	defer resp.Body.Close()
	result := RDAPResult{Domain: domain, HTTPStatus: resp.StatusCode}
	if resp.StatusCode == http.StatusNotFound {
		result.NotFound = true
		return result, nil
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return result, fmt.Errorf("rdap rate limited")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return result, fmt.Errorf("rdap http %d", resp.StatusCode)
	}
	var payload struct {
		Status []string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return result, err
	}
	result.Found = true
	result.Statuses = payload.Status
	return result, nil
}

func (c RDAPClient) bootstrapEndpoint(ctx context.Context, domain string) (string, error) {
	tld := domain
	if i := strings.LastIndexByte(domain, '.'); i >= 0 {
		tld = domain[i+1:]
	}
	bootstrapURL := c.BootstrapURL
	if bootstrapURL == "" {
		bootstrapURL = "https://data.iana.org/rdap/dns.json"
	}
	cacheKey := strings.TrimRight(bootstrapURL, "/") + "|" + strings.ToLower(tld)
	if cached, ok := rdapBootstrapCache.Load(cacheKey); ok {
		return cached.(string), nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, bootstrapURL, nil)
	if err != nil {
		return "", err
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := doWithRetry(ctx, client, req, c.MaxRetries, c.RetryBackoff, "rdap bootstrap")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("rdap bootstrap http %d", resp.StatusCode)
	}
	var payload struct {
		Services [][][]string `json:"services"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	for _, service := range payload.Services {
		if len(service) < 2 {
			continue
		}
		for _, suffix := range service[0] {
			if strings.EqualFold(suffix, tld) && len(service[1]) > 0 {
				rdapBootstrapCache.Store(cacheKey, service[1][0])
				return service[1][0], nil
			}
		}
	}
	return "", fmt.Errorf("no rdap bootstrap endpoint for .%s", tld)
}

func SignalsFromDNSRDAP(dns DNSResult, rdap RDAPResult) Signals {
	signals := Signals{DNSPresent: dns.Present, RDAPFound: rdap.Found, RDAPNotFound: rdap.NotFound}
	for _, status := range rdap.Statuses {
		normalized := normalizeRDAPStatus(status)
		switch normalized {
		case "pending_delete":
			signals.RDAPPendingDelete = true
		case "redemption_period":
			signals.RDAPRedemption = true
		case "reserved":
			signals.RDAPReserved = true
		}
	}
	return signals
}

func normalizeRDAPStatus(status string) string {
	status = strings.TrimSpace(status)
	var b strings.Builder
	var previous rune
	for _, r := range status {
		switch {
		case r == '-' || r == '_' || r == ' ' || r == '\t':
			if b.Len() > 0 && previous != '_' {
				b.WriteByte('_')
				previous = '_'
			}
		case r >= 'A' && r <= 'Z':
			if previous >= 'a' && previous <= 'z' {
				b.WriteByte('_')
			}
			lower := r + ('a' - 'A')
			b.WriteRune(lower)
			previous = lower
		default:
			b.WriteRune(r)
			previous = r
		}
	}
	return strings.Trim(b.String(), "_")
}

type RegistrarResult struct {
	Domain     string
	Available  bool
	Premium    bool
	PriceCents int
	Currency   string
	Status     string
}

type RegistrarProvider interface {
	Name() string
	Check(ctx context.Context, domain string) (RegistrarResult, error)
}

type RateLimitError struct {
	Provider   string
	RetryAfter time.Duration
}

func (e RateLimitError) Error() string {
	provider := strings.TrimSpace(e.Provider)
	if provider == "" {
		provider = "provider"
	}
	return provider + " rate limited"
}

type PorkbunProvider struct {
	APIKey     string
	SecretKey  string
	BaseURL    string
	HTTPClient *http.Client
}

func (p PorkbunProvider) Name() string { return "porkbun" }

func (p PorkbunProvider) Check(ctx context.Context, domain string) (RegistrarResult, error) {
	if p.APIKey == "" || p.SecretKey == "" {
		return RegistrarResult{}, fmt.Errorf("porkbun credentials are required")
	}
	base := strings.TrimRight(p.BaseURL, "/")
	if base == "" {
		base = "https://api.porkbun.com/api/json/v3"
	}
	body, err := json.Marshal(map[string]string{"apikey": p.APIKey, "secretapikey": p.SecretKey})
	if err != nil {
		return RegistrarResult{}, err
	}
	reqBody := body
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/domain/checkDomain/"+domain, bytes.NewReader(reqBody))
	if err != nil {
		return RegistrarResult{}, err
	}
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(reqBody)), nil
	}
	req.Header.Set("Content-Type", "application/json")
	client := p.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := doWithRetry(ctx, client, req, 3, 250*time.Millisecond, "porkbun")
	if err != nil {
		return RegistrarResult{}, err
	}
	defer resp.Body.Close()
	var payload struct {
		Status       string `json:"status"`
		Code         string `json:"code"`
		Message      string `json:"message"`
		TTLRemaining int    `json:"ttlRemaining"`
		Response     struct {
			Avail   string `json:"avail"`
			Type    string `json:"type"`
			Premium string `json:"premium"`
			Price   string `json:"price"`
		} `json:"response"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		if resp.StatusCode == http.StatusTooManyRequests {
			return RegistrarResult{}, RateLimitError{Provider: "porkbun", RetryAfter: rateLimitRetryAfter(resp.Header, 0)}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return RegistrarResult{}, fmt.Errorf("porkbun http %d", resp.StatusCode)
		}
		return RegistrarResult{}, err
	}
	if porkbunRateLimited(payload.Status, payload.Code, payload.Message) {
		return RegistrarResult{}, RateLimitError{Provider: "porkbun", RetryAfter: rateLimitRetryAfter(resp.Header, payload.TTLRemaining)}
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return RegistrarResult{}, RateLimitError{Provider: "porkbun", RetryAfter: rateLimitRetryAfter(resp.Header, payload.TTLRemaining)}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return RegistrarResult{}, fmt.Errorf("porkbun http %d", resp.StatusCode)
	}
	if !strings.EqualFold(payload.Status, "success") {
		return RegistrarResult{}, fmt.Errorf("porkbun status %s", safeStatus(payload.Status))
	}
	result := RegistrarResult{Domain: domain, Status: payload.Status, Currency: "USD"}
	result.Available = strings.EqualFold(payload.Response.Avail, "yes")
	result.Premium = strings.EqualFold(payload.Response.Type, "premium") || strings.EqualFold(payload.Response.Premium, "yes")
	if price, err := strconv.ParseFloat(strings.TrimSpace(payload.Response.Price), 64); err == nil && price > 0 {
		result.PriceCents = int(price*100 + 0.5)
	}
	return result, nil
}

func porkbunRateLimited(values ...string) bool {
	for _, value := range values {
		normalized := strings.ToLower(value)
		if strings.Contains(normalized, "rate_limit") || strings.Contains(normalized, "rate limit") || strings.Contains(normalized, "rate-limited") {
			return true
		}
	}
	return false
}

func rateLimitRetryAfter(header http.Header, ttlRemaining int) time.Duration {
	if ttlRemaining > 0 {
		return time.Duration(ttlRemaining) * time.Second
	}
	if retryAfter := parseRetryAfter(header.Get("Retry-After")); retryAfter > 0 {
		return retryAfter
	}
	if reset := strings.TrimSpace(header.Get("X-RateLimit-Reset")); reset != "" {
		if unix, err := strconv.ParseInt(reset, 10, 64); err == nil {
			if delay := time.Until(time.Unix(unix, 0)); delay > 0 {
				return delay
			}
		}
	}
	return 0
}

func safeStatus(status string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		return "unknown"
	}
	if len(status) > 64 {
		return status[:64]
	}
	return status
}

func doWithRetry(ctx context.Context, client *http.Client, req *http.Request, maxRetries int, backoff time.Duration, component string) (*http.Response, error) {
	if maxRetries <= 0 {
		maxRetries = 2
	}
	if backoff <= 0 {
		backoff = 250 * time.Millisecond
	}
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		clone := req.Clone(ctx)
		if req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, err
			}
			clone.Body = body
		}
		resp, err := client.Do(clone)
		if err == nil && !retryableHTTPStatus(resp.StatusCode) {
			return resp, nil
		}
		if err == nil {
			if resp.StatusCode == http.StatusTooManyRequests {
				lastErr = fmt.Errorf("%s rate limited http %d", component, resp.StatusCode)
			} else {
				lastErr = fmt.Errorf("%s retryable http %d", component, resp.StatusCode)
			}
			delay := retryHTTPDelay(resp.Header.Get("Retry-After"), backoff, attempt)
			if attempt == maxRetries {
				return resp, nil
			}
			resp.Body.Close()
			if err := sleep(ctx, delay); err != nil {
				return nil, err
			}
			continue
		}
		lastErr = err
		if attempt == maxRetries {
			return nil, lastErr
		}
		if err := sleep(ctx, retryHTTPDelay("", backoff, attempt)); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func retryableHTTPStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

func retryHTTPDelay(raw string, base time.Duration, attempt int) time.Duration {
	if delay := parseRetryAfter(raw); delay >= 0 {
		return delay
	}
	delay := base
	for i := 0; i < attempt; i++ {
		delay *= 2
	}
	if delay > 5*time.Second {
		return 5 * time.Second
	}
	return delay
}

func parseRetryAfter(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return -1
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(raw); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
		return 0
	}
	return -1
}

func sleep(ctx context.Context, delay time.Duration) error {
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
