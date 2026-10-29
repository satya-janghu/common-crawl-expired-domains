package availability

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClassifyRegistrarAvailable(t *testing.T) {
	got := Classify(Signals{RegistrarAvailable: true, RDAPNotFound: true})
	if got.Status != AvailableRegistrarConfirmed {
		t.Fatalf("status = %s, want %s", got.Status, AvailableRegistrarConfirmed)
	}
	if got.Confidence != ConfidenceHighest {
		t.Fatalf("confidence = %s, want %s", got.Confidence, ConfidenceHighest)
	}
}

func TestClassifyDNSAbsentAloneIsAmbiguous(t *testing.T) {
	got := Classify(Signals{})
	if got.Status != Ambiguous {
		t.Fatalf("status = %s, want %s", got.Status, Ambiguous)
	}
}

func TestClassifyRDAPNotFoundWithDNSPresentIsAmbiguous(t *testing.T) {
	got := Classify(Signals{RDAPNotFound: true, DNSPresent: true})
	if got.Status != Ambiguous {
		t.Fatalf("status = %s, want %s", got.Status, Ambiguous)
	}
}

func TestClassifyRestrictedTLD(t *testing.T) {
	got := Classify(Signals{RestrictedTLD: true})
	if got.Status != RestrictedTLD {
		t.Fatalf("status = %s, want %s", got.Status, RestrictedTLD)
	}
}

func TestClassifyError(t *testing.T) {
	got := Classify(Signals{Error: true})
	if got.Status != Error {
		t.Fatalf("status = %s, want %s", got.Status, Error)
	}
}

func TestClassifyEnumeratedStates(t *testing.T) {
	tests := []struct {
		name    string
		signals Signals
		want    Status
	}{
		{name: "registered active", signals: Signals{RDAPFound: true, DNSPresent: true}, want: RegisteredActive},
		{name: "registered no dns", signals: Signals{RDAPFound: true}, want: RegisteredNoDNS},
		{name: "pending delete", signals: Signals{RDAPPendingDelete: true}, want: PendingDelete},
		{name: "redemption", signals: Signals{RDAPRedemption: true}, want: RedemptionPeriod},
		{name: "reserved", signals: Signals{RDAPReserved: true}, want: Reserved},
		{name: "reserved tld", signals: Signals{ReservedTLD: true}, want: Reserved},
		{name: "premium", signals: Signals{RegistrarPremium: true}, want: Premium},
		{name: "unsupported", signals: Signals{UnsupportedTLD: true}, want: UnsupportedTLD},
		{name: "rate limited", signals: Signals{RateLimited: true}, want: RateLimited},
		{name: "likely available", signals: Signals{RDAPNotFound: true}, want: AvailableLikely},
	}
	for _, tt := range tests {
		got := Classify(tt.signals)
		if got.Status != tt.want {
			t.Fatalf("%s status = %s, want %s", tt.name, got.Status, tt.want)
		}
	}
}

func TestTTLPolicy(t *testing.T) {
	policy := DefaultTTLPolicy()
	if policy.TTL(AvailableRegistrarConfirmed) != time.Hour {
		t.Fatalf("registrar-confirmed TTL = %s, want 1h", policy.TTL(AvailableRegistrarConfirmed))
	}
	if policy.TTL(RegisteredActive) != 14*24*time.Hour {
		t.Fatalf("registered TTL = %s, want 14d", policy.TTL(RegisteredActive))
	}
}

func TestCheckDNSIncludesSOA(t *testing.T) {
	got := CheckDNS(context.Background(), fakeSOAResolver{}, "example.com")
	if !got.Present {
		t.Fatalf("dns result should be present: %+v", got)
	}
	if len(got.Records["SOA"]) != 1 || got.Records["SOA"][0] != "ns1.example.com hostmaster.example.com" {
		t.Fatalf("SOA records = %+v", got.Records["SOA"])
	}
}

func TestSignalsFromDNSRDAPNormalizesLifecycleStatuses(t *testing.T) {
	signals := SignalsFromDNSRDAP(DNSResult{}, RDAPResult{Found: true, Statuses: []string{"pendingDelete", "redemption-period", "reserved"}})
	if !signals.RDAPPendingDelete || !signals.RDAPRedemption || !signals.RDAPReserved {
		t.Fatalf("signals = %+v", signals)
	}
}

func TestNewDNSResolverNormalizesServerAddress(t *testing.T) {
	resolver := NewDNSResolver([]string{"1.1.1.1"})
	if len(resolver.Servers) != 1 || resolver.Servers[0] != "1.1.1.1:53" {
		t.Fatalf("servers = %+v", resolver.Servers)
	}
}

func TestRDAPLookupNotFound(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})}
	got, err := (RDAPClient{BaseURL: "https://rdap.test/domain", HTTPClient: client}).Lookup(context.Background(), "example.test")
	if err != nil {
		t.Fatal(err)
	}
	if !got.NotFound || got.Found {
		t.Fatalf("rdap result = %+v, want not found", got)
	}
}

func TestRDAPLookupUsesBootstrapEndpoint(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		switch req.URL.String() {
		case "https://iana.test/dns.json":
			body := `{"services":[[["test"],["https://rdap.test/"]]]}`
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		case "https://rdap.test/domain/example.test", "https://rdap.test/domain/second.test":
			body := `{"status":["active"]}`
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		default:
			t.Fatalf("unexpected URL: %s", req.URL.String())
			return nil, nil
		}
	})}
	got, err := (RDAPClient{BootstrapURL: "https://iana.test/dns.json", HTTPClient: client}).Lookup(context.Background(), "example.test")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Found || calls != 2 {
		t.Fatalf("rdap=%+v calls=%d", got, calls)
	}
	got, err = (RDAPClient{BootstrapURL: "https://iana.test/dns.json", HTTPClient: client}).Lookup(context.Background(), "second.test")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Found || calls != 3 {
		t.Fatalf("cached rdap=%+v calls=%d, want one bootstrap and two lookups", got, calls)
	}
}

func TestRDAPLookupRetriesRetryableHTTP(t *testing.T) {
	var calls int32
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
		}
		return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})}
	got, err := (RDAPClient{BaseURL: "https://rdap.test/domain", HTTPClient: client, MaxRetries: 1, RetryBackoff: time.Nanosecond}).Lookup(context.Background(), "example.test")
	if err != nil {
		t.Fatal(err)
	}
	if !got.NotFound || calls != 2 {
		t.Fatalf("rdap=%+v calls=%d", got, calls)
	}
}

func TestPorkbunProviderDoesNotLogSecretsInErrors(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})}
	provider := PorkbunProvider{APIKey: "fake-api-key", SecretKey: "fake-secret-key", BaseURL: "https://porkbun.test", HTTPClient: client}
	_, err := provider.Check(context.Background(), "example.com")
	if err == nil {
		t.Fatal("expected connection error")
	}
	if strings.Contains(err.Error(), "fake-api-key") || strings.Contains(err.Error(), "fake-secret-key") {
		t.Fatalf("error leaked credentials: %v", err)
	}
}

func TestPorkbunProviderParsesPriceAndPremium(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		body := `{"status":"SUCCESS","response":{"avail":"yes","type":"premium","price":"123.45"}}`
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	provider := PorkbunProvider{APIKey: "fake-api-key", SecretKey: "fake-secret-key", BaseURL: "https://porkbun.test", HTTPClient: client}
	got, err := provider.Check(context.Background(), "example.com")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Available || !got.Premium || got.PriceCents != 12345 || got.Currency != "USD" {
		t.Fatalf("result = %+v", got)
	}
}

func TestPorkbunProviderTreatsAPIErrorStatusAsError(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		body := `{"status":"ERROR","message":"bad credentials"}`
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	provider := PorkbunProvider{APIKey: "fake-api-key", SecretKey: "fake-secret-key", BaseURL: "https://porkbun.test", HTTPClient: client}
	_, err := provider.Check(context.Background(), "example.com")
	if err == nil {
		t.Fatal("expected API status error")
	}
	if strings.Contains(err.Error(), "fake-api-key") || strings.Contains(err.Error(), "fake-secret-key") || strings.Contains(err.Error(), "bad credentials") {
		t.Fatalf("error leaked sensitive detail: %v", err)
	}
}

func TestPorkbunProviderTreatsJSONRateLimitAsRateLimit(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		body := `{"status":"ERROR","code":"RATE_LIMIT_EXCEEDED","message":"1 out of 1 checks within 10 seconds used.","ttlRemaining":8}`
		return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	provider := PorkbunProvider{APIKey: "fake-api-key", SecretKey: "fake-secret-key", BaseURL: "https://porkbun.test", HTTPClient: client}
	_, err := provider.Check(context.Background(), "example.com")
	if err == nil || !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("err = %v, want rate limited", err)
	}
	var rateLimit RateLimitError
	if !errors.As(err, &rateLimit) || rateLimit.RetryAfter != 8*time.Second {
		t.Fatalf("rate limit = %+v, want 8s", rateLimit)
	}
}

func TestPorkbunProviderMalformedResponseIsError(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{broken")), Header: make(http.Header)}, nil
	})}
	provider := PorkbunProvider{APIKey: "fake-api-key", SecretKey: "fake-secret-key", BaseURL: "https://porkbun.test", HTTPClient: client}
	_, err := provider.Check(context.Background(), "example.com")
	if err == nil {
		t.Fatal("expected malformed response error")
	}
}

func TestPorkbunProviderRateLimitAfterRetries(t *testing.T) {
	var calls int32
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		header := make(http.Header)
		header.Set("Retry-After", "0")
		return &http.Response{StatusCode: http.StatusTooManyRequests, Body: io.NopCloser(bytes.NewReader(nil)), Header: header}, nil
	})}
	provider := PorkbunProvider{APIKey: "fake-api-key", SecretKey: "fake-secret-key", BaseURL: "https://porkbun.test", HTTPClient: client}
	_, err := provider.Check(context.Background(), "example.com")
	if err == nil || !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("err = %v, want rate limited", err)
	}
	if calls != 4 {
		t.Fatalf("calls = %d, want 4", calls)
	}
}

func TestPorkbunProviderRetriesRetryableHTTP(t *testing.T) {
	var calls int32
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
		}
		body := `{"status":"SUCCESS","response":{"avail":"yes","type":"standard","price":"12.34"}}`
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	provider := PorkbunProvider{APIKey: "fake-api-key", SecretKey: "fake-secret-key", BaseURL: "https://porkbun.test", HTTPClient: client}
	got, err := provider.Check(context.Background(), "example.com")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Available || calls != 2 {
		t.Fatalf("result=%+v calls=%d", got, calls)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type fakeSOAResolver struct{}

func (fakeSOAResolver) LookupNS(context.Context, string) ([]*net.NS, error) {
	return nil, net.ErrClosed
}
func (fakeSOAResolver) LookupHost(context.Context, string) ([]string, error) {
	return nil, net.ErrClosed
}
func (fakeSOAResolver) LookupMX(context.Context, string) ([]*net.MX, error) {
	return nil, net.ErrClosed
}
func (fakeSOAResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return nil, net.ErrClosed
}
func (fakeSOAResolver) LookupSOA(context.Context, string) ([]string, error) {
	return []string{"ns1.example.com hostmaster.example.com"}, nil
}
