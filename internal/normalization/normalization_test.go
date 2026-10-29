package normalization

import "testing"

func TestNormalizeSource(t *testing.T) {
	got, err := NormalizeSource("https://www.TechCrunch.com/path#frag")
	if err != nil {
		t.Fatal(err)
	}
	if got.RegistrableDomain != "techcrunch.com" {
		t.Fatalf("domain = %q, want techcrunch.com", got.RegistrableDomain)
	}
}

func TestNormalizeSourceRejectsScheme(t *testing.T) {
	_, err := NormalizeSource("ftp://example.com")
	if err == nil {
		t.Fatal("expected unsupported scheme error")
	}
}

func TestNormalizeSourceRejectsIP(t *testing.T) {
	_, err := NormalizeSource("https://127.0.0.1/")
	if err == nil {
		t.Fatal("expected IP address rejection")
	}
}

func TestNormalizeSourceRejectsPublicSuffix(t *testing.T) {
	_, err := NormalizeSource("https://co.uk/")
	if err == nil {
		t.Fatal("expected public suffix rejection")
	}
}

func TestNormalizeDomainRejectsBlockedSuffix(t *testing.T) {
	_, err := NormalizeDomain("thing.blocked.example", nil, []string{"blocked.example"})
	if err == nil {
		t.Fatal("expected blocked suffix rejection")
	}
}

func TestNormalizeDomainIDNA(t *testing.T) {
	got, err := NormalizeDomain("https://bücher.example/", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.RegistrableDomain != "xn--bcher-kva.example" {
		t.Fatalf("domain = %q, want xn--bcher-kva.example", got.RegistrableDomain)
	}
	if got.Unicode != "bücher.example" {
		t.Fatalf("unicode = %q, want bücher.example", got.Unicode)
	}
}
