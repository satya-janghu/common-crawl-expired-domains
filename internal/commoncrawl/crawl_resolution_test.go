package commoncrawl

import (
	"strings"
	"testing"
)

func TestResolveClosestOlderOrExactMonthPrefersExactMonth(t *testing.T) {
	got, err := ResolveClosestOlderOrExactMonth("2026-04", []CatalogEntry{
		{ID: "CC-MAIN-2026-10", From: "2026-05-01"},
		{ID: "CC-MAIN-2026-04", From: "2026-04-12"},
		{ID: "CC-MAIN-2026-02", From: "2026-02-18"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ResolvedID != "CC-MAIN-2026-04" {
		t.Fatalf("resolved crawl = %q, want exact month crawl", got.ResolvedID)
	}
}

func TestResolveClosestOlderOrExactMonthFallsBackOlder(t *testing.T) {
	got, err := ResolveClosestOlderOrExactMonth("2026-03", []CatalogEntry{
		{ID: "future", From: "2026-04-01"},
		{ID: "older", From: "2026-02-28"},
		{ID: "oldest", From: "2026-01-01"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ResolvedID != "older" {
		t.Fatalf("resolved crawl = %q, want closest older crawl", got.ResolvedID)
	}
}

func TestResolveClosestOlderOrExactMonthNeverSelectsFuture(t *testing.T) {
	got, err := ResolveClosestOlderOrExactMonth("2026-03", []CatalogEntry{
		{ID: "future", From: "2026-04-01"},
		{ID: "same-month", From: "2026-03-31"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ResolvedID != "same-month" {
		t.Fatalf("resolved crawl = %q, want same-month crawl", got.ResolvedID)
	}
}

func TestResolveClosestOlderOrExactMonthNoOlderCrawl(t *testing.T) {
	_, err := ResolveClosestOlderOrExactMonth("2026-03", []CatalogEntry{
		{ID: "future", From: "2026-04-01"},
	})
	if err == nil || !strings.Contains(err.Error(), "no Common Crawl catalog entry") {
		t.Fatalf("error = %v, want clear no older crawl error", err)
	}
}
