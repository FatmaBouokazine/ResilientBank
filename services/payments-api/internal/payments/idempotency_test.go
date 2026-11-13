package payments

import "testing"

func TestRegionScopedKeySeparatesRegions(t *testing.T) {
	a := RegionScopedKey("A", "order-1")
	b := RegionScopedKey("B", "order-1")

	if a == b {
		t.Fatalf("the same raw key in two regions produced the same scoped key: %q", a)
	}
	if a != "A:order-1" || b != "B:order-1" {
		t.Fatalf("unexpected scoping: A=%q B=%q", a, b)
	}
}

func TestRegionScopedKeyDefaultsToRegionA(t *testing.T) {
	if got := RegionScopedKey("", "k"); got != "A:k" {
		t.Fatalf("empty region = %q; want A:k", got)
	}
}

func TestEnsureRegionScopedDoesNotDoublePrefix(t *testing.T) {
	if got := EnsureRegionScoped("A", "A:k"); got != "A:k" {
		t.Fatalf("already-scoped key = %q; want A:k", got)
	}
	if got := EnsureRegionScoped("A", "k"); got != "A:k" {
		t.Fatalf("raw key = %q; want A:k", got)
	}
	if got := EnsureRegionScoped("B", "A:k"); got != "B:A:k" {
		t.Fatalf("foreign-region key = %q; want B:A:k", got)
	}
}

func TestStripRegionPrefix(t *testing.T) {
	if got := StripRegionPrefix("A:order-1"); got != "order-1" {
		t.Fatalf("got %q; want order-1", got)
	}
	if got := StripRegionPrefix("raw"); got != "raw" {
		t.Fatalf("got %q; want raw", got)
	}
}
