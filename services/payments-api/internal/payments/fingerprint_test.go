package payments

import (
	"testing"
)

func TestComputeRequestHash_Deterministic(t *testing.T) {
	from := "acc-100"
	to := "acc-200"
	amount := int64(4500)
	curr := "USD"

	hash1 := ComputeRequestHash(from, to, amount, curr)
	hash2 := ComputeRequestHash(from, to, amount, curr)
	hash3 := ComputeRequestHash(from, to, amount, curr)

	if hash1 == "" {
		t.Fatal("expected non-empty hash")
	}
	if hash1 != hash2 || hash2 != hash3 {
		t.Fatalf("expected identical hashes for identical inputs, got %s, %s, %s", hash1, hash2, hash3)
	}
	if len(hash1) != 64 {
		t.Fatalf("expected 64-char sha256 hex string, got len %d (%s)", len(hash1), hash1)
	}
}

func TestComputeRequestHash_ChangesWhenAnyFieldChanges(t *testing.T) {
	baseFrom := "acc-alice"
	baseTo := "acc-bob"
	baseAmount := int64(1000)
	baseCurrency := "EUR"

	baseHash := ComputeRequestHash(baseFrom, baseTo, baseAmount, baseCurrency)

	mutations := []struct {
		name     string
		from     string
		to       string
		amount   int64
		currency string
	}{
		{
			name:     "different from_account",
			from:     "acc-charlie",
			to:       baseTo,
			amount:   baseAmount,
			currency: baseCurrency,
		},
		{
			name:     "different to_account",
			from:     baseFrom,
			to:       "acc-david",
			amount:   baseAmount,
			currency: baseCurrency,
		},
		{
			name:     "different amount_cents",
			from:     baseFrom,
			to:       baseTo,
			amount:   baseAmount + 1,
			currency: baseCurrency,
		},
		{
			name:     "different currency",
			from:     baseFrom,
			to:       baseTo,
			amount:   baseAmount,
			currency: "USD",
		},
	}

	seenHashes := map[string]string{
		baseHash: "base",
	}

	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			mutatedHash := ComputeRequestHash(m.from, m.to, m.amount, m.currency)
			if mutatedHash == baseHash {
				t.Fatalf("mutation %q produced identical hash to base: %s", m.name, mutatedHash)
			}
			if prevName, exists := seenHashes[mutatedHash]; exists {
				t.Fatalf("hash collision between %q and %q: %s", m.name, prevName, mutatedHash)
			}
			seenHashes[mutatedHash] = m.name
		})
	}
}
