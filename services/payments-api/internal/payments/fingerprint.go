package payments

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// ComputeRequestHash generates a deterministic SHA-256 hex digest of the canonical payment payload.
// Format: from_account|to_account|amount_cents|currency
// This hash acts as an immutable fingerprint to detect payload tampering or conflicting reuses
// of an idempotency key.
func ComputeRequestHash(fromAccount, toAccount string, amountCents int64, currency string) string {
	payload := fmt.Sprintf("%s|%s|%d|%s", fromAccount, toAccount, amountCents, currency)
	hash := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(hash[:])
}
