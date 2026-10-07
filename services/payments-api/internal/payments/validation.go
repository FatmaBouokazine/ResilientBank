package payments

import (
	"errors"
	"unicode"

	paymentsv1 "github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/gen/payments/v1"
)

var (
	ErrEmptyIdempotencyKey     = errors.New("idempotency_key cannot be empty")
	ErrIdempotencyKeyTooLong   = errors.New("idempotency_key exceeds maximum length of 128 characters")
	ErrInvalidAmount           = errors.New("amount_cents must be strictly greater than 0")
	ErrInvalidCurrency         = errors.New("currency must be exactly 3 uppercase letters (ISO 4217)")
	ErrEmptyFromAccount        = errors.New("from_account cannot be empty")
	ErrEmptyToAccount          = errors.New("to_account cannot be empty")
	ErrIdenticalAccounts       = errors.New("from_account and to_account must be different")
)

// ValidateCreatePaymentRequest validates the incoming payment against domain constraints:
// 1. idempotency_key is not empty and <= 128 characters
// 2. amount_cents > 0
// 3. currency is exactly 3 uppercase ASCII letters
// 4. from_account and to_account are non-empty and distinct
func ValidateCreatePaymentRequest(req *paymentsv1.CreatePaymentRequest) error {
	if req == nil {
		return errors.New("request cannot be nil")
	}

	if len(req.IdempotencyKey) == 0 {
		return ErrEmptyIdempotencyKey
	}
	if len(req.IdempotencyKey) > 128 {
		return ErrIdempotencyKeyTooLong
	}

	if req.AmountCents <= 0 {
		return ErrInvalidAmount
	}

	if len(req.Currency) != 3 {
		return ErrInvalidCurrency
	}
	for _, r := range req.Currency {
		if !unicode.IsUpper(r) || !unicode.IsLetter(r) || r > 127 {
			return ErrInvalidCurrency
		}
	}

	if len(req.FromAccount) == 0 {
		return ErrEmptyFromAccount
	}
	if len(req.ToAccount) == 0 {
		return ErrEmptyToAccount
	}
	if req.FromAccount == req.ToAccount {
		return ErrIdenticalAccounts
	}

	return nil
}
