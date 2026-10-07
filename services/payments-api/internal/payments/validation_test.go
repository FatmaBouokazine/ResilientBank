package payments

import (
	"errors"
	"strings"
	"testing"

	paymentsv1 "github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/gen/payments/v1"
)

func TestValidateCreatePaymentRequest(t *testing.T) {
	longKey := strings.Repeat("a", 129)
	validKey128 := strings.Repeat("k", 128)

	tests := []struct {
		name        string
		req         *paymentsv1.CreatePaymentRequest
		wantErr     error
		wantErrText string
	}{
		{
			name:        "nil request",
			req:         nil,
			wantErrText: "request cannot be nil",
		},
		{
			name: "empty idempotency key",
			req: &paymentsv1.CreatePaymentRequest{
				IdempotencyKey: "",
				FromAccount:    "acc-alice",
				ToAccount:      "acc-bob",
				AmountCents:    1000,
				Currency:       "EUR",
			},
			wantErr: ErrEmptyIdempotencyKey,
		},
		{
			name: "idempotency key too long (> 128 chars)",
			req: &paymentsv1.CreatePaymentRequest{
				IdempotencyKey: longKey,
				FromAccount:    "acc-alice",
				ToAccount:      "acc-bob",
				AmountCents:    1000,
				Currency:       "EUR",
			},
			wantErr: ErrIdempotencyKeyTooLong,
		},
		{
			name: "idempotency key exactly 128 chars is valid",
			req: &paymentsv1.CreatePaymentRequest{
				IdempotencyKey: validKey128,
				FromAccount:    "acc-alice",
				ToAccount:      "acc-bob",
				AmountCents:    1000,
				Currency:       "EUR",
			},
			wantErr: nil,
		},
		{
			name: "zero amount_cents",
			req: &paymentsv1.CreatePaymentRequest{
				IdempotencyKey: "idem-key-1",
				FromAccount:    "acc-alice",
				ToAccount:      "acc-bob",
				AmountCents:    0,
				Currency:       "EUR",
			},
			wantErr: ErrInvalidAmount,
		},
		{
			name: "negative amount_cents",
			req: &paymentsv1.CreatePaymentRequest{
				IdempotencyKey: "idem-key-1",
				FromAccount:    "acc-alice",
				ToAccount:      "acc-bob",
				AmountCents:    -500,
				Currency:       "EUR",
			},
			wantErr: ErrInvalidAmount,
		},
		{
			name: "empty currency",
			req: &paymentsv1.CreatePaymentRequest{
				IdempotencyKey: "idem-key-1",
				FromAccount:    "acc-alice",
				ToAccount:      "acc-bob",
				AmountCents:    1000,
				Currency:       "",
			},
			wantErr: ErrInvalidCurrency,
		},
		{
			name: "currency too short",
			req: &paymentsv1.CreatePaymentRequest{
				IdempotencyKey: "idem-key-1",
				FromAccount:    "acc-alice",
				ToAccount:      "acc-bob",
				AmountCents:    1000,
				Currency:       "EU",
			},
			wantErr: ErrInvalidCurrency,
		},
		{
			name: "currency too long",
			req: &paymentsv1.CreatePaymentRequest{
				IdempotencyKey: "idem-key-1",
				FromAccount:    "acc-alice",
				ToAccount:      "acc-bob",
				AmountCents:    1000,
				Currency:       "EURO",
			},
			wantErr: ErrInvalidCurrency,
		},
		{
			name: "currency lowercase",
			req: &paymentsv1.CreatePaymentRequest{
				IdempotencyKey: "idem-key-1",
				FromAccount:    "acc-alice",
				ToAccount:      "acc-bob",
				AmountCents:    1000,
				Currency:       "eur",
			},
			wantErr: ErrInvalidCurrency,
		},
		{
			name: "currency with digits or symbols",
			req: &paymentsv1.CreatePaymentRequest{
				IdempotencyKey: "idem-key-1",
				FromAccount:    "acc-alice",
				ToAccount:      "acc-bob",
				AmountCents:    1000,
				Currency:       "E1R",
			},
			wantErr: ErrInvalidCurrency,
		},
		{
			name: "empty from_account",
			req: &paymentsv1.CreatePaymentRequest{
				IdempotencyKey: "idem-key-1",
				FromAccount:    "",
				ToAccount:      "acc-bob",
				AmountCents:    1000,
				Currency:       "EUR",
			},
			wantErr: ErrEmptyFromAccount,
		},
		{
			name: "empty to_account",
			req: &paymentsv1.CreatePaymentRequest{
				IdempotencyKey: "idem-key-1",
				FromAccount:    "acc-alice",
				ToAccount:      "",
				AmountCents:    1000,
				Currency:       "EUR",
			},
			wantErr: ErrEmptyToAccount,
		},
		{
			name: "from_account and to_account are identical",
			req: &paymentsv1.CreatePaymentRequest{
				IdempotencyKey: "idem-key-1",
				FromAccount:    "acc-same",
				ToAccount:      "acc-same",
				AmountCents:    1000,
				Currency:       "EUR",
			},
			wantErr: ErrIdenticalAccounts,
		},
		{
			name: "valid request",
			req: &paymentsv1.CreatePaymentRequest{
				IdempotencyKey: "idem-key-valid",
				FromAccount:    "acc-alice",
				ToAccount:      "acc-bob",
				AmountCents:    25000,
				Currency:       "USD",
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCreatePaymentRequest(tt.req)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %v, got %v", tt.wantErr, err)
				}
			} else if tt.wantErrText != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrText) {
					t.Fatalf("expected error containing %q, got %v", tt.wantErrText, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}
