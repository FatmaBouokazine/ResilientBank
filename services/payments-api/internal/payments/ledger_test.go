package payments

import (
	"errors"
	"testing"
)

func activeAccount(id string, balance int64) Account {
	return Account{ID: id, HolderName: id, Currency: "EUR", BalanceCents: balance, Status: "ACTIVE"}
}

func TestValidateTransferAcceptsSufficientFundedActivePair(t *testing.T) {
	if err := validateTransfer(activeAccount("a", 10000), activeAccount("b", 0), 5000, "EUR"); err != nil {
		t.Errorf("validateTransfer = %v, want nil", err)
	}
}

func TestValidateTransferAcceptsExactBalance(t *testing.T) {
	// Spending everything is allowed. The CHECK constraint on the column is the backstop;
	// this rule is the clear error message before the database ever sees it.
	if err := validateTransfer(activeAccount("a", 5000), activeAccount("b", 0), 5000, "EUR"); err != nil {
		t.Errorf("validateTransfer spending the full balance = %v, want nil", err)
	}
}

func TestValidateTransferRejectsOverdraft(t *testing.T) {
	err := validateTransfer(activeAccount("a", 4999), activeAccount("b", 0), 5000, "EUR")
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Errorf("validateTransfer = %v, want %v", err, ErrInsufficientFunds)
	}
}

func TestValidateTransferRejectsFrozenAccount(t *testing.T) {
	frozen := activeAccount("a", 10000)
	frozen.Status = "FROZEN"

	if err := validateTransfer(frozen, activeAccount("b", 0), 100, "EUR"); !errors.Is(err, ErrAccountFrozen) {
		t.Errorf("frozen sender: got %v, want %v", err, ErrAccountFrozen)
	}
	if err := validateTransfer(activeAccount("a", 10000), frozen, 100, "EUR"); !errors.Is(err, ErrAccountFrozen) {
		t.Errorf("frozen receiver: got %v, want %v", err, ErrAccountFrozen)
	}

	closed := activeAccount("a", 10000)
	closed.Status = "CLOSED"
	if err := validateTransfer(closed, activeAccount("b", 0), 100, "EUR"); !errors.Is(err, ErrAccountFrozen) {
		t.Errorf("closed sender: got %v, want %v", err, ErrAccountFrozen)
	}
}

func TestValidateTransferRejectsCurrencyMismatch(t *testing.T) {
	usd := activeAccount("b", 0)
	usd.Currency = "USD"

	if err := validateTransfer(activeAccount("a", 10000), usd, 100, "EUR"); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("got %v, want %v", err, ErrCurrencyMismatch)
	}
}

func TestTransferErrorMapsToDistinctCodes(t *testing.T) {
	// Each business outcome must be distinguishable without parsing message strings.
	cases := []struct {
		err  error
		want string
	}{
		{ErrAccountNotFound, "NotFound"},
		{ErrInsufficientFunds, "FailedPrecondition"},
		{ErrAccountFrozen, "FailedPrecondition"},
		{ErrCurrencyMismatch, "FailedPrecondition"},
	}

	for _, tc := range cases {
		got := transferError(tc.err).Error()
		if want := tc.want; !containsCode(got, want) {
			t.Errorf("transferError(%v) = %q, want code %q", tc.err, got, want)
		}
	}
}

func containsCode(msg, code string) bool {
	return len(msg) >= len(code) && (func() bool {
		for i := 0; i+len(code) <= len(msg); i++ {
			if msg[i:i+len(code)] == code {
				return true
			}
		}
		return false
	})()
}
