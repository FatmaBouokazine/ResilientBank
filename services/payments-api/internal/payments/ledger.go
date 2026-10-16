package payments

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Account is a balance holder. Balances move only through applyTransfer, never by direct
// UPDATE outside this package, so every movement is guaranteed its double-entry pair.
type Account struct {
	ID           string
	HolderName   string
	Currency     string
	BalanceCents int64
	Status       string
}

// Domain errors for the transfer path. Each maps to a specific gRPC code at the call
// site so clients can distinguish "you have no money" from "the account does not exist".
var (
	// ErrAccountNotFound means the named account does not exist.
	ErrAccountNotFound = errors.New("account not found")
	// ErrAccountFrozen means the account exists but is not ACTIVE.
	ErrAccountFrozen = errors.New("account is not active")
	// ErrCurrencyMismatch means the account's currency differs from the payment's.
	ErrCurrencyMismatch = errors.New("account currency does not match payment currency")
	// ErrInsufficientFunds means the sender cannot cover the amount.
	ErrInsufficientFunds = errors.New("insufficient funds")
)

// lockAccounts reads both accounts and holds row locks on them until the transaction
// commits.
//
// LOCKING BOTH UP FRONT IS LOAD-BEARING:
// The balance check and the balance update must see the same world. Without FOR UPDATE, two
// concurrent transfers from one account could each observe a sufficient balance and both
// commit, overdrawing it. The lock serialises spenders per account.
//
// Locks are always taken in id order so two transfers between the same pair in opposite
// directions cannot deadlock each other waiting on reversed lock order.
func lockAccounts(ctx context.Context, tx pgx.Tx, fromID, toID string) (from, to Account, err error) {
	first, second := fromID, toID
	if second < first {
		first, second = second, first
	}

	rows, err := tx.Query(ctx, `
		SELECT id, holder_name, currency, balance_cents, status
		FROM accounts
		WHERE id = ANY($1)
		ORDER BY id
		FOR UPDATE
	`, []string{first, second})
	if err != nil {
		return Account{}, Account{}, fmt.Errorf("lock accounts: %w", err)
	}
	defer rows.Close()

	byID := map[string]Account{}
	for rows.Next() {
		var a Account
		if err := rows.Scan(&a.ID, &a.HolderName, &a.Currency, &a.BalanceCents, &a.Status); err != nil {
			return Account{}, Account{}, fmt.Errorf("scan account: %w", err)
		}
		byID[a.ID] = a
	}
	if err := rows.Err(); err != nil {
		return Account{}, Account{}, fmt.Errorf("read accounts: %w", err)
	}

	from, ok := byID[fromID]
	if !ok {
		return Account{}, Account{}, fmt.Errorf("%w: %s", ErrAccountNotFound, fromID)
	}
	to, ok = byID[toID]
	if !ok {
		return Account{}, Account{}, fmt.Errorf("%w: %s", ErrAccountNotFound, toID)
	}
	return from, to, nil
}

// validateTransfer enforces the business rules before any value moves.
func validateTransfer(from, to Account, amountCents int64, currency string) error {
	if from.Status != "ACTIVE" {
		return fmt.Errorf("%w: %s is %s", ErrAccountFrozen, from.ID, from.Status)
	}
	if to.Status != "ACTIVE" {
		return fmt.Errorf("%w: %s is %s", ErrAccountFrozen, to.ID, to.Status)
	}
	if from.Currency != currency || to.Currency != currency {
		return fmt.Errorf("%w: payment is %s but accounts are %s/%s",
			ErrCurrencyMismatch, currency, from.Currency, to.Currency)
	}
	if from.BalanceCents < amountCents {
		return fmt.Errorf("%w: %s holds %d but %d is required",
			ErrInsufficientFunds, from.ID, from.BalanceCents, amountCents)
	}
	return nil
}

// applyTransfer moves value and writes its double-entry pair, all inside the caller's
// transaction.
//
// The two UPDATEs and the two INSERTs commit atomically with the payment row itself. A
// crash at any point leaves either everything or nothing, which is the only way a ledger
// stays balanced: a debit without its credit is not a partial transfer, it is corruption.
func applyTransfer(ctx context.Context, tx pgx.Tx, paymentID string, fromID, toID string, amountCents int64, currency string, at time.Time) error {
	if _, err := tx.Exec(ctx,
		`UPDATE accounts SET balance_cents = balance_cents - $1, updated_at = now() WHERE id = $2`,
		amountCents, fromID); err != nil {
		return fmt.Errorf("debit %s: %w", fromID, err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE accounts SET balance_cents = balance_cents + $1, updated_at = now() WHERE id = $2`,
		amountCents, toID); err != nil {
		return fmt.Errorf("credit %s: %w", toID, err)
	}

	debitID := uuid.New().String()
	creditID := uuid.New().String()
	if _, err := tx.Exec(ctx, `
		INSERT INTO ledger_entries (id, payment_id, account_id, direction, amount_cents, currency, created_at)
		VALUES ($1, $2, $3, 'DEBIT', $4, $5, $6),
		       ($7, $2, $8, 'CREDIT', $4, $5, $6)`,
		debitID, paymentID, fromID, amountCents, currency, at,
		creditID, toID); err != nil {
		return fmt.Errorf("write ledger pair: %w", err)
	}
	return nil
}

// recordAudit appends one row to the immutable audit trail.
func recordAudit(ctx context.Context, tx pgx.Tx, entityType, entityID, action, actor, details string) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_log (entity_type, entity_id, action, actor, details)
		VALUES ($1, $2, $3, $4, $5::jsonb)`,
		entityType, entityID, action, actor, details); err != nil {
		return fmt.Errorf("audit %s %s: %w", action, entityID, err)
	}
	return nil
}
