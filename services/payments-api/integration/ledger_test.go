//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// balanceOf reads an account's current balance.
func balanceOf(t *testing.T, pool *pgxpool.Pool, accountID string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var balance int64
	if err := pool.QueryRow(ctx, `SELECT balance_cents FROM accounts WHERE id = $1`, accountID).Scan(&balance); err != nil {
		t.Fatalf("read balance of %s: %v", accountID, err)
	}
	return balance
}

// ledgerSum returns the signed sum of a payment's entries: debits negative, credits
// positive. Double-entry demands exactly zero.
func ledgerSum(t *testing.T, pool *pgxpool.Pool, paymentID string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var sum int64
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN -amount_cents ELSE amount_cents END), 0)
		FROM ledger_entries WHERE payment_id = $1`, paymentID).Scan(&sum)
	if err != nil {
		t.Fatalf("sum ledger for %s: %v", paymentID, err)
	}
	return sum
}

func ledgerCount(t *testing.T, pool *pgxpool.Pool, paymentID string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ledger_entries WHERE payment_id = $1`, paymentID).Scan(&n); err != nil {
		t.Fatalf("count ledger for %s: %v", paymentID, err)
	}
	return n
}

// A settled payment must move value AND leave a balanced double-entry pair behind.
func TestSettledPaymentMovesValueAndBalancesLedger(t *testing.T) {
	svc, pool, _, _, _ := newRelayerHarness(t)
	ctx := context.Background()

	aliceBefore := balanceOf(t, pool, "acc-alice")
	bobBefore := balanceOf(t, pool, "acc-bob")

	resp, err := svc.CreatePayment(ctx, newRequest(t, "ledger-"+uuid.New().String(), 5000))
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}

	if got := balanceOf(t, pool, "acc-alice"); got != aliceBefore-5000 {
		t.Errorf("alice balance = %d, want %d", got, aliceBefore-5000)
	}
	if got := balanceOf(t, pool, "acc-bob"); got != bobBefore+5000 {
		t.Errorf("bob balance = %d, want %d", got, bobBefore+5000)
	}

	if got := ledgerCount(t, pool, resp.GetPaymentId()); got != 2 {
		t.Fatalf("ledger entries = %d, want exactly the DEBIT/CREDIT pair", got)
	}
	if got := ledgerSum(t, pool, resp.GetPaymentId()); got != 0 {
		t.Errorf("ledger sums to %d, want 0; the books do not balance", got)
	}
}

// Spending more than the balance must fail WITHOUT writing anything: no payment, no
// ledger rows, no outbox row, no balance change.
func TestOverdraftIsRejectedAtomically(t *testing.T) {
	svc, pool, _, _, _ := newRelayerHarness(t)
	ctx := context.Background()

	aliceBefore := balanceOf(t, pool, "acc-alice")
	key := "overdraft-" + uuid.New().String()

	_, err := svc.CreatePayment(ctx, newRequest(t, key, aliceBefore+1))
	if err == nil {
		t.Fatal("overdraft was accepted; an account spent money it does not have")
	}
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Errorf("code = %s, want FailedPrecondition", code)
	}

	if got := balanceOf(t, pool, "acc-alice"); got != aliceBefore {
		t.Errorf("alice balance moved to %d; a rejected transfer must touch nothing", got)
	}
	if got := countRows(t, pool, "payments", key); got != 0 {
		t.Errorf("payments rows = %d, want 0", got)
	}
	if got := countRows(t, pool, "outbox", key); got != 0 {
		t.Errorf("outbox rows = %d, want 0", got)
	}

	// No orphan ledger rows for a payment that does not exist.
	var orphans int
	orphanCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.QueryRow(orphanCtx, `
		SELECT count(*) FROM ledger_entries l
		LEFT JOIN payments p ON p.id = l.payment_id
		WHERE p.id IS NULL`).Scan(&orphans); err != nil {
		t.Fatalf("orphan check: %v", err)
	}
	if orphans != 0 {
		t.Errorf("%d ledger entries reference no payment", orphans)
	}
}

// An unknown account is a caller bug, reported as NotFound rather than a generic failure.
func TestUnknownAccountIsNotFound(t *testing.T) {
	svc, _, _, _, _ := newRelayerHarness(t)
	ctx := context.Background()

	req := newRequest(t, "noacct-"+uuid.New().String(), 100)
	req.FromAccount = "acc-ghost"

	_, err := svc.CreatePayment(ctx, req)
	if err == nil {
		t.Fatal("payment from a nonexistent account was accepted")
	}
	if code := status.Code(err); code != codes.NotFound {
		t.Errorf("code = %s, want NotFound", code)
	}
}

// A frozen account cannot send or receive, even with sufficient funds.
func TestFrozenAccountCannotTransact(t *testing.T) {
	svc, pool, _, _, _ := newRelayerHarness(t)
	ctx := context.Background()

	freezeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := pool.Exec(freezeCtx, `UPDATE accounts SET status = 'FROZEN' WHERE id = 'acc-alice'`); err != nil {
		t.Fatalf("freeze alice: %v", err)
	}

	_, err := svc.CreatePayment(ctx, newRequest(t, "frozen-"+uuid.New().String(), 100))
	if err == nil {
		t.Fatal("payment from a frozen account was accepted")
	}
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Errorf("code = %s, want FailedPrecondition", code)
	}
}

// Currency must match on both sides. A EUR payment cannot debit a USD account.
func TestCurrencyMismatchIsRejected(t *testing.T) {
	svc, _, _, _, _ := newRelayerHarness(t)
	ctx := context.Background()

	// acc-carol holds USD; newRequest builds a EUR payment.
	req := newRequest(t, "fx-"+uuid.New().String(), 100)
	req.FromAccount = "acc-carol"
	req.ToAccount = "acc-dave"

	_, err := svc.CreatePayment(ctx, req)
	if err == nil {
		t.Fatal("cross-currency payment was accepted without conversion")
	}
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Errorf("code = %s, want FailedPrecondition", code)
	}
}

// Concurrent spenders on one account must never overdraw it. The row lock serialises them;
// the losers fail cleanly rather than collectively spending money that is not there.
func TestConcurrentSpendersCannotOverdraw(t *testing.T) {
	svc, pool, _, _, _ := newRelayerHarness(t)

	// Alice holds 100000. Eight concurrent transfers of 20000 each demand 160000.
	// At most five can succeed; the rest must fail without moving value.
	const workers = 8
	const amount = int64(20000)

	type result struct{ err error }
	results := make(chan result, workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_, err := svc.CreatePayment(ctx, newRequest(t,
				fmt.Sprintf("race-%d-%s", i, uuid.New().String()), amount))
			results <- result{err: err}
		}(i)
	}

	succeeded := 0
	for i := 0; i < workers; i++ {
		if res := <-results; res.err == nil {
			succeeded++
		}
	}

	if succeeded > 5 {
		t.Errorf("%d transfers succeeded, but the balance only covers 5; the account was overdrawn", succeeded)
	}
	if got := balanceOf(t, pool, "acc-alice"); got < 0 {
		t.Errorf("alice balance = %d; overdraft happened under concurrency", got)
	}
	if got := balanceOf(t, pool, "acc-alice"); got != 100000-int64(succeeded)*amount {
		t.Errorf("alice balance = %d, want %d (exactly the successful transfers debited)",
			got, 100000-int64(succeeded)*amount)
	}
}

// A duplicate retry must not move value twice. The second call returns the original payment
// and the balances reflect exactly one transfer.
func TestDuplicateRetryDoesNotMoveValueTwice(t *testing.T) {
	svc, pool, _, _, _ := newRelayerHarness(t)
	ctx := context.Background()

	aliceBefore := balanceOf(t, pool, "acc-alice")
	key := "ledger-dup-" + uuid.New().String()

	first, err := svc.CreatePayment(ctx, newRequest(t, key, 3000))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := svc.CreatePayment(ctx, newRequest(t, key, 3000))
	if err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	if first.GetPaymentId() != second.GetPaymentId() {
		t.Fatal("retry returned a different payment")
	}

	if got := balanceOf(t, pool, "acc-alice"); got != aliceBefore-3000 {
		t.Errorf("alice balance = %d, want exactly one debit of 3000", got)
	}
	if got := ledgerCount(t, pool, first.GetPaymentId()); got != 2 {
		t.Errorf("ledger entries = %d, want 2; the retry must not write a second pair", got)
	}
}

// Every payment in the table must have a balanced pair. This is the global invariant the
// whole ledger exists to maintain.
func TestEveryPaymentBalancesGlobally(t *testing.T) {
	svc, pool, _, _, _ := newRelayerHarness(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := svc.CreatePayment(ctx, newRequest(t,
			fmt.Sprintf("global-%d-%s", i, uuid.New().String()), int64(1000+i*100))); err != nil {
			t.Fatalf("payment %d: %v", i, err)
		}
	}

	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var unbalanced int
	if err := pool.QueryRow(checkCtx, `
		SELECT count(*) FROM (
			SELECT l.payment_id
			FROM ledger_entries l
			GROUP BY l.payment_id
			HAVING SUM(CASE WHEN l.direction = 'DEBIT' THEN -l.amount_cents ELSE l.amount_cents END) <> 0
			   OR COUNT(*) <> 2
		) bad`).Scan(&unbalanced); err != nil {
		t.Fatalf("global balance check: %v", err)
	}
	if unbalanced != 0 {
		t.Errorf("%d payment(s) have unbalanced or incomplete ledger pairs", unbalanced)
	}
}
