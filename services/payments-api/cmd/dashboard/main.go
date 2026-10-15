// Command dashboard serves a plain-language status page for demonstrations.
//
// It reads the Prometheus endpoints of the payment service and the outbox relayer and
// renders them as a human-readable dashboard: payments processed, events delivered,
// backlog depth, and dead-letter count. No metric names, no protocol vocabulary.
//
// Run it while the stack is up:
//
//	go run ./cmd/dashboard
//
// Then open http://localhost:2114 in a browser.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	paymentsv1 "github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/gen/payments/v1"
)

// Fetches a single gauge or counter value from a Prometheus text endpoint.
func scrapeValue(endpoint, metric string) (float64, bool) {
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(endpoint)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, false
	}

	// A labelled series looks like: name{labels} value
	// An unlabelled one looks like: name value
	// Sum every matching series so counters with several outcomes still report a total.
	var total float64
	found := false
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, metric+"{") && !strings.HasPrefix(line, metric+" ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		total += v
		found = true
	}
	return total, found
}

func labelValue(endpoint, metric, label, want string) (float64, bool) {
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(endpoint)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, false
	}

	prefix := metric + "{"
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		// Extract the label block and the value.
		close := strings.Index(line, "}")
		if close < 0 {
			continue
		}
		labels := line[len(prefix):close]
		fields := strings.Fields(line[close+1:])
		if len(fields) != 1 {
			continue
		}
		for _, kv := range strings.Split(labels, ",") {
			parts := strings.SplitN(strings.TrimSpace(kv), "=", 2)
			if len(parts) == 2 && parts[0] == label && strings.Trim(parts[1], `"`) == want {
				v, err := strconv.ParseFloat(fields[0], 64)
				if err == nil {
					return v, true
				}
			}
		}
	}
	return 0, false
}

type status struct {
	PaymentsCreated   float64
	PaymentsDuplicate float64
	EventsPublished   float64
	EventsRetrying    float64
	BacklogPending    float64
	BacklogOldestSec  float64
	DeadLettered      float64

	APIUp     bool
	RelayerUp bool
}

func collect(api, relayer string) status {
	s := status{}

	_, err := http.Get(api)
	s.APIUp = err == nil
	_, err = http.Get(relayer)
	s.RelayerUp = err == nil

	if v, ok := labelValue(api, "resilientbank_payments_total", "result", "created"); ok {
		s.PaymentsCreated = v
	}
	if v, ok := labelValue(api, "resilientbank_payments_total", "result", "duplicate"); ok {
		s.PaymentsDuplicate = v
	}
	if v, ok := labelValue(relayer, "resilientbank_outbox_published_total", "result", "published"); ok {
		s.EventsPublished = v
	}
	if v, ok := labelValue(relayer, "resilientbank_outbox_published_total", "result", "retry"); ok {
		s.EventsRetrying = v
	}
	if v, ok := scrapeValue(relayer, "resilientbank_outbox_pending"); ok {
		s.BacklogPending = v
	}
	if v, ok := scrapeValue(relayer, "resilientbank_outbox_oldest_pending_age_seconds"); ok {
		s.BacklogOldestSec = v
	}
	if v, ok := scrapeValue(relayer, "resilientbank_outbox_dead_letter_total"); ok {
		s.DeadLettered = v
	}
	return s
}

func dot(up bool) string {
	if up {
		return `<span class="dot ok"></span>`
	}
	return `<span class="dot down"></span>`
}

func healthClass(s status) (string, string) {
	switch {
	case !s.APIUp:
		return "down", "Payment service is not responding."
	case s.DeadLettered > 0:
		return "warn", fmt.Sprintf("%.0f event(s) needed human attention in the dead-letter queue.", s.DeadLettered)
	case !s.RelayerUp && s.BacklogPending > 0:
		return "warn", "Delivery worker is stopped; messages are waiting safely."
	case !s.RelayerUp:
		return "warn", "Delivery worker is stopped. Payments still work."
	case s.BacklogPending > 0:
		return "ok", fmt.Sprintf("%.0f message(s) queued for delivery.", s.BacklogPending)
	default:
		return "ok", "Everything processed. Nothing waiting, nothing lost."
	}
}

func pageStatus(s status) string {
	class, message := healthClass(s)

	formatAge := func(sec float64) string {
		if sec < 60 {
			return fmt.Sprintf("%.0fs", sec)
		}
		return fmt.Sprintf("%.0fm", sec/60)
	}

	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="3">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ResilientBank ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Live Status</title>
<style>
  body { font-family: system-ui, -apple-system, sans-serif; max-width: 720px; margin: 2em auto; padding: 0 1em; color: #1a1a1a; background: #fafafa; }
  h1 { font-size: 1.6em; margin-bottom: 0.2em; }
  .sub { color: #666; margin-bottom: 1.5em; }
  .banner { padding: 1em 1.2em; border-radius: 8px; margin-bottom: 1.5em; font-size: 1.1em; }
  .banner.ok { background: #e6f4ea; border: 1px solid #34a853; }
  .banner.warn { background: #fef7e0; border: 1px solid #f9ab00; }
  .banner.down { background: #fce8e6; border: 1px solid #d93025; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1em; }
  .card { background: white; border: 1px solid #ddd; border-radius: 8px; padding: 1em 1.2em; }
  .card .num { font-size: 2em; font-weight: 700; }
  .card .lbl { color: #555; margin-top: 0.3em; }
  .row { display: flex; align-items: center; gap: 0.5em; margin: 0.6em 0; }
  .dot { width: 12px; height: 12px; border-radius: 50%%; display: inline-block; }
  .dot.ok { background: #34a853; }
  .dot.down { background: #d93025; }
  .foot { color: #888; font-size: 0.85em; margin-top: 2em; }
  /* Stripe-style checkout */
  .co-wrap { display: grid; grid-template-columns: 1.2fr 1fr; gap: 1.2em; margin-top: 0.5em; }
  @media (max-width: 640px) { .co-wrap { grid-template-columns: 1fr; } }
  .co-form { background: white; border: 1px solid #e3e8ee; border-radius: 8px; padding: 1.4em; box-shadow: 0 2px 8px rgba(60,66,87,0.08); }
  .co-title { font-size: 1.25em; font-weight: 600; color: #32325d; margin-bottom: 1em; }
  .co-label { display: block; font-size: 0.85em; font-weight: 500; color: #424770; margin-bottom: 0.6em; }
  .co-input { display: block; width: 100%%; box-sizing: border-box; padding: 0.65em 0.75em; font-size: 1em; color: #32325d; background: white; border: 1px solid #e3e8ee; border-radius: 6px; outline: none; transition: border-color 0.15s, box-shadow 0.15s; }
  .co-input:focus { border-color: #635bff; box-shadow: 0 0 0 3px rgba(99,91,255,0.15); }
  .co-input::placeholder { color: #a3acb9; }
  .co-box { border: 1px solid #e3e8ee; border-radius: 6px; overflow: hidden; margin-bottom: 0.6em; }
  .co-box .co-input { border: none; border-radius: 0; }
  .co-box .co-top { border-bottom: 1px solid #e3e8ee; }
  .co-row2 { display: grid; grid-template-columns: 1fr 1fr; gap: 0.7em; }
  .co-pay { width: 100%%; margin-top: 1em; padding: 0.75em; font-size: 1.05em; font-weight: 600; color: white; background: #635bff; border: none; border-radius: 6px; cursor: pointer; transition: background 0.15s; }
  .co-pay:hover { background: #4f46e5; }
  .co-secure { margin-top: 0.9em; font-size: 0.8em; color: #8792a2; text-align: center; }
  .co-summary { background: #f6f9fc; border: 1px solid #e3e8ee; border-radius: 8px; padding: 1.4em; align-self: start; }
  .co-sum-title { font-weight: 600; color: #32325d; margin-bottom: 1em; }
  .co-line { display: flex; justify-content: space-between; color: #525f7f; margin-bottom: 0.6em; }
  .co-total { display: flex; justify-content: space-between; font-weight: 700; color: #32325d; border-top: 1px solid #e3e8ee; padding-top: 0.8em; margin-top: 0.8em; }
</style>
</head>
<body>
<h1>ResilientBank ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Live Status</h1>
<div class="sub">Refreshes every 3 seconds. Plain words, live numbers. <a href="/checkout">Send a payment</a></div>
<div class="banner %s">%s</div>
<div class="grid">
  <div class="card"><div class="num">%.0f</div><div class="lbl">payments recorded</div></div>
  <div class="card"><div class="num">%.0f</div><div class="lbl">duplicate attempts safely ignored</div></div>
  <div class="card"><div class="num">%.0f</div><div class="lbl">notifications delivered</div></div>
  <div class="card"><div class="num">%.0f</div><div class="lbl">messages waiting to be delivered</div></div>
  <div class="card"><div class="num">%s</div><div class="lbl">oldest waiting message</div></div>
  <div class="card"><div class="num">%.0f</div><div class="lbl">messages needing human attention</div></div>
</div>
<h2 style="margin-top:1.5em">Services</h2>
<div class="row">%s Payment service (takes your money safely)</div>
<div class="row">%s Delivery worker (tells everyone about it)</div>

<div class="foot">Numbers come straight from the running system. Kill the broker and watch "waiting" grow, then drain to zero on recovery.</div>
</body>
</html>`,
		class, message,
		s.PaymentsCreated,
		s.PaymentsDuplicate,
		s.EventsPublished,
		s.BacklogPending,
		formatAge(s.BacklogOldestSec),
		s.DeadLettered,
		dot(s.APIUp), dot(s.RelayerUp),
	)
}

// pageCheckout renders the standalone Stripe-style payment page.
//
// Separated from the status page so a demonstration can open one screen for "send money"
// and another for "watch what happens". Both read the same backend; neither duplicates
// payment logic.
func pageCheckout(payHTML string) string {
	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ResilientBank Ã¢â‚¬â€ Checkout</title>
<style>
  body { font-family: system-ui, -apple-system, sans-serif; max-width: 720px; margin: 2em auto; padding: 0 1em; color: #1a1a1a; background: #fafafa; }
  a { color: #635bff; }
  .banner { padding: 1em 1.2em; border-radius: 8px; margin-bottom: 1.5em; font-size: 1.1em; }
  .banner.ok { background: #e6f4ea; border: 1px solid #34a853; }
  .banner.down { background: #fce8e6; border: 1px solid #d93025; }
  .co-wrap { display: grid; grid-template-columns: 1.2fr 1fr; gap: 1.2em; margin-top: 0.5em; }
  @media (max-width: 640px) { .co-wrap { grid-template-columns: 1fr; } }
  .co-form { background: white; border: 1px solid #e3e8ee; border-radius: 8px; padding: 1.4em; box-shadow: 0 2px 8px rgba(60,66,87,0.08); }
  .co-title { font-size: 1.25em; font-weight: 600; color: #32325d; margin-bottom: 1em; }
  .co-label { display: block; font-size: 0.85em; font-weight: 500; color: #424770; margin-bottom: 0.6em; }
  .co-input { display: block; width: 100%%; box-sizing: border-box; padding: 0.65em 0.75em; font-size: 1em; color: #32325d; background: white; border: 1px solid #e3e8ee; border-radius: 6px; outline: none; transition: border-color 0.15s, box-shadow 0.15s; }
  .co-input:focus { border-color: #635bff; box-shadow: 0 0 0 3px rgba(99,91,255,0.15); }
  .co-input::placeholder { color: #a3acb9; }
  .co-box { border: 1px solid #e3e8ee; border-radius: 6px; overflow: hidden; margin-bottom: 0.6em; }
  .co-box .co-input { border: none; border-radius: 0; }
  .co-box .co-top { border-bottom: 1px solid #e3e8ee; }
  .co-row2 { display: grid; grid-template-columns: 1fr 1fr; gap: 0.7em; }
  .co-pay { width: 100%%; margin-top: 1em; padding: 0.75em; font-size: 1.05em; font-weight: 600; color: white; background: #635bff; border: none; border-radius: 6px; cursor: pointer; transition: background 0.15s; }
  .co-pay:hover { background: #4f46e5; }
  .co-secure { margin-top: 0.9em; font-size: 0.8em; color: #8792a2; text-align: center; }
  .co-summary { background: #f6f9fc; border: 1px solid #e3e8ee; border-radius: 8px; padding: 1.4em; align-self: start; }
  .co-sum-title { font-weight: 600; color: #32325d; margin-bottom: 1em; }
  .co-line { display: flex; justify-content: space-between; color: #525f7f; margin-bottom: 0.6em; }
  .co-total { display: flex; justify-content: space-between; font-weight: 700; color: #32325d; border-top: 1px solid #e3e8ee; padding-top: 0.8em; margin-top: 0.8em; }
  .foot { color: #888; font-size: 0.85em; margin-top: 2em; }
</style>
</head>
<body>
<h1>ResilientBank Ã¢â‚¬â€ Checkout</h1>
<div class="sub"><a href="/">Back to live status</a></div>
%s
<div class="co-wrap">
  <form method="POST" action="/pay" class="co-form" id="payform">
    <div class="co-title">Pay with ResilientBank</div>
    <label class="co-label">Email
      <input class="co-input" name="email" type="email" value="alice@example.com" required autocomplete="email">
    </label>
    <div class="co-label">Transfer details</div>
    <div class="co-box">
      <input class="co-input co-top" name="from" value="acc-alice" required placeholder="From account" autocomplete="off">
      <input class="co-input co-bottom" name="to" value="acc-bob" required placeholder="To account" autocomplete="off">
    </div>
    <div class="co-row2">
      <label class="co-label">Amount (cents)
        <input class="co-input" name="amount" value="5000" required pattern="[0-9]+" inputmode="numeric" id="amountInput">
      </label>
      <label class="co-label">Currency
        <input class="co-input" name="currency" value="EUR" required maxlength="3" style="text-transform:uppercase" id="currencyInput">
      </label>
    </div>
    <button type="submit" class="co-pay" id="payBtn">Pay</button>
    <div class="co-secure">Ã°Å¸â€â€™ Payments are recorded atomically. Duplicate references return the original payment, never a second charge.</div>
  </form>
  <div class="co-summary">
    <div class="co-sum-title">Order summary</div>
    <div class="co-line"><span>Transfer</span><span id="sumAmount">Ã¢â€šÂ¬50.00</span></div>
    <div class="co-line"><span>Fee</span><span>Ã¢â€šÂ¬0.00</span></div>
    <div class="co-total"><span>Total due</span><span id="sumTotal">Ã¢â€šÂ¬50.00</span></div>
  </div>
</div>
<script>
(function(){
  var a=document.getElementById('amountInput'), c=document.getElementById('currencyInput'),
      sa=document.getElementById('sumAmount'), st=document.getElementById('sumTotal'), b=document.getElementById('payBtn');
  var sym={EUR:'\u20AC',USD:'$',GBP:'\u00A3'};
  function fmt(){ var v=parseInt(a.value||'0',10)/100, s=sym[(c.value||'').toUpperCase()]||((c.value||'').toUpperCase()+' ');
    var t=s+v.toFixed(2); sa.textContent=t; st.textContent=t; b.textContent='Pay '+t; }
  a.addEventListener('input',fmt); c.addEventListener('input',fmt); fmt();
})();
</script>
<div class="foot"><a href="/">Back to live status</a></div>
</body>
</html>`, payHTML)
}

// payResult renders the outcome of the last submitted payment, if any.
func payResultHTML(ok bool, id, errMsg string) string {
	if id == "" && errMsg == "" {
		return ""
	}
	if ok {
		return fmt.Sprintf(`<div class="banner ok" style="margin-bottom:1em">Payment recorded: <b>%s</b></div>`, id)
	}
	return fmt.Sprintf(`<div class="banner down" style="margin-bottom:1em">Payment failed: %s</div>`, errMsg)
}

func main() {
	api := envOr("PAYMENTS_METRICS", "http://localhost:2112/metrics")
	relayer := envOr("RELAYER_METRICS", "http://localhost:2113/metrics")
	grpcAddr := envOr("PAYMENTS_GRPC", "localhost:50051")
	addr := envOr("DASHBOARD_ADDR", ":2114")

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, pageStatus(collect(api, relayer)))
	})
	http.HandleFunc("/checkout", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		var payHTML string
		if id := r.URL.Query().Get("paid"); id != "" {
			payHTML = payResultHTML(true, id, "")
		} else if msg := r.URL.Query().Get("err"); msg != "" {
			payHTML = payResultHTML(false, "", msg)
		}
		fmt.Fprint(w, pageCheckout(payHTML))
	})
	http.HandleFunc("/pay", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
		handlePay(w, r, grpcAddr, logger)
	})
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	logger.Info("dashboard serving", "addr", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		logger.Error("dashboard error", "error", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// handlePay accepts the payment form and forwards it to the payment service over gRPC.
//
// Each submission gets a fresh idempotency key so every click is a genuinely new payment.
// To demonstrate duplicate handling, submit twice quickly is unnecessary: the smoke suite
// covers that path. This form is for creating visible traffic, not for testing idempotency.
func handlePay(w http.ResponseWriter, r *http.Request, grpcAddr string, logger *slog.Logger) {
	if err := r.ParseForm(); err != nil {
		redirectErr(w, r, "could not read the form")
		return
	}

	from := strings.TrimSpace(r.FormValue("from"))
	to := strings.TrimSpace(r.FormValue("to"))
	amount, err := strconv.ParseInt(strings.TrimSpace(r.FormValue("amount")), 10, 64)
	currency := strings.ToUpper(strings.TrimSpace(r.FormValue("currency")))
	if from == "" || to == "" || err != nil || amount <= 0 || len(currency) != 3 {
		redirectErr(w, r, "fill every field: distinct accounts, a positive amount, a 3-letter currency")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		logger.Warn("dashboard could not reach the payment service", "addr", grpcAddr, "error", err)
		redirectErr(w, r, "payment service is not reachable (is `make run` up?)")
		return
	}
	defer conn.Close()

	client := paymentsv1.NewPaymentServiceClient(conn)
	resp, err := client.CreatePayment(ctx, &paymentsv1.CreatePaymentRequest{
		IdempotencyKey: "dashboard-" + uuid.New().String(),
		FromAccount:    from,
		ToAccount:      to,
		AmountCents:    amount,
		Currency:       currency,
	})
	if err != nil {
		logger.Warn("CreatePayment failed", "error", err)
		redirectErr(w, r, "the payment was rejected: "+grpcMessage(err))
		return
	}

	http.Redirect(w, r, "/checkout?paid="+url.QueryEscape(resp.GetPaymentId()), http.StatusSeeOther)
}

func redirectErr(w http.ResponseWriter, r *http.Request, msg string) {
	http.Redirect(w, r, "/checkout?err="+url.QueryEscape(msg), http.StatusSeeOther)
}

// grpcMessage extracts the human-readable part of a gRPC error.
func grpcMessage(err error) string {
	msg := err.Error()
	// Trim the "rpc error: code = X desc = " prefix down to the description.
	if idx := strings.Index(msg, "desc = "); idx >= 0 {
		return msg[idx+len("desc = "):]
	}
	return msg
}
