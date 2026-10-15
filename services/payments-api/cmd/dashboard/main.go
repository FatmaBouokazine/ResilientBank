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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
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

func page(s status) string {
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
<title>ResilientBank — Live Status</title>
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
</style>
</head>
<body>
<h1>ResilientBank — Live Status</h1>
<div class="sub">Refreshes every 3 seconds. Plain words, live numbers.</div>
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

func main() {
	api := envOr("PAYMENTS_METRICS", "http://localhost:2112/metrics")
	relayer := envOr("RELAYER_METRICS", "http://localhost:2113/metrics")
	addr := envOr("DASHBOARD_ADDR", ":2114")

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, page(collect(api, relayer)))
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
