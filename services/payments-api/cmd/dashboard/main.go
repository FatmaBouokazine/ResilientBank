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
	"encoding/json"
	"fmt"
	"html"
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

type metricSample struct {
	name   string
	labels map[string]string
	value  float64
}

// Reads each Prometheus endpoint once per refresh instead of issuing a request for every card.
func scrapeMetrics(endpoint string) ([]metricSample, bool) {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(endpoint)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return nil, false
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, false
	}

	var samples []metricSample
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name, labels := fields[0], map[string]string{}
		if open := strings.IndexByte(name, '{'); open >= 0 {
			close := strings.LastIndexByte(name, '}')
			if close <= open {
				continue
			}
			for _, pair := range strings.Split(name[open+1:close], ",") {
				parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
				if len(parts) == 2 {
					labels[parts[0]] = strings.Trim(parts[1], `"`)
				}
			}
			name = name[:open]
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err == nil {
			samples = append(samples, metricSample{name: name, labels: labels, value: value})
		}
	}
	return samples, true
}

func metricValue(samples []metricSample, metric, label, want string) (float64, bool) {
	var total float64
	found := false
	for _, sample := range samples {
		if sample.name != metric {
			continue
		}
		if label != "" && sample.labels[label] != want {
			continue
		}
		total += sample.value
		found = true
	}
	return total, found
}

type status struct {
	PaymentsCreated   float64
	PaymentsDuplicate float64
	EventsPublished   float64
	EventsRetrying    float64
	BacklogPending    float64
	BacklogOldestSec  float64
	DeadLettered      float64

	APIUp         bool
	RelayerUp     bool
	HealthMessage string
}

func collect(api, relayer string) status {
	s := status{}

	apiMetrics, apiUp := scrapeMetrics(api)
	relayerMetrics, relayerUp := scrapeMetrics(relayer)
	s.APIUp, s.RelayerUp = apiUp, relayerUp

	s.PaymentsCreated, _ = metricValue(apiMetrics, "resilientbank_payments_total", "result", "created")
	s.PaymentsDuplicate, _ = metricValue(apiMetrics, "resilientbank_payments_total", "result", "duplicate")
	s.EventsPublished, _ = metricValue(relayerMetrics, "resilientbank_outbox_published_total", "result", "published")
	s.EventsRetrying, _ = metricValue(relayerMetrics, "resilientbank_outbox_published_total", "result", "retry")
	s.BacklogPending, _ = metricValue(relayerMetrics, "resilientbank_outbox_pending", "", "")
	s.BacklogOldestSec, _ = metricValue(relayerMetrics, "resilientbank_outbox_oldest_pending_age_seconds", "", "")
	s.DeadLettered, _ = metricValue(relayerMetrics, "resilientbank_outbox_dead_letter_total", "", "")
	_, s.HealthMessage = healthClass(s)
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
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" content="#f6f5f2"><title>ResilientBank | Operations</title>
<style>
:root{color-scheme:light;--ink:#171717;--muted:#73716d;--line:#e6e3de;--paper:#f6f5f2;--surface:#fff;--accent:#fd5108;--green:#21845a;--red:#c94444;--amber:#b36c16}
*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);font:15px/1.5 Arial,Helvetica,sans-serif}a{color:inherit;text-decoration:none}.shell{min-height:100vh}.sidebar{position:fixed;inset:0 auto 0 0;width:244px;background:#fff;border-right:1px solid var(--line);padding:26px 18px;display:flex;flex-direction:column}.brand{display:flex;align-items:center;gap:11px;padding:0 9px 34px;font-weight:750;font-size:17px;letter-spacing:-.4px}.brandmark{display:grid;place-items:center;width:34px;height:34px;background:var(--accent);color:#fff;font-weight:800}.nav-label{padding:0 10px 9px;color:#99958e;font-size:10px;font-weight:700;letter-spacing:1.2px;text-transform:uppercase}.nav-link{display:flex;align-items:center;gap:11px;padding:11px 12px;border-radius:7px;color:#5e5b56;font-weight:600;font-size:13px}.nav-link.active{background:#fff2eb;color:#bf3d08}.nav-icon{width:18px;text-align:center;font-size:16px}.sidebar-bottom{margin-top:auto;padding:15px 11px 0;border-top:1px solid var(--line);color:var(--muted);font-size:12px}.live-dot{display:inline-block;width:8px;height:8px;margin-right:7px;border-radius:50%%;background:var(--green)}main{margin-left:244px;padding:26px clamp(24px,4vw,58px) 52px;max-width:1600px}.topbar{display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--line);padding-bottom:19px;margin-bottom:31px}.crumb{font-size:12px;color:var(--muted)}.top-actions{display:flex;align-items:center;gap:16px}.refresh{font-size:12px;color:var(--muted)}.avatar{display:grid;place-items:center;width:34px;height:34px;border-radius:50%%;background:#e9e2d9;font-size:12px;font-weight:700}.intro{display:flex;align-items:flex-end;justify-content:space-between;gap:22px;margin-bottom:25px}.kicker{margin:0 0 5px;color:var(--muted);font-size:12px;font-weight:600}.intro h1{font-size:30px;letter-spacing:-1.1px;line-height:1.2;margin:0}.intro p{margin:8px 0 0;color:var(--muted);font-size:13px}.button{display:inline-flex;align-items:center;justify-content:center;gap:8px;background:var(--accent);border:0;border-radius:5px;padding:12px 17px;color:white;font-size:13px;font-weight:700;white-space:nowrap}.button:hover{background:#de4607}.banner{display:flex;align-items:center;gap:13px;background:#fff;border:1px solid var(--line);border-left:4px solid var(--green);border-radius:7px;padding:15px 17px;margin-bottom:20px}.banner.warn{border-left-color:var(--amber)}.banner.down{border-left-color:var(--red)}.state-mark{display:grid;place-items:center;width:30px;height:30px;border-radius:50%%;background:#eaf5ef;color:var(--green);font-weight:800}.warn .state-mark{background:#fff5e8;color:var(--amber)}.down .state-mark{background:#fceeee;color:var(--red)}.banner strong{display:block;font-size:13px}.banner span{display:block;color:var(--muted);font-size:12px;margin-top:2px}.section-head{display:flex;align-items:center;justify-content:space-between;margin:26px 0 12px}.section-head h2{font-size:15px;letter-spacing:-.2px;margin:0}.section-head small{font-size:11px;color:var(--muted)}.metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:13px}.metric,.panel{background:var(--surface);border:1px solid var(--line);border-radius:8px}.metric{padding:17px 17px 16px;min-height:120px}.metric-label{font-size:11px;color:var(--muted);font-weight:600}.metric-value{font-size:27px;letter-spacing:-1px;font-weight:700;margin-top:12px}.metric-foot{font-size:10px;color:var(--muted);margin-top:3px}.content-grid{display:grid;grid-template-columns:minmax(0,1.35fr) minmax(260px,.85fr);gap:14px}.panel{padding:19px}.panel-title{font-size:13px;font-weight:700}.panel-sub{font-size:11px;color:var(--muted);margin-top:3px}.flow{display:flex;align-items:center;gap:12px;margin-top:23px;padding:15px;background:#faf9f7;border-radius:6px}.flow-node{flex:1;min-width:0}.flow-name{font-size:12px;font-weight:700}.flow-desc{font-size:10px;color:var(--muted);margin-top:3px}.flow-arrow{color:#aaa;font-size:18px}.service{display:flex;align-items:center;justify-content:space-between;padding:13px 0;border-bottom:1px solid #f0eeea}.service:last-child{border-bottom:0;padding-bottom:0}.service-name{font-size:12px;font-weight:600}.service-copy{font-size:10px;color:var(--muted);margin-top:2px}.pill{display:inline-flex;align-items:center;gap:6px;border-radius:20px;background:#edf6f0;color:#286f4e;padding:5px 9px;font-size:10px;font-weight:700}.pill.down{background:#fceeee;color:var(--red)}.pill .live-dot{width:6px;height:6px;margin:0}.queue{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:16px}.queue-item{padding:13px;background:#faf9f7;border-radius:6px}.queue-num{font-size:18px;font-weight:700}.queue-copy{color:var(--muted);font-size:10px;margin-top:3px}.activity{margin-top:14px;display:flex;gap:12px;align-items:flex-start;border-top:1px solid var(--line);padding-top:14px;color:var(--muted);font-size:11px}.activity-mark{color:var(--accent);font-weight:800}.footer{margin-top:23px;color:#8c8983;font-size:10px;display:flex;justify-content:space-between}
@media(max-width:1050px){.sidebar{width:205px}main{margin-left:205px}.metrics{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:720px){.sidebar{position:static;width:auto;height:auto;padding:15px 18px;border-right:0;border-bottom:1px solid var(--line)}.brand{padding:0}.nav-label,.nav-link,.sidebar-bottom{display:none}main{margin:0;padding:20px 17px 35px}.topbar{margin-bottom:24px}.intro{align-items:flex-start;flex-direction:column}.intro h1{font-size:27px}.content-grid{grid-template-columns:1fr}.metrics{gap:9px}.metric{padding:14px;min-height:108px}.metric-value{font-size:23px}.flow{gap:7px;padding:12px}.flow-desc{font-size:9px}.footer{gap:12px;flex-direction:column}}
</style></head><body><div class="shell">
<aside class="sidebar"><a class="brand" href="/"><span class="brandmark">R</span>ResilientBank</a><div class="nav-label">Workspace</div><a class="nav-link active" href="/"><span class="nav-icon">◫</span>Overview</a><a class="nav-link" href="/checkout"><span class="nav-icon">↗</span>Payments</a><div class="sidebar-bottom"><span class="live-dot"></span>Live system overview</div></aside>
<main><div class="topbar"><div class="crumb">Workspace&nbsp; / &nbsp;Overview</div><div class="top-actions"><span class="refresh">Live updates · every 4 seconds</span><span class="avatar">RB</span></div></div>
<section class="intro"><div><p class="kicker">MONITORING</p><h1>System overview</h1><p>Your payments and delivery systems at a glance.</p></div><a class="button" href="/checkout">＋&nbsp; Create payment</a></section>
<div id="healthBanner" class="banner %s"><div class="state-mark">✓</div><div><strong id="healthTitle">%s</strong><span id="healthCopy">System health is based on live service signals.</span></div></div>
<div class="section-head"><h2>Payment activity</h2><small>Since service start · live counters</small></div><section class="metrics">
<article class="metric"><div class="metric-label">Payments accepted</div><div class="metric-value" data-metric="payments">%.0f</div><div class="metric-foot">Successfully recorded</div></article>
<article class="metric"><div class="metric-label">Safe retries</div><div class="metric-value" data-metric="duplicates">%.0f</div><div class="metric-foot">Duplicates not charged again</div></article>
<article class="metric"><div class="metric-label">Messages delivered</div><div class="metric-value" data-metric="published">%.0f</div><div class="metric-foot">Payment notifications sent</div></article>
<article class="metric"><div class="metric-label">Needs attention</div><div class="metric-value" data-metric="dead">%.0f</div><div class="metric-foot">Messages set aside for review</div></article>
</section>
<div class="section-head"><h2>System health</h2><small>Service availability and delivery</small></div><section class="content-grid"><article class="panel"><div class="panel-title">Payment lifecycle</div><div class="panel-sub">Every accepted payment is safely recorded before delivery.</div><div class="flow"><div class="flow-node"><div class="flow-name">Payment service</div><div class="flow-desc">Records the transfer</div></div><span class="flow-arrow">→</span><div class="flow-node"><div class="flow-name">Durable queue</div><div class="flow-desc">Holds messages safely</div></div><span class="flow-arrow">→</span><div class="flow-node"><div class="flow-name">Delivery worker</div><div class="flow-desc">Notifies connected services</div></div></div>
<div class="queue"><div class="queue-item"><div class="queue-num" data-metric="pending">%.0f</div><div class="queue-copy">Messages waiting for delivery</div></div><div class="queue-item"><div class="queue-num" data-metric="age">%s</div><div class="queue-copy">Age of oldest waiting message</div></div></div>
<div class="activity"><span class="activity-mark">●</span><span>When delivery is interrupted, messages wait safely and continue when service returns. Payments remain recorded.</span></div></article>
<article class="panel"><div class="panel-title">Services</div><div class="panel-sub">Current connection status</div><div class="service"><div><div class="service-name">Payment processing</div><div class="service-copy">Accepts and records transfers</div></div><span id="apiStatus" class="pill"><span class="live-dot"></span>%s</span></div><div class="service"><div><div class="service-name">Message delivery</div><div class="service-copy">Delivers payment notifications</div></div><span id="relayerStatus" class="pill"><span class="live-dot"></span>%s</span></div><div class="activity"><span class="activity-mark">↻</span><span>Metrics update automatically. Use the payment form to send a test transfer.</span></div></article></section>
<div class="footer"><span>ResilientBank · Operations overview</span><span>Demo environment · No card details are collected</span></div></main></div>
<script>
(function(){function update(){fetch('/status.json',{cache:'no-store'}).then(function(r){if(!r.ok)throw Error('status');return r.json()}).then(function(s){var vals={payments:s.PaymentsCreated,duplicates:s.PaymentsDuplicate,published:s.EventsPublished,dead:s.DeadLettered,pending:s.BacklogPending,age:s.BacklogOldestSec<60?Math.round(s.BacklogOldestSec)+'s':Math.round(s.BacklogOldestSec/60)+'m'};Object.keys(vals).forEach(function(k){document.querySelectorAll('[data-metric="'+k+'"]').forEach(function(e){e.textContent=typeof vals[k]==='number'?Math.round(vals[k]).toLocaleString():vals[k]})});var b=document.getElementById('healthBanner'),title=document.getElementById('healthTitle'),copy=document.getElementById('healthCopy');var down=!s.APIUp,warn=!down&&(!s.RelayerUp||s.DeadLettered>0);b.className='banner '+(down?'down':warn?'warn':'');title.textContent=down?'Payment service unavailable':warn?'Attention needed':'All systems operational';copy.textContent=s.HealthMessage;[['apiStatus',s.APIUp],['relayerStatus',s.RelayerUp]].forEach(function(x){var e=document.getElementById(x[0]);e.className='pill'+(x[1]?'':' down');e.innerHTML='<span class="live-dot"></span>'+(x[1]?'Operational':'Unavailable')})}).catch(function(){})}setInterval(update,4000)})();
</script></body></html>`,
		class, html.EscapeString(message), s.PaymentsCreated, s.PaymentsDuplicate,
		s.EventsPublished, s.DeadLettered, s.BacklogPending, formatAge(s.BacklogOldestSec),
		statusLabel(s.APIUp), statusLabel(s.RelayerUp))
}

func statusLabel(up bool) string {
	if up {
		return "Operational"
	}
	return "Unavailable"
}

// pageCheckout renders the standalone Stripe-style payment page.
//
// Separated from the status page so a demonstration can open one screen for "send money"
// and another for "watch what happens". Both read the same backend; neither duplicates
// payment logic.
// pageCheckout renders the payment page in the PwC Clear Editorial system:
// white surfaces, black editorial type, square inputs, and a single vivid orange
// action. The backend fields are unchanged; only the presentation follows the tokens.
func pageCheckout(payHTML, idempotencyKey, from, to, amount, currency string) string {
	return fmt.Sprintf(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#f6f5f2"><title>ResilientBank | New payment</title>
<style>
:root{--ink:#171717;--muted:#77746f;--line:#e6e3de;--paper:#f6f5f2;--accent:#fd5108;--green:#21845a;--red:#c94444}*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);font:14px/1.5 Arial,Helvetica,sans-serif}a{color:inherit;text-decoration:none}.top{height:68px;background:#fff;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between;padding:0 max(22px,calc((100vw - 1120px)/2))}.brand{display:flex;align-items:center;gap:10px;font-weight:750;font-size:15px}.brandmark{display:grid;place-items:center;width:32px;height:32px;background:var(--accent);color:#fff;font-weight:800}.top-link{font-size:12px;color:var(--muted)}main{max-width:1050px;margin:0 auto;padding:46px 22px 70px}.crumb{font-size:11px;color:var(--muted);margin-bottom:22px}.crumb a:hover{color:var(--ink)}.heading{margin-bottom:24px}.eyebrow{font-size:10px;letter-spacing:1.3px;font-weight:700;color:#a19c94;margin:0 0 7px}.heading h1{font-size:30px;letter-spacing:-1px;line-height:1.2;margin:0}.heading p{font-size:13px;color:var(--muted);margin:8px 0 0}.notice{margin:0 0 18px;padding:14px 16px;background:#fff;border:1px solid var(--line);border-left:4px solid var(--green);border-radius:6px;font-size:12px}.notice.error{border-left-color:var(--red);color:#873333}.layout{display:grid;grid-template-columns:minmax(0,1.2fr) minmax(270px,.8fr);gap:17px;align-items:start}.card{background:#fff;border:1px solid var(--line);border-radius:8px;padding:24px}.card-head{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:23px}.card-title{font-size:15px;font-weight:700}.card-caption{font-size:11px;color:var(--muted);margin-top:4px}.step{font-size:10px;color:var(--muted);background:#f6f5f2;border-radius:20px;padding:5px 9px}.field{margin-bottom:17px}.field label{display:block;font-size:11px;font-weight:700;margin-bottom:6px}.hint{font-size:10px;color:var(--muted);font-weight:400;margin-left:4px}.input{width:100%%;height:43px;border:1px solid #d8d5cf;background:white;border-radius:5px;padding:0 12px;color:var(--ink);font:13px Arial,sans-serif;outline:none}.input:focus{border-color:var(--accent);box-shadow:0 0 0 3px #fd51081c}.input::placeholder{color:#aaa69f}.columns{display:grid;grid-template-columns:1fr 1fr;gap:12px}.amount-wrap{position:relative}.amount-wrap .input{height:49px;padding-left:34px;font-size:17px;font-weight:600}.currency-mark{position:absolute;left:13px;top:13px;color:var(--muted);font-weight:700}.field-note{font-size:10px;color:var(--muted);margin-top:6px}.details{border-top:1px solid var(--line);margin-top:21px;padding-top:18px}.summary{position:sticky;top:20px}.summary-total{display:flex;align-items:center;justify-content:space-between;padding:17px 0 20px;border-bottom:1px solid var(--line)}.summary-total span:first-child{font-size:11px;color:var(--muted)}.summary-total strong{font-size:23px;letter-spacing:-.6px}.summary-row{display:flex;justify-content:space-between;font-size:11px;padding-top:13px;color:#555}.summary-row strong{font-weight:600;color:var(--ink)}.submit{width:100%%;height:46px;margin-top:22px;border:0;border-radius:5px;background:var(--accent);color:white;font-size:13px;font-weight:700;cursor:pointer}.submit:hover{background:#df4607}.submit:disabled{opacity:.65;cursor:wait}.security{margin-top:13px;text-align:center;color:var(--muted);font-size:10px}.security span{color:var(--green);font-weight:700}.summary-foot{margin-top:22px;padding:13px;background:#f8f7f4;border-radius:5px;color:var(--muted);font-size:10px;line-height:1.6}.success-id{font-family:monospace;display:block;margin-top:5px;color:var(--ink);overflow-wrap:anywhere}.back{display:inline-block;margin-top:22px;font-size:11px;font-weight:700}.bottom{margin-top:35px;color:#96918a;font-size:10px;text-align:center}.demo{display:inline-block;margin-top:3px;padding:5px 8px;border:1px solid var(--line);border-radius:4px;color:#746f68;font-size:9px;letter-spacing:.5px;text-transform:uppercase}
@media(max-width:700px){main{padding:28px 15px 50px}.top{padding:0 15px}.layout{grid-template-columns:1fr}.summary{position:static;grid-row:1}.card{padding:19px}.heading h1{font-size:27px}}
</style></head><body><header class="top"><a class="brand" href="/"><span class="brandmark">R</span>ResilientBank</a><a class="top-link" href="/">←&nbsp; Operations overview</a></header><main><div class="crumb"><a href="/">Workspace</a>&nbsp; / &nbsp;Payments&nbsp; / &nbsp;New payment</div><section class="heading"><p class="eyebrow">PAYMENTS</p><h1>Create a payment</h1><p>Send a transfer between demo accounts. Review the details before submitting.</p></section>%s
<div class="layout"><form method="POST" action="/pay" class="card" id="payform"><div class="card-head"><div><div class="card-title">Transfer details</div><div class="card-caption">All fields are required</div></div><span class="step">STEP 1 OF 1</span></div>
<input type="hidden" name="idempotency_key" value="%s"><div class="field"><label for="from">From account</label><input class="input" id="from" name="from" value="%s" required autocomplete="off" spellcheck="false"><div class="field-note">Demo sender account · Alice Doe</div></div>
<div class="field"><label for="to">To account</label><input class="input" id="to" name="to" value="%s" required autocomplete="off" spellcheck="false"><div class="field-note">Demo recipient account · Bob Smith</div></div>
<div class="details"><div class="columns"><div class="field"><label for="amountInput">Amount</label><div class="amount-wrap"><span class="currency-mark" id="currencyMark">€</span><input class="input" id="amountInput" name="amount" value="%s" required inputmode="decimal" autocomplete="off" pattern="[0-9]+([.][0-9]{1,2})?" aria-describedby="amountHint"></div><div class="field-note" id="amountHint">Enter an amount in euros</div></div><div class="field"><label for="currencyInput">Currency</label><select class="input" id="currencyInput" name="currency"><option value="EUR" %s>EUR · Euro</option><option value="USD" %s>USD · US dollar</option></select><div class="field-note">Must match both account currencies</div></div></div></div>
<button type="submit" class="submit" id="payBtn">Review and send €50.00</button><div class="security"><span>●</span> Protected against accidental duplicate submissions</div></form>
<aside class="card summary"><div class="card-head"><div><div class="card-title">Payment summary</div><div class="card-caption">Check the transfer before sending</div></div></div><div class="summary-row"><span>From</span><strong id="summaryFrom">acc-alice</strong></div><div class="summary-row"><span>To</span><strong id="summaryTo">acc-bob</strong></div><div class="summary-total"><span>Total transfer</span><strong id="sumTotal">€50.00</strong></div><div class="summary-row"><span>Processing fee</span><strong>€0.00</strong></div><div class="summary-foot">Payments are recorded before notifications are delivered. If delivery is temporarily interrupted, the payment remains safely recorded while its message waits.</div><a class="back" href="/">←&nbsp; Back to operations</a></aside></div><div class="bottom">This is a demonstration payment environment. No card information is requested or stored.<br><span class="demo">Demo environment</span></div></main>
<script>(function(){var a=document.getElementById('amountInput'),c=document.getElementById('currencyInput'),b=document.getElementById('payBtn'),total=document.getElementById('sumTotal'),from=document.getElementById('from'),to=document.getElementById('to');function fmt(){var n=Number(a.value),cur=c.value,symbol=cur==='USD'?'$':'€',v=Number.isFinite(n)?n.toFixed(2):'0.00';document.getElementById('currencyMark').textContent=symbol;total.textContent=symbol+v;b.textContent='Review and send '+symbol+v;document.getElementById('summaryFrom').textContent=from.value||'—';document.getElementById('summaryTo').textContent=to.value||'—'}[a,c,from,to].forEach(function(e){e.addEventListener('input',fmt);e.addEventListener('change',fmt)});document.getElementById('payform').addEventListener('submit',function(){b.disabled=true;b.textContent='Submitting payment…'});fmt()})();</script></body></html>`, payHTML, html.EscapeString(idempotencyKey), html.EscapeString(from), html.EscapeString(to), html.EscapeString(amount), selectedCurrency(currency, "EUR"), selectedCurrency(currency, "USD"))
}

// payResult renders the outcome of the last submitted payment, if any.
func selectedCurrency(current, option string) string {
	if strings.EqualFold(current, option) {
		return "selected"
	}
	return ""
}

func payResultHTML(ok bool, id, errMsg string) string {
	if id == "" && errMsg == "" {
		return ""
	}
	if ok {
		return fmt.Sprintf(`<div class="notice" role="status" aria-live="polite">Payment accepted. Your payment reference:<span class="success-id">%s</span></div>`, html.EscapeString(id))
	}
	return fmt.Sprintf(`<div class="notice error" role="alert">Payment could not be completed: %s</div>`, html.EscapeString(errMsg))
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
		key := r.URL.Query().Get("key")
		if key == "" {
			key = uuid.NewString()
		}
		from, to := r.URL.Query().Get("from"), r.URL.Query().Get("to")
		amount, currency := r.URL.Query().Get("amount"), r.URL.Query().Get("currency")
		if from == "" {
			from = "acc-alice"
		}
		if to == "" {
			to = "acc-bob"
		}
		if amount == "" {
			amount = "50.00"
		}
		if currency == "" {
			currency = "EUR"
		}
		fmt.Fprint(w, pageCheckout(payHTML, key, from, to, amount, currency))
	})
	http.HandleFunc("/status.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		json.NewEncoder(w).Encode(collect(api, relayer))
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
// The browser keeps one idempotency key for this payment attempt so retries are safe.
func handlePay(w http.ResponseWriter, r *http.Request, grpcAddr string, logger *slog.Logger) {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	if err := r.ParseForm(); err != nil {
		renderPaymentError(w, r, "could not read the form", "")
		return
	}

	from := strings.TrimSpace(r.FormValue("from"))
	to := strings.TrimSpace(r.FormValue("to"))
	amount, err := parseAmountCents(strings.TrimSpace(r.FormValue("amount")))
	currency := strings.ToUpper(strings.TrimSpace(r.FormValue("currency")))
	key := strings.TrimSpace(r.FormValue("idempotency_key"))
	if from == "" || to == "" || from == to || err != nil || amount <= 0 || len(currency) != 3 || key == "" || len(key) > 128 {
		renderPaymentError(w, r, "Enter distinct accounts, a positive amount, a 3-letter currency, and a valid payment reference.", key)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		logger.Warn("dashboard could not reach the payment service", "addr", grpcAddr, "error", err)
		renderPaymentError(w, r, "payment service is not reachable. Check that the local services are running.", key)
		return
	}
	defer conn.Close()

	client := paymentsv1.NewPaymentServiceClient(conn)
	resp, err := client.CreatePayment(ctx, &paymentsv1.CreatePaymentRequest{
		IdempotencyKey: key,
		FromAccount:    from,
		ToAccount:      to,
		AmountCents:    amount,
		Currency:       currency,
	})
	if err != nil {
		logger.Warn("CreatePayment failed", "error", err)
		renderPaymentError(w, r, "the payment was rejected: "+grpcMessage(err), key)
		return
	}

	http.Redirect(w, r, "/checkout?paid="+url.QueryEscape(resp.GetPaymentId()), http.StatusSeeOther)
}

func parseAmountCents(raw string) (int64, error) {
	parts := strings.Split(raw, ".")
	if len(parts) > 2 || parts[0] == "" || (len(parts) == 2 && (len(parts[1]) == 0 || len(parts[1]) > 2)) {
		return 0, fmt.Errorf("invalid amount")
	}
	whole := parts[0]
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	for len(fraction) < 2 {
		fraction += "0"
	}
	for _, digit := range whole + fraction {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("invalid amount")
		}
	}
	return strconv.ParseInt(whole+fraction, 10, 64)
}

func renderPaymentError(w http.ResponseWriter, r *http.Request, msg, key string) {
	if key == "" || len(key) > 128 {
		key = uuid.NewString()
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusUnprocessableEntity)
	fmt.Fprint(w, pageCheckout(
		payResultHTML(false, "", msg),
		key,
		r.FormValue("from"),
		r.FormValue("to"),
		r.FormValue("amount"),
		r.FormValue("currency"),
	))
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
