#!/usr/bin/env bash
set -euo pipefail

# scripts/demo.sh
# Narrated end-to-end demonstration of the platform's three claims:
#   1. a payment cannot be charged twice,
#   2. a committed payment always gets its event delivered,
#   3. the system keeps both promises while the broker is killed.
#
# Non-destructive: it adds demo payments to an already-running stack and never drops
# volumes. Start the stack first (make up-all) or let this script start it with --up.
#
# Usage:
#   scripts/demo.sh                 # demo against a running stack
#   scripts/demo.sh --up            # start the full Docker stack first
#   scripts/demo.sh --skip-chaos    # stop before killing Kafka
#   scripts/demo.sh --seconds 60    # longer chaos traffic window

GRPC_HOST="${GRPC_HOST:-localhost:50051}"
RELAYER_METRICS_URL="${RELAYER_METRICS_URL:-http://localhost:2113/metrics}"
PAYMENTS_DIR="services/payments-api"
DATABASE_URL="${DATABASE_URL:-postgres://resilient:resilient@127.0.0.1:5432/payments?sslmode=disable}"

START_STACK=0
RUN_CHAOS=1
CHAOS_SECONDS=45

while [ $# -gt 0 ]; do
    case "$1" in
        --up) START_STACK=1 ;;
        --skip-chaos) RUN_CHAOS=0 ;;
        --seconds) shift; CHAOS_SECONDS="${1:-45}" ;;
        -h|--help) sed -n '4,17p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
    shift
done

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
say()  { printf '  %s\n' "$*"; }

require() {
    command -v "$1" >/dev/null 2>&1 || { echo "missing prerequisite: $1" >&2; exit 1; }
}

require grpcurl
require jq
require curl

wait_for_api() {
    say "waiting for the payment API at $GRPC_HOST ..."
    for _ in $(seq 1 60); do
        if grpcurl -plaintext "$GRPC_HOST" list >/dev/null 2>&1; then
            say "API is up."
            return 0
        fi
        sleep 1
    done
    echo "the API never became reachable at $GRPC_HOST" >&2
    exit 1
}

create_payment() {
    local key="$1" amount="$2" from="${3:-acc-alice}" to="${4:-acc-bob}" currency="${5:-EUR}"
    grpcurl -plaintext -d "{
        \"idempotency_key\": \"$key\",
        \"from_account\": \"$from\",
        \"to_account\": \"$to\",
        \"amount_cents\": $amount,
        \"currency\": \"$currency\"
    }" "$GRPC_HOST" payments.v1.PaymentService/CreatePayment
}

if [ "$START_STACK" -eq 1 ]; then
    bold "Starting the stack"
    "$(dirname "$0")/docker-up.sh"
fi

wait_for_api

bold "Seeding demo accounts"
set +e
docker exec -i rb-postgres psql -v ON_ERROR_STOP=1 -U resilient -d payments \
    < "$(dirname "$0")/seed-demo-accounts.sql" >/dev/null 2>&1
if [ $? -eq 0 ]; then
    say "demo accounts present (idempotent seed)."
else
    say "could not seed via docker; assuming accounts already exist."
fi
set -e

# ---------------------------------------------------------------------------
bold "Claim 1: a payment cannot be charged twice"
# ---------------------------------------------------------------------------
DEMO_KEY="demo-$(date +%s)-$RANDOM"
say "Sending one payment with reference $DEMO_KEY ..."
FIRST=$(create_payment "$DEMO_KEY" 5000)
FIRST_ID=$(echo "$FIRST" | jq -r '.paymentId // .payment_id')
say "Accepted as payment $FIRST_ID"

say "Sending the same reference again (double-click / retry) ..."
SECOND=$(create_payment "$DEMO_KEY" 5000)
SECOND_ID=$(echo "$SECOND" | jq -r '.paymentId // .payment_id')

if [ "$FIRST_ID" = "$SECOND_ID" ]; then
    say "PASS: same payment returned ($SECOND_ID). The customer is charged once."
else
    say "FAIL: two different payment ids were created ($FIRST_ID vs $SECOND_ID)"
    exit 1
fi

say "Reusing the reference with a different amount must be rejected ..."
set +e
CONFLICT=$(create_payment "$DEMO_KEY" 9999 2>&1)
CONFLICT_CODE=$?
set -e
if [ $CONFLICT_CODE -ne 0 ] && echo "$CONFLICT" | grep -qi "AlreadyExists"; then
    say "PASS: rejected with AlreadyExists. A reference is bound to one amount."
else
    say "FAIL: expected AlreadyExists, got: $CONFLICT"
    exit 1
fi

# ---------------------------------------------------------------------------
bold "Claim 2: a committed payment always gets its event delivered"
# ---------------------------------------------------------------------------
say "A payment is written to PostgreSQL together with an outbox note in one transaction."
say "A separate relayer publishes those notes to Kafka. Its backlog is visible here:"
if curl -sf "$RELAYER_METRICS_URL" >/dev/null 2>&1; then
    PENDING=$(curl -s "$RELAYER_METRICS_URL" | grep '^resilientbank_outbox_pending ' | awk '{print $2}')
    AGE=$(curl -s "$RELAYER_METRICS_URL" | grep '^resilientbank_outbox_oldest_pending_age_seconds ' | awk '{print $2}')
    say "outbox pending=${PENDING:-?} oldest_pending_age=${AGE:-?}s"
else
    say "relayer metrics not reachable at $RELAYER_METRICS_URL (start it with: make relayer)"
fi

if [ "$RUN_CHAOS" -eq 0 ]; then
    bold "Skipping the fault-injection phase (--skip-chaos)."
    exit 0
fi

# ---------------------------------------------------------------------------
bold "Claim 3: kill the messenger mid-flight and nothing is lost"
# ---------------------------------------------------------------------------
say "Sending steady traffic for up to ${CHAOS_SECONDS}s while Kafka is killed."

if command -v go >/dev/null 2>&1; then
    ( cd "$PAYMENTS_DIR" && go run ./cmd/loadgen \
        -duration "${CHAOS_SECONDS}s" -warmup 2s -workers 8 -rate 40 -slo-p95 2s -slo-p99 3s \
        -slo-error-rate 1 ) &
    LOAD_PID=$!
else
    ( for i in $(seq 1 "$CHAOS_SECONDS"); do
        create_payment "demo-load-$(date +%s)-$i-$$" 100 acc-bank acc-alice >/dev/null 2>&1 || true
        sleep 1
    done ) &
    LOAD_PID=$!
fi

sleep 3
set +e
( cd "$PAYMENTS_DIR" && DATABASE_URL="$DATABASE_URL" \
    go run ./cmd/chaos run kafka-kill --duration 15s )
CHAOS_CODE=$?
set -e

wait "$LOAD_PID" 2>/dev/null || true

if [ $CHAOS_CODE -eq 0 ]; then
    say "PASS: the experiment reported the guarantees held or recovered."
else
    say "FAIL: the experiment reported a violation (exit $CHAOS_CODE)."
    exit 1
fi

say "Waiting for the outbox to drain after the broker recovers ..."
DRAINED=0
for _ in $(seq 1 120); do
    if curl -sf "$RELAYER_METRICS_URL" >/dev/null 2>&1; then
        PENDING=$(curl -s "$RELAYER_METRICS_URL" | grep '^resilientbank_outbox_pending ' | awk '{print $2}')
        if [ "${PENDING:-1}" = "0" ]; then DRAINED=1; break; fi
    fi
    sleep 1
done
if [ $DRAINED -eq 1 ]; then
    say "PASS: outbox pending=0. Every committed payment reached Kafka."
else
    say "NOTE: outbox has not fully drained yet; the relayer is still working through it."
fi

bold "Demo complete"
say "Next: open the dashboard (make dashboard) and the pitch guide (docs/pitch-guide.md)."
