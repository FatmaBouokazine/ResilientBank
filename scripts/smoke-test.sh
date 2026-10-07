#!/usr/bin/env bash
set -euo pipefail

GRPC_HOST="${GRPC_HOST:-localhost:50051}"
METRICS_URL="${METRICS_URL:-http://localhost:2112/metrics}"

echo "=== ResilientBank Smoke Test ==="
echo "Target gRPC: $GRPC_HOST"
echo "Target Metrics: $METRICS_URL"

# Check 1: The service list contains payments.v1.PaymentService
echo -n "Check 1: Service list contains payments.v1.PaymentService... "
SERVICES=$(grpcurl -plaintext "$GRPC_HOST" list)
if echo "$SERVICES" | grep -q "payments.v1.PaymentService"; then
    echo "PASS"
else
    echo "FAIL"
    echo "$SERVICES"
    exit 1
fi

# Check 2: CreatePayment with a random key, sent twice, returns the SAME payment_id
echo -n "Check 2: Idempotent CreatePayment returns identical payment_id... "
RAND_SUFFIX=$(date +%s)$(od -An -N4 -tu4 /dev/urandom | tr -d ' ' 2>/dev/null || echo $RANDOM)
IDEM_KEY="smoke-key-${RAND_SUFFIX}"

REQ1=$(grpcurl -plaintext -d "{
    \"idempotency_key\": \"$IDEM_KEY\",
    \"from_account\": \"acc-alice\",
    \"to_account\": \"acc-bob\",
    \"amount_cents\": 5000,
    \"currency\": \"EUR\"
}" "$GRPC_HOST" payments.v1.PaymentService/CreatePayment)

PAYMENT_ID_1=$(echo "$REQ1" | jq -r '.paymentId // .payment_id')
STATUS_1=$(echo "$REQ1" | jq -r .status)

if [ -z "$PAYMENT_ID_1" ] || [ "$PAYMENT_ID_1" = "null" ]; then
    echo "FAIL (Failed to extract payment_id from first call)"
    exit 1
fi

REQ2=$(grpcurl -plaintext -d "{
    \"idempotency_key\": \"$IDEM_KEY\",
    \"from_account\": \"acc-alice\",
    \"to_account\": \"acc-bob\",
    \"amount_cents\": 5000,
    \"currency\": \"EUR\"
}" "$GRPC_HOST" payments.v1.PaymentService/CreatePayment)

PAYMENT_ID_2=$(echo "$REQ2" | jq -r '.paymentId // .payment_id')

if [ "$PAYMENT_ID_1" = "$PAYMENT_ID_2" ]; then
    echo "PASS (payment_id: $PAYMENT_ID_1, status: $STATUS_1)"
else
    echo "FAIL (Mismatch: $PAYMENT_ID_1 != $PAYMENT_ID_2)"
    exit 1
fi

# Check 3: GetPayment on that id succeeds
echo -n "Check 3: GetPayment on that id succeeds... "
GET_RES=$(grpcurl -plaintext -d "{\"payment_id\": \"$PAYMENT_ID_1\"}" "$GRPC_HOST" payments.v1.PaymentService/GetPayment)
GET_ID=$(echo "$GET_RES" | jq -r '.paymentId // .payment_id')
GET_STATUS=$(echo "$GET_RES" | jq -r .status)

if [ "$GET_ID" = "$PAYMENT_ID_1" ] && [ -n "$GET_STATUS" ] && [ "$GET_STATUS" != "null" ]; then
    echo "PASS"
else
    echo "FAIL (Expected $PAYMENT_ID_1, got $GET_ID)"
    exit 1
fi

# Check 4: Reusing the key with a different amount returns AlreadyExists
echo -n "Check 4: Reusing key with different amount returns AlreadyExists... "
set +e
CONFLICT_OUT=$(grpcurl -plaintext -d "{
    \"idempotency_key\": \"$IDEM_KEY\",
    \"from_account\": \"acc-alice\",
    \"to_account\": \"acc-bob\",
    \"amount_cents\": 9999,
    \"currency\": \"EUR\"
}" "$GRPC_HOST" payments.v1.PaymentService/CreatePayment 2>&1)
CONFLICT_CODE=$?
set -e

if [ $CONFLICT_CODE -ne 0 ] && echo "$CONFLICT_OUT" | grep -qi "AlreadyExists"; then
    echo "PASS"
else
    echo "FAIL (Expected AlreadyExists, got exit=$CONFLICT_CODE output=$CONFLICT_OUT)"
    exit 1
fi

# Check 5: An invalid request returns InvalidArgument
echo -n "Check 5: Invalid request returns InvalidArgument... "
set +e
INVALID_OUT=$(grpcurl -plaintext -d "{
    \"idempotency_key\": \"\",
    \"from_account\": \"acc-alice\",
    \"to_account\": \"acc-alice\",
    \"amount_cents\": -100,
    \"currency\": \"INVALID\"
}" "$GRPC_HOST" payments.v1.PaymentService/CreatePayment 2>&1)
INVALID_CODE=$?
set -e

if [ $INVALID_CODE -ne 0 ] && echo "$INVALID_OUT" | grep -qi "InvalidArgument"; then
    echo "PASS"
else
    echo "FAIL (Expected InvalidArgument, got exit=$INVALID_CODE output=$INVALID_OUT)"
    exit 1
fi

# Check 6: curl :2112/metrics contains resilientbank_payments_total
echo -n "Check 6: curl :2112/metrics contains resilientbank_payments_total... "
METRICS_OUT=$(curl -s "$METRICS_URL")
if echo "$METRICS_OUT" | grep -q "resilientbank_payments_total"; then
    echo "PASS"
else
    echo "FAIL (Metric resilientbank_payments_total not found in $METRICS_URL)"
    exit 1
fi

echo "=== All 6 Smoke Tests PASSED ==="
