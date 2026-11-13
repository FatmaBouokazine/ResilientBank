# Demo runbook

This is the operational companion to `docs/pitch-guide.md`. The pitch guide explains what
to say; this page covers what to run, in what order, and what should appear on screen.

## Prerequisites

- Docker with Compose v2
- Go 1.24+
- `grpcurl`, `jq`, `curl` on `PATH`
- Git Bash or any POSIX shell on Windows

## Fast path

```bash
make up-all          # infra, migrations, API, relayer, dashboard
make seed            # idempotent demo accounts
scripts/demo.sh      # narrated three-claim demo, including a Kafka kill
```

Open the dashboard at <http://localhost:2114> in a second window so the audience can watch
the backlog move while the fault is running.

## Slow path, step by step

Use this when you want to drive each claim yourself instead of letting the script narrate.

1. **Start and verify the stack**

   ```bash
   make up-all
   make seed
   scripts/smoke-test.sh
   ```

   The smoke suite proves the basics: the service is reachable, duplicate references
   return the same payment, and the outbox reaches `PUBLISHED`.

2. **Claim 1: no double charge**

   ```bash
   KEY=demo-$(date +%s)
   grpcurl -plaintext -d "{\"idempotency_key\":\"$KEY\",\"from_account\":\"acc-alice\",\"to_account\":\"acc-bob\",\"amount_cents\":5000,\"currency\":\"EUR\"}" \
     localhost:50051 payments.v1.PaymentService/CreatePayment
   ```

   Run the same command again. The `paymentId` must be identical. Change the amount and it
   must fail with `AlreadyExists`.

3. **Claim 2: the event is never lost**

   ```bash
   curl -s localhost:2113/metrics | grep resilientbank_outbox_
   ```

   `resilientbank_outbox_pending` is the backlog the relayer is working through, and
   `resilientbank_outbox_oldest_pending_age_seconds` is how long the oldest row has waited.
   A payment is committed in PostgreSQL before any Kafka call is attempted, so these numbers
   can grow but a payment is never silently dropped.

4. **Claim 3: break it live**

   ```bash
   cd services/payments-api
   go run ./cmd/loadgen -duration 45s -warmup 2s -workers 8 -rate 40 -slo-error-rate 1 &
   DATABASE_URL="postgres://resilient:resilient@127.0.0.1:5432/payments?sslmode=disable" \
     go run ./cmd/chaos run kafka-kill --duration 15s
   ```

   The chaos runner injects the fault, samples the outbox SLOs during it, then waits for
   recovery. Exit code `0` means the guarantees held or the system recovered; `1` means a
   violation. The load generator in the background keeps committing payments throughout,
   which is what makes the experiment meaningful.

5. **Close on the drain**

   ```bash
   watch -n 1 "curl -s localhost:2113/metrics | grep '^resilientbank_outbox_pending '"
   ```

   When the backlog returns to zero, every payment committed during the outage has reached
   Kafka.

## What not to do on stage

- Do not run `make reset` or any `-v` compose command during the demo; it drops data.
- Do not start a second relayer while the integration tests are running; it drains the
  outbox before the tests observe it.
- Do not claim the latency numbers from `make loadgen` unless the machine was idle. Report
  them as "on this laptop, under this load", not as production guarantees.
