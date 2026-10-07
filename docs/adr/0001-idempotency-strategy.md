# ADR 0001: Idempotency Strategy for Payment Ingestion

## Status

Accepted

## Context

Financial transactions in ResilientBank must adhere to strict exactly-once semantics from the perspective of the caller. Network timeouts, client retries, transient network partitions, and concurrent duplicate submissions can cause identical payment requests to arrive multiple times at the payment service. 

Without idempotency guarantees, duplicate execution would lead to double-debiting customer accounts, violating core banking invariants.

Key design requirements:
1. Low latency for high-throughput normal operations.
2. Resilience against cache failures (e.g., Redis crash or restart).
3. Protection against concurrent race conditions where two identical requests arrive simultaneously.
4. Detection of idempotency key reuse with modified parameters (payload conflict detection).

## Decision

We adopt a two-layer idempotency architecture backed by cryptographic request fingerprinting:

### 1. Request Fingerprinting
Each incoming `CreatePaymentRequest` is fingerprinted by computing the SHA-256 hash of the canonicalized payload:
`SHA-256(from_account + "|" + to_account + "|" + amount_cents + "|" + currency)`

This fingerprint is stored alongside the payment ID and idempotency key.

### 2. Redis Fast Path (Optimization Layer)
Before querying the database, the service queries Redis for key `idem:<idempotency_key>`:
- **Cache Hit with Matching Fingerprint**: The payment was already processed. The service immediately returns the existing `payment_id` with status `duplicate`.
- **Cache Hit with Conflicting Fingerprint**: The idempotency key was reused with different payment parameters. The service rejects the request immediately with gRPC status `codes.AlreadyExists` ("idempotency key reused with different parameters").
- **Cache Miss**: The request proceeds to PostgreSQL.
- **Redis Error / Timeout**: Redis operations run with tight timeouts (200ms) and 1 retry. Any Redis failure is caught, logged as a warning, recorded in the `resilientbank_dependency_errors_total{dependency="redis"}` metric, and safely bypassed to PostgreSQL. Redis is strictly an optimization, never a single point of failure.

### 3. PostgreSQL UNIQUE Constraint (Durability & Invariant Guarantee)
The `payments` table maintains a `UNIQUE` constraint on `idempotency_key`:
- The payment is inserted using `INSERT INTO payments (...) VALUES (...) ON CONFLICT (idempotency_key) DO NOTHING`.
- If 1 row is affected, the payment is newly created and committed.
- If 0 rows are affected (due to concurrent race condition or cache miss for an existing record), the service queries the existing record by `idempotency_key` and compares the stored `request_hash` against the current request hash:
  - If hashes match: Return existing `payment_id` as `duplicate`.
  - If hashes differ: Return `codes.AlreadyExists` as `conflict`.

### 4. Write-Through Cache
Upon successful insertion or resolution in PostgreSQL, the record is populated back into Redis with a 24-hour TTL (`IDEMPOTENCY_TTL`) via `SET idem:<key>`. Any failure during cache write is logged as a non-fatal warning.

## Consequences

### Positive
- **High Performance**: Typical duplicate requests are caught in Redis in sub-millisecond time without querying PostgreSQL.
- **Fail-Safe**: If Redis is completely unavailable or suffers latency spikes, payments still succeed and remain strictly idempotent because the database UNIQUE constraint acts as the source of truth.
- **Concurrency Safe**: Two simultaneous requests with the same key race at the PostgreSQL transaction/unique index level, preventing double inserts.
- **Tamper & Collision Detection**: Reusing keys with altered amounts or accounts is immediately flagged as a conflict.

### Negative / Trade-offs
- Slight operational overhead of managing both Redis and PostgreSQL.
- Storage footprint of maintaining idempotency keys and request hashes in PostgreSQL.
