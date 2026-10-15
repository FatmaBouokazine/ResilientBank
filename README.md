# ResilientBank

ResilientBank is an enterprise-grade multi-region banking resilience platform engineered to guarantee zero-loss financial transactions, sub-second failover, and continuous auditability under catastrophic infrastructure failures through active-active replication, distributed consensus, and autonomous anomaly detection.

## Prerequisites

Before running the platform locally, ensure the following tooling is installed:

- **Docker** (with Docker Compose v2+)
- **Go** (1.24 or newer; CI uses the version pinned in `services/payments-api/go.mod`)
- **GNU Make**
- **grpcurl** (`go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest`)
- **jq** (v1.6+)

`buf`, `protoc-gen-go`, and `protoc-gen-go-grpc` are **not** installed manually. Their
versions are pinned in `tools/go.mod` so every developer and CI runner generate identical
stubs:

```bash
make tools    # install the pinned versions
make verify   # confirm the installed versions match the pins
```

Generated stubs record the generator version in a header comment, and `buf generate` output
can differ between releases. `make verify` exists to catch that drift before it turns into a
spurious diff on someone else's machine.

### Windows notes

`bash` resolves to `C:\Windows\system32\bash.exe` (WSL), which is often absent or broken
even when Git for Windows is installed. If `make verify` or `make smoke` fails to find a
working shell, point Make at Git Bash explicitly:

```bash
make verify BASH="C:/Program Files/Git/bin/bash.exe"
```

If a local PostgreSQL installation already owns port 5432, Docker cannot bind it. Use a
different port consistently:

```bash
make dev-up integration POSTGRES_PORT=5433
```

## Database Migrations

The schema is owned by a versioned [goose](https://github.com/pressly/goose) runner built
from `services/payments-api/cmd/migrate`. Migrations are embedded in the binary, so the
migrator can never be run against a schema different from the one compiled into it.

```bash
make migrate          # apply all pending migrations
make migrate-status   # list applied and pending migrations
make migrate-down     # roll back the most recent migration
```

Migrations upgrade an **existing** database in place. A database created before the runner
existed simply has no `goose_db_version` row, so applying the baseline migration is a no-op
that preserves existing payments.

> Do not run `make reset` casually. It drops the database volume and destroys every payment
> in it. There is no undo.

## Quick Start

1. **Start infrastructure and apply the schema** (PostgreSQL, Redis, Kafka KRaft, Prometheus):
   ```bash
   make dev-up    # equivalent to: make up && make migrate
   ```

2. **Run the Payment Service**:
   ```bash
   make run
   ```

3. **Run the Smoke Test Suite** (verifies gRPC reflection, idempotency, conflict detection, validation, metrics):
   ```bash
   make smoke
   ```

4. **Run Unit Tests**:
   ```bash
   make test
   ```

5. **Run Atomicity Integration Tests** (proves the outbox transaction against a real database):
   ```bash
   make integration
   ```

6. **Stop infrastructure**:
   ```bash
   make down
   ```

`make help` lists every available target.

## Contributing

Work lands as **stacked branches**, one per step of a phase, because several steps are only
reviewable in isolation once the one before them has landed:

1. Branch from the default branch: `feat/phase2-stepN-<slug>`.
2. Open a PR targeting the default branch, and state which earlier step branches it builds on.
3. **Merge in order.** A later step cannot be merged before the ones it is stacked on, or its
   base disappears and its diff becomes unreviewable.

Before opening a PR:

```bash
make verify       # toolchain matches the pins
make test         # unit tests
make integration  # atomicity tests against a real database
```

## Architecture Roadmap

- [x] **Phase 1: Local Region A Payment Service** — gRPC Payment API, two-layer idempotency (Redis fast-path + PostgreSQL UNIQUE constraint), Prometheus instrumentation, local Docker Compose infra, smoke tests, and CI.
- [ ] **Phase 2: Event-Driven Core & Outbox Pattern** — payment events are staged in a transactional outbox and published to Kafka by a separate relayer, with dead-letter routing.
  - [x] Payment event contract (`events.v1.PaymentEvent`) with `buf breaking` enforcement
  - [x] Payment and outbox row written atomically in one transaction (ADR 0002)
  - [ ] Kafka producer and `outbox-relayer` process
  - [ ] Retry backoff and dead-letter queue
- [ ] **Phase 2: Event-Driven Core & Outbox Pattern** — Kafka producer integration, transactional outbox in PostgreSQL, and dead-letter queues.
- [ ] **Phase 3: High-Availability Consensus & WAL** — BadgerDB Write-Ahead Log (WAL) and Raft consensus node for local durability.
- [ ] **Phase 4: Dual-Region Active-Active Replication** — Region B deployment, cross-region replication, conflict resolution, and split-brain mitigation.
- [ ] **Phase 5: Observability & Distributed Tracing** — OpenTelemetry, Jaeger, Grafana dashboards, Loki log aggregation, and Pyroscope profiling.
- [ ] **Phase 6: Real-Time ML Anomaly Detection** — Django microservice running Isolation Forest & LSTM on payment streams emitting anomaly scores.
- [ ] **Phase 7: Resilience & Chaos Engineering** — Chaos Mesh experiments (network partitions, latency injection, leader loss) with automated SLO verification.
- [ ] **Phase 8: Security, Cryptography & Compliance** — HashiCorp Vault secrets management, Ed25519 payload signing, Merkle audit trails, and automated DORA/PCI-DSS reporting.
- [ ] **Phase 9: Production GitOps & Policy Governance** — Terraform cloud infra, Kubernetes manifests, ArgoCD GitOps pipelines, and Open Policy Agent (OPA) gatekeepers.
