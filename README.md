# ResilientBank

ResilientBank is an enterprise-grade multi-region banking resilience platform engineered to guarantee zero-loss financial transactions, sub-second failover, and continuous auditability under catastrophic infrastructure failures through active-active replication, distributed consensus, and autonomous anomaly detection.

## Prerequisites

Before running the platform locally, ensure the following tooling is installed:

- **Docker** (with Docker Compose v2+)
- **Go** (1.24+ or 1.27+)
- **buf** (`github.com/bufbuild/buf`)
- **protoc-gen-go** (`google.golang.org/protobuf/cmd/protoc-gen-go`)
- **protoc-gen-go-grpc** (`google.golang.org/grpc/cmd/protoc-gen-go-grpc`)
- **grpcurl** (`github.com/fullstorydev/grpcurl/cmd/grpcurl`)
- **jq** (v1.6+)

## Quick Start

1. **Start local infrastructure** (PostgreSQL, Redis, Kafka KRaft, Prometheus):
   ```bash
   make up
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

5. **Stop infrastructure**:
   ```bash
   make down
   ```
   Or reset with clean volumes:
   ```bash
   make reset
   ```

## Architecture Roadmap

- [x] **Phase 1: Local Region A Payment Service** — gRPC Payment API, two-layer idempotency (Redis fast-path + PostgreSQL UNIQUE constraint), Prometheus instrumentation, local Docker Compose infra, smoke tests, and CI.
- [ ] **Phase 2: Event-Driven Core & Outbox Pattern** — Kafka producer integration, transactional outbox in PostgreSQL, and dead-letter queues.
- [ ] **Phase 3: High-Availability Consensus & WAL** — BadgerDB Write-Ahead Log (WAL) and Raft consensus node for local durability.
- [ ] **Phase 4: Dual-Region Active-Active Replication** — Region B deployment, cross-region replication, conflict resolution, and split-brain mitigation.
- [ ] **Phase 5: Observability & Distributed Tracing** — OpenTelemetry, Jaeger, Grafana dashboards, Loki log aggregation, and Pyroscope profiling.
- [ ] **Phase 6: Real-Time ML Anomaly Detection** — Django microservice running Isolation Forest & LSTM on payment streams emitting anomaly scores.
- [ ] **Phase 7: Resilience & Chaos Engineering** — Chaos Mesh experiments (network partitions, latency injection, leader loss) with automated SLO verification.
- [ ] **Phase 8: Security, Cryptography & Compliance** — HashiCorp Vault secrets management, Ed25519 payload signing, Merkle audit trails, and automated DORA/PCI-DSS reporting.
- [ ] **Phase 9: Production GitOps & Policy Governance** — Terraform cloud infra, Kubernetes manifests, ArgoCD GitOps pipelines, and Open Policy Agent (OPA) gatekeepers.
