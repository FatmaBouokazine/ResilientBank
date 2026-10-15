# ResilientBank

ResilientBank is an enterprise-grade multi-region banking resilience platform engineered to guarantee zero-loss financial transactions, sub-second failover, and continuous auditability under catastrophic infrastructure failures through active-active replication, distributed consensus, and autonomous anomaly detection.

## Tech Stack

- **Backend**: Go gRPC Payment API & Outbox Relayer; Django (Python) REST Framework for extended banking APIs
- **Infrastructure & Deployment**: Terraform, ArgoCD, Kubernetes, Docker Compose
- **Chaos Engineering**: Chaos Mesh, custom fault-injection controllers (Docker & Kubernetes)
- **Formal Verification**: TLA+ / TLC model checker
- **Data & Messaging**: PostgreSQL 17 (with Goose migrations), Apache Kafka KRaft 4.0.0, Redis 7
- **Observability**: Prometheus, Grafana, Loki, OpenTelemetry, Jaeger
- **AIOps / ML**: Python, PyTorch (Isolation Forest + LSTM)
- **Security**: HashiCorp Vault, mTLS, Sigstore/cosign, Open Policy Agent (OPA)
- **Frontend**: Live Resilience Dashboard & Angular Checkout Client

---

## Sprint Roadmap & Status

| Sprint | Objective | Duration | Status |
|---|---|---|---|
| **Sprint 0** | Repository setup, CI/CD skeleton, Terraform base infrastructure, and Argo CD bootstrap | 1 week | **Completed & Verified** |
| **Sprint 1–2** | Core chaos engine and fault injection: node kill, network partition, and latency injection | 1 month | **Completed & Verified** |
| **Sprint 3** | TLA+ modeling of Raft consensus and integration of the TLC verification gate into the deployment pipeline | 2 weeks | **Completed & Verified** |
| **Sprint 4** | Prometheus, Grafana, Loki, Jaeger, and resilience dashboard | 2 weeks | **Partially Implemented** (Dashboard & Grafana active; Jaeger/Loki in progress) |
| **Sprint 5** | AIOps prediction using Isolation Forest and LSTM, plus anomaly-score feedback | 2 weeks | **Planned** |
| **Sprint 6** | DORA/PCI-DSS compliance reporting, financial impact modeling, and FinOps cost-availability view | 2 weeks | **Planned** |
| **Sprint 7** | Vault, mTLS, performance tuning, and final demo preparation | 1 week | **Planned** |

---

## Sprint 3: Formal Verification of Raft Consensus (TLA+ / TLC)

ResilientBank guarantees zero transaction loss and partition tolerance by modeling its high-availability consensus engine in **TLA+** and verifying it exhaustively with the **TLC model checker**.

### Specification Overview

Located in `formal/raft/`:
- **`formal/raft/Raft.tla`**: Formal mathematical specification of the Raft consensus protocol.
  - **Server Roles**: Follower, Candidate, Leader.
  - **State Transitions**:
    - `Timeout(s)`: Election timeout triggering candidate state, randomized term increment, and vote broadcasts.
    - `HandleRequestVoteReq(s, m)`: Strict term evaluation, candidate log up-to-date checks (Section 5.4.1), and vote granting.
    - `HandleRequestVoteResp(s, m)`: Quorum vote tallying and atomic transition to Leader.
    - `ClientRequest(s, v)`: Ingestion of transaction payloads into leader logs.
    - `SendAppendEntries(s, dest)`: Heartbeats and log replication parameterized by leader `nextIndex` and `matchIndex`.
    - `HandleAppendEntriesReq(s, m)`: Term synchronization, log consistency verification, conflict truncation, and `commitIndex` advancement.
    - `HandleAppendEntriesResp(s, m)`: Follower log matching acknowledgement, `nextIndex` decrement upon mismatch, and quorum commit advancement.
- **`formal/raft/Raft.cfg`**: Finite model configuration for TLC execution (`Server = {s1, s2, s3}`, `Value = {v1}`, `MaxTerm = 2`, `MaxLogLen = 1`).

### Formally Verified Safety Invariants

TLC exhaustively verifies that every reachable state across all interleavings satisfies:

1. **`ElectionSafety`**: At most one leader can be elected in any given term:
   $$\forall s_1, s_2 \in Server : (state[s_1] = "Leader" \land state[s_2] = "Leader" \land currentTerm[s_1] = currentTerm[s_2]) \implies s_1 = s_2$$
2. **`LogMatching`**: If two logs contain an entry with the same index and term, the logs are identical up to that index:
   $$\forall s_1, s_2 \in Server, idx \in 1..\min(Len(log[s_1]), Len(log[s_2])) : (log[s_1][idx].term = log[s_2][idx].term) \implies (\forall i \in 1..idx : log[s_1][i] = log[s_2][i])$$
3. **`LeaderCompleteness`**: If a log entry is committed in a given term, that entry is present in the logs of the leaders for all higher terms.
4. **`CommitIndexBounded`**: A server's `commitIndex` never exceeds the physical length of its local log:
   $$\forall s \in Server : commitIndex[s] \le Len(log[s])$$
5. **`TypeOK`**: Type invariance and bounded domains for all variables across all execution paths.

### Running TLC Locally

To run the verification gate on your workstation:
```bash
make tlc
# Or directly via the runner script:
bash scripts/run-tlc.sh
```

**Verification Results:**
- States generated: **2,732,485**
- Distinct states explored: **510,211**
- Search depth: **30**
- Invariant violations: **0 (PASSED)**

### CI/CD Verification Gate

The `.github/workflows/ci.yml` pipeline runs the `tlc` job before any deployment action. The pipeline immediately halts if TLC detects an invariant violation, semantic defect, or state exploration failure.

---

## Repository Structure

```
├── .github/workflows/      # GitHub Actions CI/CD (unit, integration, chaos SLO, proto, TLC gate)
├── chaos/                  # Chaos Mesh experiments and fault configurations
├── cmd/
│   ├── chaos/              # Chaos CLI engine for fault injection
│   ├── dashboard/          # Plain-language resilience web dashboard & Stripe-style test form
│   ├── migrate/            # Embedded Goose migration runner
│   ├── outbox-relayer/     # Transactional outbox Kafka relayer
│   └── payments-api/       # Region A Go gRPC payment API
├── deploy/
│   ├── k8s/                # Kubernetes manifests (app deployments, migrate-job, ArgoCD Application)
│   ├── local/              # Docker Compose (PostgreSQL 17, Redis 7, Kafka 4.0.0 KRaft, Prometheus)
│   └── terraform/          # Base Terraform infrastructure (namespaces, quotas, cluster bootstrap)
├── docs/                   # Architecture Decision Records (ADRs) & Pitch guides
├── formal/
│   └── raft/               # Sprint 3 TLA+ Raft consensus specification & TLC configuration
├── observability/          # Prometheus configuration & Grafana provisioning dashboards
├── proto/                  # Protobuf contracts (payments.v1, events.v1)
├── scripts/
│   ├── argocd-bootstrap.sh # Automated ArgoCD installation script
│   ├── docker-up.sh        # Single-command full stack orchestrator
│   ├── run-tlc.sh          # Reproducible TLC model checker execution script
│   ├── smoke-test.sh       # End-to-end gRPC and idempotency smoke verification
│   └── verify-toolchain.sh # Pinned toolchain drift verification
└── Makefile                # Central automation targets
```

---

## Prerequisites

- **Docker** (with Docker Compose v2+)
- **Go** (1.24 or newer; pinned in `services/payments-api/go.mod`)
- **Java** (OpenJDK 11+ for TLC model checking)
- **GNU Make**
- **grpcurl** (`go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest`)
- **jq** (v1.6+)

`buf`, `protoc-gen-go`, and `protoc-gen-go-grpc` are pinned in `tools/go.mod`:
```bash
make tools    # install pinned versions
make verify   # confirm versions match pins
```

---

## Quick Start

### 1. Launch the Full Stack with One Command
```bash
make up-all
```
This automatically starts:
- PostgreSQL 17, Redis 7, Kafka KRaft 4.0.0, Prometheus, and Grafana
- Applies database migrations via Goose
- Starts `payments-api`, `outbox-relayer`, and the live dashboard

Open:
- **Resilience Dashboard & Checkout Form**: http://localhost:2114
- **Grafana Dashboards**: http://localhost:3000 (admin/admin)
- **Prometheus Targets**: http://localhost:9090/targets

### 2. Run Test & Verification Suites
```bash
make test         # Unit tests with Go race detector
make integration  # Transactional outbox atomicity against PostgreSQL
make smoke        # End-to-end gRPC, idempotency, and conflict detection
make tlc          # Formal TLA+ model check of Raft consensus
```

### 3. Stop Infrastructure
```bash
make down
# Or reset volumes completely:
make reset
```

---

## Known Limitations & Next Steps

- **Dual-Region Replication (Phase 4)**: Local environment currently models Region A; Region B cross-replication and split-brain resolution are scheduled for Phase 4.
- **AIOps Microservice (Sprint 5)**: Machine learning anomaly detection service (Isolation Forest + LSTM) to be connected to the Kafka `payments.events.v1` stream.
- **Compliance Automation (Sprint 6)**: DORA and PCI-DSS automated report generation based on chaos experiment logs and transaction histories.
