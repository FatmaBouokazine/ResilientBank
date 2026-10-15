.PHONY: help tools verify up down reset migrate migrate-down migrate-status dev-up logs topics proto build run relayer dashboard test integration smoke tf-init tf-plan tf-apply tf-destroy argocd-bootstrap image-build image-load k8s-up

# On Windows, `bash` resolves to C:\Windows\system32\bash.exe (WSL), which may be absent
# or broken even when Git for Windows is installed. Override to point at Git Bash:
#   make verify BASH="C:/Program Files/Git/bin/bash.exe"
BASH ?= bash

COMPOSE := docker compose -f deploy/local/docker-compose.yml

# Postgres host port. Override when a local PostgreSQL already owns 5432:
#   make up migrate integration POSTGRES_PORT=5433
POSTGRES_PORT ?= 5432

# Connection string used by the migrator, the service, and the integration tests.
# Derived from POSTGRES_PORT so all three agree even on a non-default port.
DATABASE_URL ?= postgres://resilient:resilient@127.0.0.1:$(POSTGRES_PORT)/payments?sslmode=disable

# Tool versions below MUST match the require block in tools/go.mod.
# tools/go.mod is the source of truth; `make verify` fails if these drift apart.
# They are repeated here because `go install pkg@version` is the only install form that
# works: installing from the tools module itself fails to build buf v1.50.0 against
# modern Go, because buf's pinned docker dependency no longer compiles.
BUF_VERSION             := v1.50.0
PROTOC_GEN_GO_VERSION   := v1.36.12
PROTOC_GEN_GRPC_VERSION := v1.5.1

help:
	@echo "Setup:"
	@echo "  make tools                install pinned buf + protoc plugins"
	@echo "  make verify               check installed toolchain matches tools/go.mod"
	@echo ""
	@echo "Database:"
	@echo "  make up                   start infrastructure"
	@echo "  make migrate              apply pending database migrations"
	@echo "  make migrate-status       show applied/pending migrations"
	@echo "  make topics               create the Kafka topics (idempotent)"
	@echo "  make reset                DESTRUCTIVE: drop volumes and start clean"
	@echo ""
	@echo "Build and test:"
	@echo "  make proto                lint and regenerate protobuf stubs"
	@echo "  make build                compile the services"
	@echo "  make run                  run the payments API"
	@echo "  make relayer              run the outbox relayer (publishes to Kafka)"
	@echo "  make test                 unit tests (race detector)"
	@echo "  make integration          atomicity and relay tests against a real database"
	@echo "  make smoke                end-to-end gRPC and event smoke suite"

tools:
	@echo "Installing pinned toolchain from tools/go.mod"
	go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GRPC_VERSION)
	@echo "Done. Run 'make verify' to confirm."

verify:
	$(BASH) scripts/verify-toolchain.sh

up:
	POSTGRES_PORT=$(POSTGRES_PORT) $(COMPOSE) up -d

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v

migrate:
	cd services/payments-api && DATABASE_URL="$(DATABASE_URL)" go run ./cmd/migrate up

migrate-down:
	cd services/payments-api && DATABASE_URL="$(DATABASE_URL)" go run ./cmd/migrate down

migrate-status:
	cd services/payments-api && DATABASE_URL="$(DATABASE_URL)" go run ./cmd/migrate status

# Single command for a working local stack, for developers who are not using a migration
# runner in their own workflow. Waits for Postgres to report healthy before migrating so
# the first developer to clone does not hit a connection race.
dev-up: up migrate

logs:
	$(COMPOSE) logs -f

# Recreate the Kafka topics. Idempotent: existing topics keep their configuration.
# Needed after adding a topic, or to repair a broker volume that was wiped.
topics:
	$(COMPOSE) up -d kafka kafka-init
	$(COMPOSE) logs -f kafka-init

proto:
	buf lint && buf generate

build:
	cd services/payments-api && go build ./...

run:
	cd services/payments-api && go run ./cmd/payments-api

# The outbox relayer runs as its own process so broker stalls never reach payment
# ingestion. Safe to leave stopped: events accumulate in the outbox and drain on start.
relayer:
	cd services/payments-api && DATABASE_URL="$(DATABASE_URL)" go run ./cmd/outbox-relayer

# Live plain-language status page for demonstrations. Reads the metrics endpoints of the
# payment service and the relayer and renders them without protocol vocabulary.
# Open http://localhost:2114 while the stack is up.
dashboard:
	cd services/payments-api && go run ./cmd/dashboard

test:
	cd services/payments-api && go test -v -race ./...

# Integration tests prove transactional atomicity and relay behaviour against a real
# database and cannot run on in-memory fakes. Requires `make up migrate` first. Point at a
# non-default instance with TEST_DATABASE_URL if 5432 is occupied by a local PostgreSQL.
#
# Do NOT run `make relayer` at the same time: a live relayer will drain the outbox before
# the tests' fake publisher observes it, and the relay tests will fail for the wrong reason.
integration:
	cd services/payments-api && TEST_DATABASE_URL="$(DATABASE_URL)" go test -v -race -tags=integration ./...

smoke:
	$(BASH) scripts/smoke-test.sh