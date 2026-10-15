.PHONY: help tools verify up down reset migrate migrate-down migrate-status logs proto build run test integration smoke

# On Windows, `bash` resolves to C:\Windows\system32\bash.exe (WSL), which may be absent
# or broken even when Git for Windows is installed. Override to point at Git Bash:
#   make verify BASH="C:/Program Files/Git/bin/bash.exe"
BASH ?= bash

COMPOSE := docker compose -f deploy/local/docker-compose.yml

# Postgres host port. Override when a local PostgreSQL already owns 5432:
#   make up migrate integration POSTGRES_PORT=5433
POSTGRES_PORT ?= 5432

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
	@echo "  make reset                DESTRUCTIVE: drop volumes and start clean"
	@echo ""
	@echo "Build and test:"
	@echo "  make proto                lint and regenerate protobuf stubs"
	@echo "  make build                compile the services"
	@echo "  make run                  run the payments API"
	@echo "  make test                 unit tests (race detector)"
	@echo "  make integration          atomicity tests against a real database"
	@echo "  make smoke                end-to-end gRPC smoke suite"

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
	cd services/payments-api && go run ./cmd/migrate up

migrate-down:
	cd services/payments-api && go run ./cmd/migrate down

migrate-status:
	cd services/payments-api && go run ./cmd/migrate status

logs:
	$(COMPOSE) logs -f

proto:
	buf lint && buf generate

build:
	cd services/payments-api && go build ./...

run:
	cd services/payments-api && go run ./cmd/payments-api

test:
	cd services/payments-api && go test -v -race ./...

# Integration tests prove transactional atomicity against a real database and cannot run
# against in-memory fakes. Requires `make up migrate` first. Point at a non-default
# instance with TEST_DATABASE_URL if 5432 is occupied by a local PostgreSQL.
integration:
	cd services/payments-api && TEST_DATABASE_URL="$${TEST_DATABASE_URL:-postgres://resilient:resilient@127.0.0.1:$(POSTGRES_PORT)/payments?sslmode=disable}" go test -v -race -tags=integration ./...

smoke:
	$(BASH) scripts/smoke-test.sh