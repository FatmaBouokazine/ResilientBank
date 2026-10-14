.PHONY: up down reset migrate logs proto build run test smoke integration

up:
	docker compose -f deploy/local/docker-compose.yml up -d

down:
	docker compose -f deploy/local/docker-compose.yml down

reset:
	docker compose -f deploy/local/docker-compose.yml down -v

# DESTRUCTIVE. Postgres only executes /docker-entrypoint-initdb.d scripts when the data
# volume is empty, so there is no way to apply a new migration to an existing database.
# This recreates the volume and replays every migration in deploy/local/migrations in
# filename order. Adopt a real migration runner before this holds real data.
migrate:
	@echo "WARNING: dropping the pgdata volume and replaying all migrations."
	docker compose -f deploy/local/docker-compose.yml down -v
	docker compose -f deploy/local/docker-compose.yml up -d

logs:
	docker compose -f deploy/local/docker-compose.yml logs -f

proto:
	buf lint && buf generate

build:
	cd services/payments-api && go build ./...

run:
	cd services/payments-api && go run ./cmd/payments-api

test:
	cd services/payments-api && go test -v -race ./...

# Integration tests prove transactional atomicity against a real database and cannot run
# on in-memory fakes. Requires `make up` with Postgres reachable.
integration:
	cd services/payments-api && go test -v -race -tags=integration ./...

smoke:
	bash scripts/smoke-test.sh
