.PHONY: up down reset logs proto build run test smoke

up:
	docker compose -f deploy/local/docker-compose.yml up -d

down:
	docker compose -f deploy/local/docker-compose.yml down

reset:
	docker compose -f deploy/local/docker-compose.yml down -v

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

smoke:
	bash scripts/smoke-test.sh
