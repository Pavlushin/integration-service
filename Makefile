SHELL := /bin/zsh

PROJECT_ROOT := $(CURDIR)
COMPOSE := env PROJECT_ROOT="$(PROJECT_ROOT)" docker compose --env-file .env -f deployments/docker-compose.yml
GO := env GOCACHE="$(PROJECT_ROOT)/.gocache" go

.PHONY: dev-up dev-down dev-logs postgres-migrate api worker api-dev worker-dev stack stack-up \
	pg-forward-up pg-forward-down rabbit-forward-up rabbit-forward-down rabbit-ui-forward-up rabbit-ui-forward-down rabbit-ui-forward-restart \
	db-shell db-jobs db-jobs-full db-outbox db-audit log-tail logs-corr logs-job trace build fmt

dev-up:
	$(COMPOSE) --profile forwarders up -d postgres rabbitmq postgres-port-forwarder rabbitmq-port-forwarder rabbitmq-management-forwarder

dev-down:
	$(COMPOSE) down

dev-logs:
	$(COMPOSE) logs -f

postgres-migrate:
	$(COMPOSE) --profile tools run --rm postgres-migrate

api:
	$(COMPOSE) up --build api

worker:
	$(COMPOSE) up --build worker

stack:
	$(COMPOSE) up --build api worker

stack-up:
	$(COMPOSE) up -d --build api worker

pg-forward-up:
	$(COMPOSE) --profile forwarders up -d postgres postgres-port-forwarder

pg-forward-down:
	$(COMPOSE) stop postgres-port-forwarder

rabbit-forward-up:
	$(COMPOSE) --profile forwarders up -d rabbitmq rabbitmq-port-forwarder

rabbit-forward-down:
	$(COMPOSE) stop rabbitmq-port-forwarder

rabbit-ui-forward-up:
	$(COMPOSE) --profile forwarders up -d --force-recreate rabbitmq rabbitmq-management-forwarder

rabbit-ui-forward-down:
	$(COMPOSE) rm -sf rabbitmq-management-forwarder

rabbit-ui-forward-restart:
	-$(COMPOSE) rm -sf rabbitmq-management-forwarder
	$(COMPOSE) --profile forwarders up -d --force-recreate rabbitmq rabbitmq-management-forwarder

db-shell:
	$(COMPOSE) exec postgres sh -lc 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

db-jobs:
	$(COMPOSE) exec -T postgres sh -lc 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT id, correlation_id, type, kind, direction, status, dedupe_key, created_at FROM integration_jobs ORDER BY created_at DESC LIMIT 20;"'

db-jobs-full:
	$(COMPOSE) exec -T postgres sh -lc 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT id, correlation_id, parent_id, type, kind, direction, status, dedupe_key, created_at, started_at, finished_at FROM integration_jobs ORDER BY created_at DESC LIMIT 50;"'

db-outbox:
	$(COMPOSE) exec -T postgres sh -lc 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT id, topic, payload_json, created_at, available_at, published_at, last_error FROM integration_outbox ORDER BY created_at DESC LIMIT 20;"'

db-audit:
	$(COMPOSE) exec -T postgres sh -lc 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT id, job_id, action, actor, reason, metadata_json, created_at FROM integration_job_audit ORDER BY created_at DESC LIMIT 20;"'

log-tail:
	@tail -n 100 $$(ls -1t out/logs/*.log | head -n 2)

logs-corr:
	@if [ -z "$(CORR)" ]; then echo "usage: make logs-corr CORR=<correlation_id>"; exit 1; fi
	@grep -h "$(CORR)" out/logs/*.log || true

logs-job:
	@if [ -z "$(JOB)" ]; then echo "usage: make logs-job JOB=<job_id>"; exit 1; fi
	@grep -h "$(JOB)" out/logs/*.log || true

trace:
	@if [ -z "$(CORR)" ]; then echo "usage: make trace CORR=<correlation_id>"; exit 1; fi
	@printf '\n== JOBS ==\n'
	@$(COMPOSE) exec -T postgres sh -lc 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT id, correlation_id, parent_id, type, kind, direction, status, dedupe_key, result_path, attempts, created_at, started_at, finished_at FROM integration_jobs WHERE correlation_id = '\''$(CORR)'\'' ORDER BY created_at ASC;"'
	@printf '\n== OUTBOX ==\n'
	@$(COMPOSE) exec -T postgres sh -lc 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT o.id, o.topic, o.payload_json, o.created_at, o.available_at, o.published_at, o.last_error FROM integration_outbox o WHERE EXISTS (SELECT 1 FROM integration_jobs j WHERE j.correlation_id = '\''$(CORR)'\'' AND o.payload_json ->> '\''job_id'\'' = j.id) ORDER BY o.created_at ASC;"'
	@printf '\n== AUDIT ==\n'
	@$(COMPOSE) exec -T postgres sh -lc 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT a.id, a.job_id, a.action, a.actor, a.reason, a.metadata_json, a.created_at FROM integration_job_audit a WHERE EXISTS (SELECT 1 FROM integration_jobs j WHERE j.correlation_id = '\''$(CORR)'\'' AND a.job_id = j.id) ORDER BY a.created_at ASC;"'
	@printf '\n== LOGS ==\n'
	@grep -h "$(CORR)" out/logs/*.log || true

api-dev:
	$(GO) run ./cmd/api

worker-dev:
	$(GO) run ./cmd/worker

build:
	$(GO) build ./...

fmt:
	$(GO) fmt ./...
