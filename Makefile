# Makefile
# Atalhos dos comandos do dia a dia do payrouter.
# Objetivo: ter um vocabulário único (test, lint, up, migrate-up...) igual ao do CLAUDE.md
# e ao do CI. Rodar via Git Bash ou WSL (make não vem no Windows puro).

.PHONY: test lint up down migrate-up migrate-down replay

test:
	go test -race ./...

lint:
	golangci-lint run ./...

up:
	docker compose up -d postgres

down:
	docker compose down

migrate-up:
	docker compose run --rm migrate up

migrate-down:
	docker compose run --rm migrate down -all

# Stub: o replay só existe na sessão 24 (Fase E).
replay:
	@echo "replay ainda não implementado (sessão 24)"; exit 1
