# Makefile
# Atalhos dos comandos do dia a dia do payrouter.
# Objetivo: ter um vocabulário único (test, lint, up, migrate-up...) igual ao do CLAUDE.md
# e ao do CI. Rodar via Git Bash ou WSL (make não vem no Windows puro).

.PHONY: test test-flake lint env up db down logs logs-relay demo-client migrate-up migrate-down replay logs-participant logs-processor

test:
	go test -race ./...

# Repete os testes de concorrência 20 vezes sob -race: critério de pronto da Fase A.
test-flake:
	go test -race -count=20 -run Concorrencia ./internal/adapters/postgres/...

lint:
	golangci-lint run ./...

# Cria o .env com um JWT_SECRET aleatório, se ainda não existir. Nunca sobrescreve.
env:
	@test -f .env || { printf 'JWT_SECRET=%s\n' "$$(openssl rand -hex 32)" > .env && echo ".env criado"; }

# Sobe tudo (Postgres, migrations, Redpanda, API, relay, processor, participante) e espera a API ficar pronta (/readyz).
up: env
	docker compose up -d --build --wait

# Só o Postgres, para rodar a API ou os binários direto com `go run`.
db:
	docker compose up -d postgres

down:
	docker compose down

logs:
	docker compose logs -f api

logs-relay:
	docker compose logs -f relay

logs-participant:
	docker compose logs -f participant

logs-processor:
	docker compose logs -f processor

# Cadastra um cliente e imprime o segredo uma única vez: make demo-client ID=loja-a
ID ?= demo
demo-client: up
	docker compose run --rm --no-deps --entrypoint clientctl api create $(ID)

migrate-up:
	docker compose run --rm migrate up

migrate-down:
	docker compose run --rm migrate down -all

# Stub: o replay só existe na sessão 24 (Fase E).
replay:
	@echo "replay ainda não implementado (sessão 24)"; exit 1
