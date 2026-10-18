<!--
README.md: porta de entrada do repositório payrouter.
Objetivo: dizer em poucas linhas o que o sistema é, qual a tese, em que estado está e como
rodar. É a versão mínima; o README final (arquitetura, medições, limitações) vem na Fase G.
-->

# payrouter

Serviço que recebe instruções de pagamento, roteia cada uma para um participante externo
e trata o **estado desconhecido como estado de primeira classe**: quando o envio dá timeout,
o sistema não sabe se a operação aconteceu, registra essa incerteza e a resolve por
conciliação ativa.

## Tese

Correção quando a transação não existe mais. Não há `ROLLBACK` para uma mensagem já enviada
a outro sistema, então o projeto documenta decisões defensáveis sob incerteza: event
sourcing, outbox transacional, consumidores idempotentes por versão e conciliação por
consulta de status, nunca por reenvio.

## Estado atual

Fase A concluída (sessões 1 a 4): domínio com a máquina de estados, event store em Postgres
com concorrência otimista e teste com 32 goroutines disputando o mesmo agregado.

Fase B em andamento (sessões 5 e 6 prontas): aceitar instrução com idempotência escopada
na mesma transação, e a API HTTP com autenticação por token temporário.

Ainda não existe Kafka, projeção nem participante simulado. O plano completo está em
[`docs/executionPlan.md`](docs/executionPlan.md).

## Como rodar

Requisitos: Go, Docker e `make` (no Windows, via Git Bash ou WSL).

```bash
make up           # sobe o Postgres 16
make migrate-up   # aplica as migrations
make test         # go test -race ./... (os testes de integração usam o Docker)
make lint
```

## API

A API lê `DATABASE_URL` e `JWT_SECRET` (mínimo de 32 bytes; assina os tokens). Os clientes
ficam no banco e são cadastrados por linha de comando; o segredo aparece uma única vez.

```bash
export DATABASE_URL='postgres://...'
export JWT_SECRET='um-segredo-aleatorio-com-pelo-menos-32-bytes'

go run ./cmd/clientctl create loja-a     # imprime client_id e client_secret
go run ./cmd/api                         # ouve em :8080 (ADDR altera)

# 1. autenticar: devolve um token que vale 15 minutos
curl -s localhost:8080/auth/token \
  -d '{"client_id":"loja-a","client_secret":"<segredo>"}'

# 2. criar uma instrução (Idempotency-Key obrigatório, valor sempre em centavos)
curl -s localhost:8080/instructions \
  -H "Authorization: Bearer <access_token>" -H "Idempotency-Key: $(uuidgen)" \
  -d '{"debtor":"a","creditor":"b","amount_cents":15000,"end_to_end_id":"e2e-1"}'

# 3. consultar (estado reconstruído do event store, sem atraso)
curl -s localhost:8080/instructions/<id> -H "Authorization: Bearer <access_token>"
```

| Rota | Resposta |
|---|---|
| `POST /auth/token` | `200` com `access_token`; `401 invalid_credentials` igual para cliente inexistente, segredo errado e cliente inativo |
| `POST /instructions` | `202` com `Location`; repetição da mesma chave devolve a mesma resposta com `Idempotent-Replayed: true` |
| `GET /instructions/{id}` | `200`, `404` se não existir, `400` se o id não for UUID |

Token ausente, inválido ou expirado dá `401` com `WWW-Authenticate: Bearer`: o cliente
autentica de novo. Um token já emitido vale até expirar mesmo se o cliente for desativado
(`clientctl deactivate`), no máximo 15 minutos.

## Documentação

- [`docs/escope.md`](docs/escope.md): objetivo, escopo, agregado, modelo de dados, decisões e experimentos
- [`docs/executionPlan.md`](docs/executionPlan.md): fases, sessões, riscos e perguntas de defesa
- [`docs/commitpattern.md`](docs/commitpattern.md): padrão das mensagens de commit
