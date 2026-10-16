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

Ainda não existe API, Kafka, projeção nem participante simulado. O plano completo está em
[`docs/executionPlan.md`](docs/executionPlan.md).

## Como rodar

Requisitos: Go, Docker e `make` (no Windows, via Git Bash ou WSL).

```bash
make up           # sobe o Postgres 16
make migrate-up   # aplica as migrations
make test         # go test -race ./... (os testes de integração usam o Docker)
make lint
```

## Documentação

- [`docs/escope.md`](docs/escope.md): objetivo, escopo, agregado, modelo de dados, decisões e experimentos
- [`docs/executionPlan.md`](docs/executionPlan.md): fases, sessões, riscos e perguntas de defesa
- [`docs/commitpattern.md`](docs/commitpattern.md): padrão das mensagens de commit
