# Padrão de commit deste projeto

Formato: `tipo(escopo opcional): resumo curto no imperativo`

## Tipos usados

- `feat` — funcionalidade nova (ex.: `feat: rate limiting per API key (ADR 6)`)
- `fix` — correção de bug ou de CI (ex.: `fix: resolve golangci-lint failures blocking CI`)
- `perf` — otimização de performance, com o escopo indicando qual estratégia (ex.: `perf(locking): pipeline lock/balance and insert queries, cut round trips 8→4`)
- `docs` — documentação, ADRs, relatórios (ex.: `docs: close Phase D — 5 ADRs, performance report skeleton, final README`)
- `test` — testes/benchmarks sem mudar comportamento (ex.: `test: close Session 9 — pprof on locking, benchmem on the domain`)

## Assunto (primeira linha)

- Minúsculo depois do tipo, sem ponto final.
- Quando o commit fecha um item do plano/backlog ou decisão documentada, referencia isso entre parênteses: `(ADR 6)`, `(X5, ADR 7)`, `(Phase D)`.
- Curto e específico — diz o que mudou, não "atualizações diversas".

## Corpo

- Lista com `-`, um item por arquivo/decisão relevante, não por ordem cronológica.
- Cada item explica o **porquê**, não só o quê — trade-off considerado, por que uma abordagem foi descartada, o que o teste prova.
- Quando algo foi mudado de rota no meio do trabalho (ex.: transação por linha em vez de por lote), isso é dito explicitamente, não escondido.
- Sem aspas decorativas nem negrito/itálico dentro do corpo — texto corrido.
- Fecha citando como foi verificado (`go test -race ./...`, lint limpo, teste manual contra o container real) quando relevante.

## Rodapé

Toda vez que o trabalho teve participação da IA:

Co-Authored-By: Claude Sonnet 5 noreply@anthropic.com

## Exemplo completo (deste projeto)

feat: transactional outbox + polling relay, no broker (X5, ADR 7)

- app.TransferTx gets a 5th method, AppendEvent, implemented once on
  *lockingTx and inherited by *serializableTx for free via the
  existing embed. Called from TransferService.Transfer right after
  Execute succeeds -- never on the replay path.
- internal/adapters/postgres/relay.go: OutboxRelay.PollOnce claims
  and processes one row per transaction, not one transaction per
  whole batch. Deliberate change from the first pass: a batch-wide
  transaction would roll back a failing row's own retry bookkeeping
  along with its neighbors' successful results.
- Verified with go test -race ./... and a manual interruption test
  against the real container: docker compose stop relay, a deposit
  lands unpublished, docker compose start relay, same event_id
  delivered.