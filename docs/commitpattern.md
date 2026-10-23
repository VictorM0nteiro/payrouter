# Commit pattern for this project

Format: `type(optional scope): short summary in the imperative`

## Types used

- `feat` — new functionality (e.g. `feat: rate limiting per API key (ADR 6)`)
- `fix` — bug or CI fix (e.g. `fix: resolve golangci-lint failures blocking CI`)
- `perf` — performance optimization, with the scope naming the strategy (e.g. `perf(locking): pipeline lock/balance and insert queries, cut round trips 8→4`)
- `docs` — documentation, ADRs, reports (e.g. `docs: close Phase D — 5 ADRs, performance report skeleton, final README`)
- `test` — tests/benchmarks without a behavior change (e.g. `test: close Session 9 — pprof on locking, benchmem on the domain`)

## Subject (first line)

- Lowercase after the type, no trailing period.
- When the commit closes a plan/backlog item or a documented decision, reference it in parentheses: `(ADR 6)`, `(X5, ADR 7)`, `(Phase D)`.
- Short and specific — say what changed, not "miscellaneous updates".

## Body

- A list with `-`, one item per relevant file/decision, not in chronological order.
- Each item explains the **why**, not only the what — the trade-off considered, why an approach was dropped, what the test proves.
- When something changed course midway (e.g. a transaction per row instead of per batch), say so explicitly instead of hiding it.
- No decorative quotes and no bold/italic inside the body — plain running text.
- Close by stating how it was verified (`go test -race ./...`, clean lint, manual test against the real container) when relevant.


## Full example (from this project)

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
