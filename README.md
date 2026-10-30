<!--
README.md: entry point of the payrouter repository (v1, end of Phase B).
Purpose: someone who clones it can bring up the system, register a client and create an
instruction without asking anyone anything. It also says what the system is, what its thesis
is, what state it is in and what does not exist yet. The final README (architecture,
measurements, limitations) comes in Phase G.
-->

# payrouter

A service that receives payment instructions, routes each one to an external participant
and treats the **unknown state as a first-class state**: when a send times out, the system
does not know whether the operation happened, records that uncertainty and resolves it
through active reconciliation.

## Thesis

Correctness when the transaction no longer exists. There is no `ROLLBACK` for a message
already sent to another system, so the project documents defensible decisions under
uncertainty: event sourcing, a transactional outbox, consumers idempotent by version and
reconciliation by status query, never by resending.

This already shows at the HTTP edge. If a request's deadline expires in the middle of the
commit, the server does not know whether the instruction was stored: it answers `504` and the
client retries with the same `Idempotency-Key`, which is safe because the key is stored in
the same transaction as the event.

## Current state

Phases A and B are complete (sessions 1 to 8): domain with the state machine, event store in
Postgres with optimistic concurrency, accepting an instruction with scoped idempotency, HTTP
API with temporary-token authentication, correlation ID, structured logs, explicit deadlines,
health probes and graceful shutdown.

Phase C is complete (sessions 9 to 12): every event is stored together with an outbox
row, in the same transaction, and the relay publishes the outbox to Kafka (Redpanda) with the
key equal to the `aggregate_id`. Delivery is at-least-once: if the relay dies between
publishing and marking, the message goes out again, and whoever consumes it is idempotent by
version.

The simulated participant (the external counterparty) exists, and the processor routes each
accepted instruction to it: without any intervention it reaches `settled` or `rejected`,
going through Kafka. If the send gets no conclusive answer the instruction becomes `unknown`,
with a reconciliation task created in the same transaction, and is never resent (session 13,
Phase D). The reconciliation loop (session 14) then asks the participant for the status of each
`unknown` instruction, with backoff and jitter, and resolves it from the answer: a `late`
instruction that timed out ends `settled` by itself. After `RECONCILE_MAX_ATTEMPTS` queries
without a conclusion it gives up and the instruction becomes `failed` (session 15). One
end-to-end test runs all the participant behaviors through the whole pipeline (session 16). A
`429` from the participant is the opposite of a timeout: definitely not processed, so the
instruction returns to `accepted` and is resent; an output rate limit with backpressure keeps it
from happening, and a per-client input limit protects the API (session 17). There is still no
projection. The full plan is in
[`docs/executionPlan.md`](docs/executionPlan.md).

## Up and running in five minutes

Requirements: Docker (with Compose) and `make`. On Windows, use Git Bash or WSL.

```bash
make up            # creates .env with a random JWT_SECRET and brings up Postgres, migrations, Redpanda, API, relay, participant and processor
make demo-client   # registers the "demo" client and prints the client_secret (exactly once)
```

With the `client_secret` in hand:

```bash
# 1. authenticate: returns a token valid for 15 minutes
curl -s localhost:8080/auth/token \
  -d '{"client_id":"demo","client_secret":"<secret>"}'

# 2. create an instruction (Idempotency-Key required, amount always in cents)
curl -si localhost:8080/instructions \
  -H "Authorization: Bearer <access_token>" -H "Idempotency-Key: my-key-1" \
  -d '{"debtor":"a","creditor":"b","amount_cents":15000,"end_to_end_id":"e2e-1"}'

# 3. repeating step 2 returns the same instruction, with the header Idempotent-Replayed: true

# 4. read it back (state rebuilt from the event store, with no lag): after a moment
#    the processor has routed it and the status is "settled"
curl -s localhost:8080/instructions/<id> -H "Authorization: Bearer <access_token>"

# 5. force a behavior with a prefix in end_to_end_id: "reject:1" ends in "rejected";
#    "late:1" ends "unknown" (the participant processed it, the answer came too late)
```

To see the instruction reach Kafka:

```bash
docker compose exec redpanda rpk topic consume payrouter.instructions.events -n 5
```

For another client: `make demo-client ID=store-a`. To follow the logs: `make logs` (API),
`make logs-relay` (relay), `make logs-processor` (processor) and `make logs-participant`
(participant).
To stop: `make down` (add `-v` to `docker compose down` to delete the database).

## The simulated participant

`participant` (port 9090) is the external counterparty. It is part of the system, not a mock:
it keeps what it processed, never processes the same `instruction_id` twice, and counts how
many times each instruction arrived (`received`).

| Behavior | Processes? | Answer |
|---|---|---|
| `accept` | yes | `accepted` at once |
| `reject` | yes | `rejected` with `REJECT_CODE` (default `insufficient_funds`) |
| `slow` | yes | `accepted` after `SLOW_DELAY` (1 s), inside the client's deadline |
| `drop` | no | never answers; the status query finds nothing |
| `late` | yes, at once | `accepted` only after `LATE_DELAY` (10 s), when the client already gave up: the cruel case |
| `crash` | no | the process exits; compose restarts it |
| `throttle` | no, on the first arrival | `429` with `Retry-After: 1`; from the second arrival on it behaves as `accept` |

The participant can also enforce a real rate limit: with `PARTICIPANT_TPS` above 0, arrivals over
that rate (with a burst of `PARTICIPANT_BURST`, default `ceil(TPS)`) get `429` and are NOT
processed. `GET /payments/{id}` reports `received` (arrivals that were admitted) and `throttled`
(arrivals refused with `429`), so `received == 1` stays the proof that nothing was sent twice.

What the processor does with each one is pinned by `TestPontaAPontaDosComportamentos`
(`cmd/processor`), which checks the exact event trail and that the participant received the
instruction once:

| Behavior | Ends | Trail after `InstructionAccepted` |
|---|---|---|
| `accept`, `slow` | `settled` (`slow` never becomes `unknown`) | `RoutingAttempted`, `ParticipantAccepted` |
| `reject` | `rejected` | `RoutingAttempted`, `ParticipantRejected` |
| `throttle` | `settled` after a resend | `RoutingAttempted`, `RoutingThrottled`, `RoutingAttempted`, `ParticipantAccepted` |
| `late` | `settled` via status query | `RoutingAttempted`, `RoutingTimedOut`, `StatusQueried`, `ParticipantAccepted` |
| `drop`, `crash` | `failed` | `RoutingAttempted`, `RoutingTimedOut`, `StatusQueried` x N, `ReconciliationExhausted` |

`drop` and `crash` look the same to the processor: both end `unknown`, then `failed`, and nothing is resent.

`PARTICIPANT_BEHAVIOR` sets the default (`accept`). A prefix in `end_to_end_id` forces the
behavior of one instruction, for example `late:order-1`. The status query is
`GET /payments/{instruction_id}`:

```bash
curl -s -m 2 localhost:9090/payments -d '{"instruction_id":"x1","amount_cents":100,"debtor":"a","creditor":"b","end_to_end_id":"late:1"}'
# the client times out, but the operation happened:
curl -s localhost:9090/payments/x1   # {"status":"accepted","received":1}
```

## The processor

`processor` consumes `InstructionAccepted` (and `RoutingThrottled`) from Kafka, sends the instruction to the participant
and records the answer. It is where the side effect lives (command side, never the projection).

- **Idempotent by the aggregate version, with no deduplication table.** It reloads the
  aggregate and acts only if the status is `accepted` at the message's version. Before sending
  it appends `RoutingAttempted` with the expected version: that append is the claim, so a
  repeated message, or two processors on the same message, send at most once.
- **The send is outside any transaction**, and the answer is a second append (3 attempts).
- **No conclusive answer is never resent.** A timeout, network error or `5xx` is an unknown
  outcome: the processor appends `RoutingTimedOut` (the instruction becomes `unknown`) and
  creates its reconciliation task in the same transaction, then the offset advances. Resending
  could charge twice; the only safe operation is to query the status.
- **A transient error (database down) repeats the same message in place**, so order per
  partition holds and lag grows instead of anything being dropped.

### Reconciliation

A loop inside the processor works the `reconciliation_tasks` queue.

- **Sweep with a lease.** Each round claims the due tasks (`next_attempt_at <= now()`) with
  `FOR UPDATE SKIP LOCKED` and pushes their due time forward by a lease, in one statement. Two
  processors never get the same task; if one dies mid-query, the task comes back when the lease
  expires.
- **The status query resolves it, never a resend.** `GET /payments/{id}` on the participant:
  `accepted` appends `StatusQueried` + `ParticipantAccepted`, `rejected` appends `StatusQueried`
  + `ParticipantRejected`, and the task is deleted, all in one transaction. `404 not_found` or a
  failed query appends `StatusQueried{unknown}` and reschedules the task. `not_found` is not
  taken as "not processed": a participant that lost the message and one that has not caught up
  look the same, so nothing is resent.
- **Backoff with jitter.** The wait doubles on each attempt from `RECONCILE_BACKOFF_BASE` up to
  `RECONCILE_BACKOFF_MAX`; half of it is fixed and half is random, so tasks that failed together
  do not query together and the wait never falls near zero.
- **Giving up.** The `RECONCILE_MAX_ATTEMPTS`-th query without a conclusion appends
  `StatusQueried{unknown}` + `ReconciliationExhausted{attempts}` in one transaction and deletes the
  task: the instruction becomes `failed`. `failed` means "gave up, a human must look", NOT "not
  processed", so nothing is resent. A conclusive answer on the last attempt still wins.
- **The cruel case is a test.** The participant processes a `late` instruction but answers after
  the client timed out: the instruction goes `unknown`, reconciliation queries and finds it
  processed, and it ends `settled` with the participant's `received` still 1
  (`cmd/processor/cruel_test.go`).

| Variable | Default | Effect |
|---|---|---|
| `DATABASE_URL`, `KAFKA_BROKERS` | (required) | Postgres and Kafka |
| `PARTICIPANT_URL` | (required) | e.g. `http://participant:9090` |
| `PARTICIPANT_TIMEOUT` | `3s` | deadline of each send; `late` (10 s) does not fit, `slow` (1 s) does |
| `RECONCILE_FIRST_DELAY` | `5s` | how long after the timeout the first status query is due |
| `RECONCILE_BATCH` | `50` | tasks claimed per round |
| `RECONCILE_POLL` | `1s` | wait between empty rounds |
| `RECONCILE_LEASE` | `30s` | how long a claimed task stays out of the queue |
| `RECONCILE_BACKOFF_BASE`, `RECONCILE_BACKOFF_MAX` | `5s`, `1m` | wait between status queries: doubles from the base, capped at the max |
| `RECONCILE_MAX_ATTEMPTS` | `10` | status queries without a conclusion before giving up (`failed`) |
| `OUTPUT_TPS` | `0` (off) | output rate limit toward the participant, in sends per second |
| `OUTPUT_BURST` | `ceil(OUTPUT_TPS)` | burst allowed above `OUTPUT_TPS` |
| `KAFKA_GROUP` | `payrouter-processor` | consumer group |

### Rate limits and 429

Two failures that look alike in the log and must never be handled alike:

| Participant answer | What it proves | Reaction |
|---|---|---|
| `429` | definitely NOT processed | `RoutingThrottled` returns the instruction to `accepted` and the message of that event resends it |
| timeout, `5xx`, unreadable | I don't know | `unknown`, a reconciliation task, and only a status query: never a resend |

- **A throttled instruction is re-driven through the outbox.** `RoutingThrottled` is appended like
  any event, the relay publishes it, and the processor handles it exactly as `InstructionAccepted`:
  it acts only if the instruction is `accepted` at the message's version, so a repeated or stale
  message is skipped. After recording the `429` the handler waits the participant's `Retry-After`
  (at most 10 s), which holds the partition: backpressure on a participant that said to slow down.
  A `429` on a status query tells nothing about the instruction and stays an unknown outcome.
- **The output limit comes before the claim.** With `OUTPUT_TPS` set, each send reserves a token
  first. If none is available the use case writes NOTHING (no `RoutingAttempted`, so a crash while
  waiting leaves no instruction stuck `in_flight`) and asks the consumer to wait. The consumer
  pauses that partition, waits, resumes it, and repeats the SAME message: the offset does not
  advance, order is kept, nothing is dropped or piled up in memory. Lag grows, and that is correct:
  the instruction waits. The limiter is the token bucket from wallet-go, reused (not a
  contribution of this project).
- **The input limit protects the API.** With `API_RATE_RPS` set, each authenticated client gets
  its own bucket on `POST /instructions` and `GET /instructions/{id}`; over the limit it gets
  `429 rate_limited` with `Retry-After`, and the use case does not run. This `429` (API to
  client) is unrelated to the participant's `429`.

## Development

```bash
make db           # Postgres only, to run the API with `go run ./cmd/api`
make migrate-up   # applies the migrations (`make up` already does this)
make test         # go test -race ./... (the integration tests use Docker)
make test-flake   # the concurrency tests 20 times under -race
make lint
```

To run the API outside Docker, export `DATABASE_URL`
(`postgres://payrouter:payrouter@localhost:5432/payrouter?sslmode=disable`) and `JWT_SECRET`
(the value from `.env`) and use `go run ./cmd/api`. Clients: `go run ./cmd/clientctl create <id>`.

## API

| Route | Response |
|---|---|
| `POST /auth/token` | `200` with `access_token`; `401 invalid_credentials`, identical for an unknown client, a wrong secret and an inactive client |
| `POST /instructions` | `202` with `Location`; repeating the same key returns the same response with `Idempotent-Replayed: true`; the same key with a different body is `422` |
| `GET /instructions/{id}` | `200`, `404` if it does not exist, `400` if the id is not a UUID |
| `GET /healthz`, `GET /readyz` | public probes, see below |

Errors always have the format `{"error":{"code":"...","message":"..."}}`. A missing,
invalid or expired token gives `401` with `WWW-Authenticate: Bearer`: the client authenticates again.
An already issued token is valid until it expires even if the client is deactivated
(`clientctl deactivate`), at most 15 minutes.

### Configuration

| Variable | Default | Effect |
|---|---|---|
| `DATABASE_URL` | (required) | Postgres connection |
| `JWT_SECRET` | (required) | signs the tokens; at least 32 bytes |
| `ADDR` | `:8080` | listen address |
| `LOG_FORMAT` | `json` | `text` writes human-readable logs to the terminal |
| `REQUEST_TIMEOUT` | `5s` | deadline of each request, passed on to Postgres through the context |
| `API_RATE_RPS` | `0` (off) | requests per second allowed per client; over it, `429 rate_limited` |
| `API_RATE_BURST` | `ceil(API_RATE_RPS)` | burst allowed above `API_RATE_RPS` |

### Observability and deadlines

- **Correlation ID**: the client's `X-Correlation-ID` header is reused if it has up to 64
  characters from `[A-Za-z0-9._-]`; otherwise the server generates a UUID. It comes back in every
  response (errors included) and appears in every log line of the request.
- **Access log**: one line per request with `method`, `route` (the pattern, such as
  `GET /instructions/{id}`), `status`, `duration_ms` and `client_id`. It never records the token,
  the body or amounts.
- **Deadline exceeded**: `504 request_timeout`, with an unknown result. A client that
  gives up the connection shows as `499` in the logs.
- **Probes** (public, no access log): `GET /healthz` says whether the process is alive and
  never queries the database; `GET /readyz` pings Postgres (2 s limit) and answers `503`,
  with no detail in the body, if it does not respond.
- **Server deadlines**: `ReadHeaderTimeout` 5 s, `ReadTimeout` 10 s, `WriteTimeout` 15 s
  (larger than the request deadline so the 504 fits), `IdleTimeout` 60 s; pool with
  `ConnectTimeout` 5 s and connections renewed every 30 min.

### Shutdown

On receiving `SIGINT` or `SIGTERM` (`docker stop`), the API stops accepting connections, waits for
in-flight requests for up to 10 s and only then closes the Postgres pool. A request that
does not finish within that deadline is cut, and for the client that is the same unknown
result as the `504`: retry with the same `Idempotency-Key`. A second signal kills the process immediately.

## Known limitations (v1)

- An instruction the participant never recognizes (for example `drop`) ends `failed` after the
  attempt limit, with no alert or escalation: someone has to look at `failed` instructions.
- The reconciliation queries one task at a time, so a large batch of slow queries can outlast the
  lease and a task may be queried twice. Queries are safe and the write is guarded by the version.
- An instruction can still stay `in_flight` forever if the processor dies between claiming and
  sending, or if even the `unknown` record cannot be written.
- There is no dead-letter queue: a message without the expected headers is logged and skipped.
- The processor handles one message at a time (no worker pool yet), so a partition waiting on the
  output limit also delays the others read by the same consumer.
- The output limiter lets several waiting callers wake together and compete for one token; fine
  for a sequential consumer, to revisit when the elastic pool processes in parallel.
- A message the processor skips (repeated or stale) still spends one output token.
- The input limiter keeps one bucket per client forever (clients are registered by an admin, so
  the set is small); expire idle buckets if registration is ever opened.
- The participant keeps its state in memory: a `crash` or restart forgets everything it
  processed.
- At-least-once delivery: the same event can appear twice in the topic after the relay
  dies. The processor discards what it has already seen by version.
- Published outbox rows are not deleted: there is no cleanup routine yet.
- Token without refresh and without revocation before it expires; no rate limit on `/auth/token`.
- No TLS: meant to run behind a proxy or only on a local network.
- The Postgres development secrets are in `docker-compose.yml`; do not use them outside the
  local environment.

## Documentation

- [`docs/escope.md`](docs/escope.md): objective, scope, aggregate, data model, decisions and experiments
- [`docs/executionPlan.md`](docs/executionPlan.md): phases, sessions, risks and defense questions
- [`docs/commitpattern.md`](docs/commitpattern.md): commit message pattern
