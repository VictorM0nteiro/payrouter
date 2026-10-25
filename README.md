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

Phase C is in progress (sessions 9 to 11): every event is stored together with an outbox
row, in the same transaction, and the relay publishes the outbox to Kafka (Redpanda) with the
key equal to the `aggregate_id`. Delivery is at-least-once: if the relay dies between
publishing and marking, the message goes out again, and whoever consumes it is idempotent by
version.

The simulated participant (the external counterparty) also exists now, but nothing routes to
it yet. There is still no consumer or projection: the instruction is accepted, goes to the
topic and stays in `accepted`. The full plan is in
[`docs/executionPlan.md`](docs/executionPlan.md).

## Up and running in five minutes

Requirements: Docker (with Compose) and `make`. On Windows, use Git Bash or WSL.

```bash
make up            # creates .env with a random JWT_SECRET and brings up Postgres, migrations, Redpanda, API and relay
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

# 4. read it back (state rebuilt from the event store, with no lag)
curl -s localhost:8080/instructions/<id> -H "Authorization: Bearer <access_token>"
```

To see the instruction reach Kafka:

```bash
docker compose exec redpanda rpk topic consume payrouter.instructions.events -n 5
```

For another client: `make demo-client ID=store-a`. To follow the logs: `make logs` (API),
`make logs-relay` (relay) and `make logs-participant` (participant).
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

`PARTICIPANT_BEHAVIOR` sets the default (`accept`). A prefix in `end_to_end_id` forces the
behavior of one instruction, for example `late:order-1`. The status query is
`GET /payments/{instruction_id}`:

```bash
curl -s -m 2 localhost:9090/payments -d '{"instruction_id":"x1","amount_cents":100,"debtor":"a","creditor":"b","end_to_end_id":"late:1"}'
# the client times out, but the operation happened:
curl -s localhost:9090/payments/x1   # {"status":"accepted","received":1}
```

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

- The instruction stays in `accepted`: the relay publishes it to Kafka and the participant
  exists, but there is no processor to route between them yet (session 12).
- The participant keeps its state in memory: a `crash` or restart forgets everything it
  processed.
- At-least-once delivery: the same event can appear twice in the topic after the relay
  dies. There is no consumer yet; when there is one, it will discard what it has already seen by version.
- Published outbox rows are not deleted: there is no cleanup routine yet.
- Token without refresh and without revocation before it expires; no rate limit on `/auth/token`.
- No TLS: meant to run behind a proxy or only on a local network.
- The Postgres development secrets are in `docker-compose.yml`; do not use them outside the
  local environment.

## Documentation

- [`docs/escope.md`](docs/escope.md): objective, scope, aggregate, data model, decisions and experiments
- [`docs/executionPlan.md`](docs/executionPlan.md): phases, sessions, risks and defense questions
- [`docs/commitpattern.md`](docs/commitpattern.md): commit message pattern
