# payrouter — Payment instruction router with an unknown state

> Scope document. Companion to the execution plan. Working name; change it if you
> want.

---

## 1. Objective

### What the system is

A service that receives payment instructions, routes each one to an external participant
and treats **the unknown state as a first-class state**: when a send times out, the system
does not know whether the operation happened, records that uncertainty and actively
resolves it through reconciliation.

That sentence goes at the top of the README. Anything that does not support it is out of scope.

### The thesis, and why it differs from the previous project's

[wallet-go](https://github.com/VictorM0nteiro/wallet-go-) proves correctness **inside** a
transaction: lock, zero-sum invariant, rollback, all or nothing. That is why it is strong,
and it is also its declared limit.

This project proves correctness **when the transaction no longer exists**. There is no
`ROLLBACK` that undoes a message sent to another system; there is no `COMMIT` that makes
two consumers consistent at the same instant. What is left are defensible decisions under
uncertainty, and that is what the project documents.

If the thesis were the same as wallet-go's, the second project would be redundant. The
yardstick for any addition here: **does this only exist because the boundary is
asynchronous?** If the answer is no, it is probably repetition.

### Event sourcing in five lines

The state of an instruction is not a column that gets updated. It is the sequence of events
that happened to it, stored in order and never edited. The current state is obtained by
*rebuilding*: start from zero and apply each event in order.

Three consequences the project explores:

1. **The history is the information, not a by-product.** "This instruction was attempted
   three times, timed out on the first two and the participant confirmed on the status
   query" is a question a `status` field cannot answer and a sequence of events answers
   effortlessly.
2. **Projections are disposable.** Any read view is a pure function of the events, and so
   it can be thrown away and rebuilt. That is what makes a projection bug recoverable
   instead of permanent.
3. **A side effect cannot live in the projection.** If the message were sent while
   processing an event, a rebuild would resend everything. It is the decision that organizes
   the whole architecture (ADR 2).

### The unknown-state problem

You send the instruction to the participant and the socket times out. From your side, two
worlds are indistinguishable:

- the message did not arrive, and nothing happened
- the message arrived, was processed, and only the response was lost

No local information separates the two. Telling the client "it failed" can be a lie — and,
in a payment system, it is the expensive lie. Telling them "it worked" is one too.

The only honest way out is a third state: `unknown`. And what defines the quality of the
system is what it does with that state, because `unknown` **does not resolve itself**.

### What the project proves

That you know how to design an event-sourced aggregate with optimistic concurrency; keep a
consumer correct under at-least-once delivery and reordering; separate side effects from
projection so that replay is safe; resolve uncertainty by reconciliation instead of
guessing; and measure the cost of eventual consistency instead of asserting it.

---

## 2. Scope

### In

- Receiving and validating payment instructions, with an `Idempotency-Key`
- Event store in Postgres with optimistic concurrency by aggregate version
- Transactional outbox and a relay publishing to Kafka
- Asynchronous routing to a simulated external participant
- Timeout handling: the `unknown` state and active reconciliation with backoff
- Document projection in MongoDB, with an idempotent consumer
- Status cache in Redis
- Full projection replay
- Observability with OpenTelemetry and the four measurements of section 7
- Simulated participant with configurable behavior, including lying
- **Output** rate limit toward the participant, with backpressure on the consumer
- Per-client input rate limit (ported from wallet-go, declared as reuse)
- **Elastic worker pool** in the processor and **elastic connection pool** toward the
  participant, both preserving order per `aggregate_id` (decision 10)

### Out (deliberately)

- Ledger, balance, double entry — that is wallet-go, and repeating it wastes the project
- Integration with SPI, Bacen, Dict or any real system
- Authentication beyond client_id + secret exchanged for a short JWT (15 min, no refresh, no revocation, no full OAuth or roles)
- Multi-currency, scheduling, recurring instructions
- A frontend of any kind
- Saga with compensation across multiple aggregates
- Kubernetes, service mesh, cloud (extras queue)

**Scope rule:** when facing "do I add a feature or deepen what exists", deepen. One
measured, documented flow is worth more than five endpoints.

---

## 3. The aggregate

One `PaymentInstruction`. All of the project's complexity fits in its lifecycle.

### State machine

```
         accepted
             │ route
             ▼
         in_flight ──────┬──────────┐
             │           │          │
        ok response   rejected    timeout
             │           │          │
             ▼           ▼          ▼
          settled    rejected    unknown
                                    │
              status query (StatusQueried, records)
                       ┌────────────┼────────────┐
                       ▼            ▼            ▼
                   settled      rejected      unknown
                                           (reschedules; after N
                                            attempts, failed
                                            with manual
                                            intervention recorded)
```

`settled`, `rejected` and `failed` are terminal. `unknown` is the only state that requires
background work to leave itself.

There is a fourth way out of `in_flight` that the diagram above omits to keep it clean: the
participant answering `429`. That **returns** the instruction to `accepted`, awaiting a new
attempt, and it is the opposite of a timeout — `429` means definitely not processed, so
resending is safe. On the normal path this should not happen, because the output rate limit
limits itself beforehand; when it does, it means the limit is misconfigured, and the event
records that.

### Events

| Event | Meaning | Transition |
|---|---|---|
| `InstructionAccepted` | Validated and persisted, nothing sent | → `accepted` |
| `RoutingAttempted` | Message dispatched to the participant | → `in_flight` |
| `ParticipantAccepted` | Participant confirmed (in the response or discovered during reconciliation) | `in_flight` or `unknown` → `settled` |
| `ParticipantRejected` | Participant denied, with a code (same) | `in_flight` or `unknown` → `rejected` |
| `RoutingTimedOut` | No response within the deadline | → `unknown` |
| `StatusQueried` | Reconciliation query made, with the result | `unknown` → `unknown` (only records) |
| `RoutingThrottled` | Participant returned `429`, nothing was processed | → `accepted` |
| `ReconciliationExhausted` | N queries without a conclusion | → `failed` |

Eight events. An illegal transition is rejected **in the domain**, with a typed error — not
by a database constraint, not by an `if` scattered through the handler.

`StatusQueried` never changes the state: it records the query and what the participant
answered. What resolves is `ParticipantAccepted` or `ParticipantRejected`, emitted right
after when the query is conclusive. They are two separate facts ("I queried" and "I
resolved"), which keeps reconciliation auditable and matches the timeline of the Mongo
document in section 4.

---

## 4. Data model

```sql
-- The event store. Single source of truth.
CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_id    UUID    NOT NULL,
    aggregate_type  TEXT    NOT NULL,
    version         INT     NOT NULL CHECK (version > 0),
    event_type      TEXT    NOT NULL,
    payload         JSONB   NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (aggregate_id, version)
);

CREATE INDEX events_aggregate_idx ON events (aggregate_id, version);

-- Transactional outbox: written in the SAME transaction as the event.
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    event_id      BIGINT NOT NULL REFERENCES events(id),
    aggregate_id  UUID   NOT NULL,
    topic         TEXT   NOT NULL,
    payload       JSONB  NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;

-- Work queue of the process manager.
CREATE TABLE reconciliation_tasks (
    aggregate_id     UUID PRIMARY KEY,
    attempts         INT  NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX reconciliation_due_idx ON reconciliation_tasks (next_attempt_at);

-- Command-side idempotency. Same contract as wallet-go.
CREATE TABLE idempotency_keys (
    scope                TEXT NOT NULL,
    endpoint             TEXT NOT NULL,
    key                  TEXT NOT NULL,
    request_fingerprint  TEXT NOT NULL,
    state                TEXT NOT NULL,
    status_code          INT,
    response_body        JSONB,
    aggregate_id         UUID,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, endpoint, key)
);
```

### Invariants tested in CI

```sql
-- 1. An aggregate's versions are contiguous from 1, with no gap.
SELECT aggregate_id
FROM events
GROUP BY aggregate_id
HAVING COUNT(*) <> MAX(version) OR MIN(version) <> 1;

-- 2. Every event has exactly one outbox row.
SELECT e.id FROM events e
LEFT JOIN outbox o ON o.event_id = e.id
GROUP BY e.id
HAVING COUNT(o.id) <> 1;
```

Both must return zero rows, always.

### Projection in MongoDB

One document per instruction. The nested timeline is what justifies a document instead of
a table: it is read together with the rest, never queried separately.

```json
{
  "_id": "7c9e...",
  "version": 5,
  "status": "settled",
  "amount_cents": 15000,
  "debtor": "...",
  "creditor": "...",
  "end_to_end_id": "...",
  "attempts": 3,
  "timeline": [
    { "event": "InstructionAccepted", "version": 1, "at": "..." },
    { "event": "RoutingAttempted",    "version": 2, "at": "..." },
    { "event": "RoutingTimedOut",     "version": 3, "at": "..." },
    { "event": "StatusQueried",       "version": 4, "at": "...", "result": "accepted" },
    { "event": "ParticipantAccepted", "version": 5, "at": "..." }
  ],
  "updated_at": "..."
}
```

The `version` in the document is the consumer's idempotency mechanism (ADR 3).

### Cache in Redis

`status:<aggregate_id>` → `{"status": "...", "version": N}`, with a TTL. The version
travels in the value so the client can detect staleness instead of trusting blindly.

---

## 5. Decisions already locked

**1. Optimistic concurrency by expected version.** To write version 4, the command asserts
it read version 3. If someone else wrote in between, `UNIQUE (aggregate_id, version)`
refuses it and the command rereads and tries again. No row lock, no `SERIALIZABLE`.

This closes a trilogy with the previous project: pessimistic lock, serializable isolation
and optimistic version are three answers to the same problem, and you can say when each one
fits.

**2. A side effect lives on the command side, never in the projection.** Sending to the
participant is an external effect. Projections are a pure function of the events and can be
rebuilt; the send is a reaction to a command, not to a replay.

**3. The consumer's idempotency is the version, not a deduplication table.** The
projection stores `version` and discards an event with a lower or equal version. It costs
one field and covers redelivery and reordering with the same mechanism.

**4. Kafka partition by `aggregate_id`.** Order is guaranteed within one instruction, which
is where it matters. Between different instructions it does not matter, and no global
ordering is imposed — saying so is stronger than imposing one.

**5. Reading one aggregate is authoritative; reading a list is eventual.**
`GET /instructions/{id}` rebuilds from the event store, which for a single aggregate is
cheap. `GET /instructions?status=unknown` reads the projection. This dissolves
read-your-writes without inventing any machinery, and it is explicit in the API contract.

**6. At-least-once delivery, assumed.** The relay publishes and only then marks
`published_at`. Dying midway republishes; the reverse order would lose an event. The
consequence is in item 3.

**7. The simulated participant is part of the system, not a mock.** Its own process, with
behavior configurable through environment variables: accepts, rejects, is slow, times out,
**answers after the timeout** (the cruel case: it processed, but you did not find out) and
goes down. It is what makes the thesis testable.

**8. Money as `int64` cents.** Same rule as the previous project: never float, in any
layer, not even in logs or JSON.

**9. Output rate limit with backpressure, not dropping.** The participant has a
configurable TPS limit. When input exceeds that limit, the processor **slows its
consumption** — pauses the partition and does not advance the offset — instead of dropping
a message or piling up in memory. The consequence is that lag grows, and that is correct:
the instruction is not lost, it waits.

`429` and timeout demand opposite reactions, and this is the most important distinction in
the project after `unknown` itself: `429` is "definitely not processed", so resending is
safe; timeout is "I don't know", so the only safe operation is to query the status. Two
failures that look the same in the log and cannot be handled the same way.

The per-client **input** rate limit is wallet-go's token bucket, ported. It is declared as
reuse in the README, because it is not a contribution of this project.

**10. Elastic concurrency that never breaks per-aggregate order.** The processor routes in
parallel, but two events of the same `aggregate_id` never run at the same time or out of
order. This only exists because the boundary is asynchronous: the parallelism comes from the
arrival of independent messages, and the limit comes from the external participant.

- **Elastic worker pool** (`internal/elastic`, generic): between `min` and `max`
  goroutines, grows when the internal queue and the wait time rise, shrinks on idleness,
  with hysteresis to avoid oscillating. Key-affinity dispatch: the same aggregate always
  lands on the same lane while it has in-flight work (a map of active keys), so resizing the
  pool reorders nothing. The offset only advances up to the **contiguous watermark**:
  finishing message 7 before 5 does not commit 7.
- **Elastic connection pool** toward the participant: an adaptive in-flight request limit
  (AIMD: additive increase on success, multiplicative decrease on timeout or `429`), plus an
  `http.Transport` with `MaxConnsPerHost` following the current limit. An idle connection
  is closed after a TTL. The ceiling is the output rate limit; the pool does not replace it.
- **Defensible sizing (Little's Law):** workers needed ≈ target TPS × the participant's p99
  latency. Measure it, do not guess `max`.
- **Postgres uses a static `pgxpool`**, sized and measured. `pgxpool` does not resize at
  runtime and reinventing it adds nothing to the thesis; declared as a limitation.
- A timeout is still "I don't know": the pool never resends on its own. Lowering the limit
  on timeout is only load control, and the instruction goes to `unknown` as usual.

---

## 6. Architecture

Hexagonal, in its minimal version, for the same practical reason as always: the domain must
be testable without Kafka, without Mongo and without Docker.

```
  POST /instructions
  GET  /instructions/{id}        ┌──────────────┐
  GET  /instructions?...    ───▶ │     api      │  command + authoritative read
                                 └──────┬───────┘
                                        │ one SQL transaction:
                                        │ events + outbox
                                        ▼
                                 ┌──────────────┐
                                 │  PostgreSQL  │  event store · outbox · tasks
                                 └──────┬───────┘
                                        │
                                 ┌──────▼───────┐
                                 │    relay     │  FOR UPDATE SKIP LOCKED
                                 └──────┬───────┘
                                        ▼
                                   ┌─────────┐
                                   │  Kafka  │  partition by aggregate_id
                                   └────┬────┘
                        ┌───────────────┴───────────────┐
                        ▼                               ▼
                 ┌──────────────┐               ┌──────────────┐
                 │  processor   │               │  projector   │
                 │ routes +     │               │ Mongo +      │
                 │ reconciles   │               │ Redis        │
                 └──────┬───────┘               └──────────────┘
                        │ HTTP
                        ▼
                 ┌──────────────┐
                 │ participant  │  simulated, configurable, can lie
                 └──────────────┘
```

```
cmd/
  api/            HTTP: command and authoritative read
  relay/          outbox → Kafka
  processor/      process manager: routing + reconciliation (side effect)
  projector/      Kafka → Mongo + Redis (pure function, replayable)
  participant/    simulated participant
  loadtest/       load and measurement harness
internal/
  domain/         PaymentInstruction, events, state machine, Money
                  — zero imports of pgx, net/http, kafka, mongo
  app/            use cases + ports
  elastic/        WorkerPool and ConnPool/adaptive limiter, generic, with no
                  dependency on Kafka or HTTP — testable in isolation
  adapters/
    http/         handlers, DTOs, domain error → status
    postgres/     event store, outbox, tasks, idempotency
    kafka/        producer, consumer
    mongo/        projection
    redis/        cache
    otel/         instrumentation
```

**Test that the hexagonal architecture is not decoration:** `go test ./internal/domain/...`
runs in milliseconds and covers the whole state machine, illegal transitions included,
without any container.

---

## 7. The experiments

Four measurements, all coming out of infrastructure the project builds anyway.

### 7.1 Projection lag

**The question:** how long between the event commit and the projection reflecting it, under
load, and how does that scale with the number of consumers?

| Condition | Variable |
|---|---|
| Consumers | 1, 2, 4 |
| Load | sustained harness rate, two levels |

Record the lag p50/p95/p99, the throughput of processed events, and the point at which the
consumer stops keeping up with production.

### 7.2 Replay time

**The question:** how much does it cost to rebuild the projection from zero, as a function
of the number of events?

Measure with 10k, 100k and 1M events. The result is the honest answer to "what if the
projection gets corrupted?" — and it is also the batch layer of section 11.

### 7.3 Cached read against rebuild

**The question:** what is the latency of `GET /instructions/{id}` rebuilding from the event
store, and how much does the status cache save?

Record the latency of both paths and the cache hit rate. This is the least interesting of
the four and the first to cut if time gets tight.

### 7.4 Participant saturation

**The question:** when the input rate exceeds the output limit, does the system degrade
gracefully or collapse?

| Condition | Variable |
|---|---|
| Input | 50/s (at the limit), 100/s, 200/s |
| Output limit | fixed at 50/s |
| Concurrency | fixed pool (1, 8, 32) against elastic pool (min 1, max 32); burst: 10 s at 200/s between periods at 20/s |

Record, besides what is below: active goroutines over time, resize events, routing p99 and
the time until the pool shrinks after the burst. The hypothesis to write beforehand: the
elastic pool matches the throughput of the large fixed pool with far fewer idle goroutines,
at the cost of a latency spike at the start of the burst. If the large fixed pool always
wins, elasticity is not justified and the report says so.

Record: growth of the reconciliation queue, projection lag, `POST` latency (it must keep
answering fast — it only writes an event), and whether the consumer recovers when input
returns to normal.

The expected result is that lag grows proportionally and nothing is lost. If anything is
lost, the backpressure is wrong, and finding that out by measuring beats finding it out in
an interview question.

**Methodology, for all four:** hardware, OS, versions, parameters, seed and the exact
command recorded. Reproducibility is half of credibility; the other half is explaining
*why* the number is what it is.

And the lesson from the previous project, already learned: the load generator, the services
and the databases competing for the same machine produce noise that can exceed the measured
effect. Declare that under "threats to validity" instead of hiding it.

---

## 8. ADRs to write

Short files in `docs/adr/`: context, options considered, decision, consequences.

| # | Decision | What to defend |
|---|---|---|
| 1 | Event sourcing instead of a status column | The history of attempts is the information. **Assumed cost:** reading one aggregate becomes a rebuild |
| 2 | Side effect outside the projection | Without it, replay resends messages. It is what makes the projection disposable |
| 3 | Consumer idempotency by version | Covers redelivery and reordering with one field, instead of a deduplication table |
| 4 | Optimistic concurrency by expected version | Compare with the previous project's pessimistic lock and `SERIALIZABLE`: when each one fits |
| 5 | `unknown` as a first-class state | A timeout is not a failure. Answering "failed" without knowing is the expensive lie in a payment system |
| 6 | Partition by `aggregate_id`, no global order | Order where it matters, and nothing beyond. Why global ordering would be cost with no benefit |
| 7 | Authoritative aggregate read, eventual list | Dissolves read-your-writes without extra machinery; the contract stays honest |
| 8 | At-least-once instead of at-most-once | Publish before marking. The alternative loses an event, which is worse than duplicating |
| 9 | Backpressure instead of dropping at the output limit | A payment instruction is not dropped: it waits. And why `429` and timeout, which look like the same failure, demand opposite reactions |
| 10 | Elastic pools with per-aggregate serialization | Parallelism without breaking order: key affinity + offset watermark. Why `pgxpool` stayed static. When elasticity does not pay off |

ADR 5 is the most valuable. It is the only one without a clean answer, and that is why it
shows judgment instead of knowledge.

---

## 9. Stack

```
Language    Go 1.25+
HTTP        net/http + chi
Event store PostgreSQL 16 · pgx/v5 · golang-migrate
Messaging   Kafka · franz-go (or segmentio/kafka-go)
Projection  MongoDB · mongo-driver
Cache       Redis · go-redis
Tests       testify · testcontainers-go · go test -race
Observab.   OpenTelemetry · Prometheus · Grafana · log/slog
Load        in-house harness in Go
Profiling   go test -bench -benchmem · pprof
Infra       Docker · Docker Compose
CI          GitHub Actions · golangci-lint
```

Standard library whenever the difference is small. On observability: OTel is vendor-neutral
and exports to Datadog and New Relic — you can say that truthfully without paying anything.

The metrics (Prometheus client, Grafana dashboard) arrive in session 17b, before the elastic
pools, because those sessions need numbers while they happen. They reuse the wallet-go setup
(own registry per binary, nil-safe recorders, route pattern as label). Labels are closed sets,
never `aggregate_id`. The OTel trace stays in Phase F.

---

## 10. Structure of the final README

1. One sentence saying what the system does
2. The thesis, in three lines: what changes when the transaction ends
3. Architecture diagram and the five processes
4. The state machine, with `unknown` highlighted
5. Event sourcing primer in five lines
6. Design decisions, with a link to each ADR
7. Measurement results: the four tables and the methodology
8. Induced-failure report: hypothesis, observed, divergence
9. How to run: `docker compose up` and the tests passing
10. **Known limitations**

Item 10 is the one that impresses most, and here it has plenty of material: no saga, no
multi-region, no global ordering, a simulated and not a real participant, reconciliation
without real human escalation.

---

## 11. Extras queue

Only after everything above, in this order:

1. **Batch layer in the shape of the Lambda Architecture** — scheduled rebuild by full
   replay as the authoritative correction path, coexisting with the incremental projection.
   Replay already exists because of experiment 7.2; the extra is scheduling it, comparing the
   result of the two layers and recording the divergence when there is one. **Honest
   description:** "a rebuild layer by replay plus an incremental layer, in the shape of the
   Lambda Architecture", plus the explanation of why Kappa replaced it in practice
2. **DLQ and poison messages** — an event the consumer cannot process after N attempts goes
   to a dead topic, with a test
3. **Single-host deploy** with Terraform and Caddy
4. **Managed AWS** — RDS, MSK or Kafka on EC2, ElastiCache
5. **Elasticsearch as a third projection** — search by instruction; closes the item that was
   left out of the selection

---

## 12. How to start

The first session produces, in this order:

1. Repository, folder structure, CI with lint and tests running since the first commit
2. `CLAUDE.md` with the inviolable rules
3. Migration 001 with `events`, `outbox`, `reconciliation_tasks` and `idempotency_keys`
4. The state machine in the domain, with the eight events and their tests — **without a
   database, without Kafka, without anything**

Start with the domain. The state machine is the only part of the system you need to know by
heart to defend the project out loud, and it is the cheapest to build.

---

## 13. Coverage of the target job postings

The project exists to qualify for two postings: Software Engineer at Mercado Livre
(Go + React + AI) and Pix at Stone (Go, Kafka, Postgres, event-driven). The mapping below
is honest: what the scope covers, what it covers only in the extras queue, and what stays
**deliberately out** (the scope rule says deepen, do not widen).

| Posting requirement | Where it is in the project | Status |
|---|---|---|
| Go: APIs, concurrency, tests | the whole project; optimistic concurrency (S4); elastic worker pool and connection pool (S18-19) | Covered |
| Microservices, asynchronous systems, messaging (Kafka) | 5 processes, outbox, relay, consumers | Covered |
| Event Sourcing, Hexagonal Architecture | event-sourced aggregate, hexagonal `internal/` | Covered |
| CQRS | command (Postgres) separated from list reads (Mongo/Redis) | Covered (ADR 7) |
| Lambda Architecture | replay + incremental projection | Extra 1 (§11) |
| PostgreSQL / relational | event store, outbox, tasks | Covered (MySQL not; same family, declare it) |
| NoSQL: MongoDB, Redis | projection and cache | Covered |
| Elasticsearch | third projection | Extra 5 (§11) |
| Observability (OTel, Prometheus, Grafana) | Phase F | Covered (New Relic/Datadog: OTel exports to both) |
| Automated tests, CI | testcontainers, `-race`, GitHub Actions | Covered (Azure DevOps not; equivalent pipeline) |
| Resilience, financial market, payments | the whole thesis: `unknown`, reconciliation | Covered |
| Cloud (AWS/GCP) | Extras 3 and 4 (§11) | Extra |
| React/TypeScript | — | **Out** (§2 excludes a frontend) |
| AI: agents, MCP, tools, function calling | — | **Out** |

React and AI are central requirements only of the Mercado Livre posting and are another
project, not a feature of this one. If you go after that posting, the natural path is a
small separate project (an MCP Server in Go exposing `GET /instructions` and the status
query as tools), after this one. That is your decision; nothing here includes it.
