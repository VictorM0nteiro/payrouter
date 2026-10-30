# payrouter — Execution plan

> Companion to the scope document. The scope defines **what** and **why**; this one defines
> **when** and **how**. 33 sessions of ~2.5 h, or ~21 weeks at 4 h per week.

---

## 1. How to work with Claude Code on this project

The lesson from the previous project is concrete and worth repeating here because it was
expensive: the analysis section of wallet-go's performance report was written by Claude, and
that became a declared limitation in the README. The text was correct; the problem is that
the conclusion the project existed for you to defend was not yours.

On this project the rule is the same, with one more front to watch: **asynchronous
architecture is easy to generate and hard to defend.** An agent writes an idempotent consumer
in four minutes. Explaining, under questioning, why idempotency is by version and not by a
deduplication table is another matter.

### Delegate without guilt

Adapter boilerplate, DTOs, table-driven test scaffolding, `docker-compose.yml`, the
Makefile, migrations, CI and lint configuration, the simulated participant, the load
harness, report formatting, an ADR draft from a decision that **you** already made.

### Do not delegate

- **`internal/domain/`** — the state machine and the aggregate rebuild. It is what the
  interview asks about and it is the cheapest part to write by hand.
- **The consumer's idempotency and the optimistic concurrency control.** They are the two
  central mechanisms of the thesis.
- **The key affinity and the offset watermark of the worker pool.** It is where parallelism
  turns into an ordering bug; the resizing itself can be delegated, the invariant cannot.
- **The decision** behind any ADR. Claude Code drafts; the choice is yours.
- **The analysis of the four measurements.** "Why does the lag grow like this with 4
  consumers" is the entire interview question.

### The acceptance rule

Do not commit code you cannot explain in two minutes, out loud, without looking. If you
cannot, ask for the explanation before accepting it; if it still does not add up, rewrite
it by hand.

### `CLAUDE.md` at the root

Create it in session 1.

```markdown
# payrouter

Payment instruction router. It treats the unknown state (a timeout with no answer)
as a first-class state, resolved by active reconciliation.

## Thesis
Correctness when the transaction no longer exists. Any addition must pass the test:
does this only exist because the boundary is asynchronous?

## Inviolable rules
- `internal/domain/` does not import pgx, net/http, kafka, mongo or redis.
- Side effects (sending to the participant) live on the command side, NEVER in the projection.
  The projection is a pure function of the events and must be replayable.
- Appending to the event store uses an expected version; the conflict is the UNIQUE (aggregate_id, version).
- The consumer is idempotent by the aggregate version, not by a deduplication table.
- Kafka partitions by aggregate_id. No global ordering is imposed.
- GET of one aggregate rebuilds from the event store (authoritative).
  GET of a list reads the projection (eventual). It is part of the API contract.
- An illegal state transition is a typed error in the domain, not a database constraint.
- Money is int64 cents. Never float, not even in logs or JSON.
- The relay publishes and THEN marks published_at. At-least-once, assumed.
- At the output limit, backpressure (pause the partition), never drop.
- Elastic pools (`internal/elastic`) never run two events of the same aggregate_id
  at the same time or out of order; the offset only advances up to the contiguous watermark.
  A pool never resends on its own: lowering the limit on timeout is load control.
- Participant 429 = not processed, resending is safe.
  Timeout = I don't know, the only safe operation is to query the status. Never confuse the two.

## Commands
make test        # go test -race ./...
make lint
make up          # docker compose up -d
make migrate-up
make replay      # rebuilds the projection from scratch

## Current state
Session N — <what is done>
```

Update the state line at the end of each session.

---

## 2. Conventions

**Branches.** Solo work: commits straight to `main`, no PRs. CI runs on `push`.

**Commits.** Imperative, one logical change per commit.

**Tests.** Table-driven by default. The case name describes the behavior:
`evento_com_versao_menor_e_descartado` (the project's test names are in Portuguese;
"an event with a lower version is discarded").

**Events.** Named in the past tense, always (`InstructionAccepted`, not `AcceptInstruction`).
A command is imperative, an event is an accomplished fact. Mixing the two up is the most
common vocabulary mistake in event sourcing and it shows up in review.

**No `pkg/`.**

---

## 3. Phases

### Phase A — domain and event store (sessions 1-4)

| Session | Deliverable |
|---|---|
| 1 | Repo, CI with lint + `-race`, `CLAUDE.md`, migration 001 with the four tables |
| 2 | The eight events, `Money`, the state machine, aggregate rebuild, illegal transitions with a typed error |
| 3 | Postgres adapter: append with expected version, aggregate load, testcontainers |
| 4 | Optimistic concurrency test: N goroutines writing the same version, exactly one wins |

**Done when:** `go test ./internal/domain/...` covers the whole state machine in under 1 s
without Docker, and the session 4 test passes under `-race` twenty times with no flake.

**Trap:** the temptation to give the aggregate a method that writes. The domain decides and
produces events; the adapter does the writing. If the domain imports pgx, phase A failed.

---

### Phase B — command and read (sessions 5-8)

| Session | Deliverable |
|---|---|
| 5 | "Accept instruction" use case + scoped idempotency with a fingerprint, in the same transaction |
| 6 | HTTP: `POST /instructions` (202), authoritative `GET /instructions/{id}`, DTOs, domain error → status; `POST /auth/token` (client_id + secret → 15-minute HS256 JWT), clients in the `clients` table, registration through `cmd/clientctl` |
| 7 | Correlation ID, `log/slog`, explicit timeouts, propagated `context`, separate `/healthz` and `/readyz` |
| 8 | `docker compose up`, full graceful shutdown, README v1 |

**Done when:** another person clones it, brings it up and creates an instruction without
asking you anything.

**Presentable milestone 1.** If a hiring process opens here, you have a working,
documented event-sourced service.

---

### Phase C — the asynchronous boundary (sessions 9-12)

| Session | Deliverable |
|---|---|
| 9 | Outbox written in the same transaction as the event; invariant 2 in CI |
| 10 | `cmd/relay` with `FOR UPDATE SKIP LOCKED` publishing to Kafka; test killing the relay midway |
| 11 | `cmd/participant`: the six configurable behaviors, including answering after the timeout |
| 12 | `cmd/processor` consuming `InstructionAccepted`, routing, and applying the response |

**Done when:** an instruction created over HTTP reaches `settled` or `rejected` without
intervention, going through Kafka, and the relay interruption test proves nothing was
lost.

**Trap:** publishing to Kafka inside the handler. That is a *dual write* and it is exactly
what the outbox exists to avoid — the event has to be in the same SQL transaction.

---

### Phase D — the unknown state (sessions 13-20)

The heart of the project. Save your energy: it is the phase that will pay off most in an
interview and the one that will break most.

| Session | Deliverable |
|---|---|
| 13 | Send timeout → `RoutingTimedOut` → `unknown`, with the reconciliation task created in the same transaction |
| 14 | Reconciliation loop: sweep by `next_attempt_at`, status query, backoff with jitter |
| 15 | `ReconciliationExhausted` after N attempts; **the cruel-case test**: the participant answers after the timeout and reconciliation discovers the operation happened |
| 16 | End-to-end test of the six participant behaviors |
| 17 | Output rate limit: limiter before the send, partition `pause`/`resume`, `RoutingThrottled` on `429`. Plus the input token bucket ported from wallet-go |
| 17b | Metrics from now on: Prometheus `/metrics` in api, relay, processor and participant (429 vs timeout, backpressure waits, reconciliation backlog and outcomes, outbox pending and age, API rate and latency), Prometheus + Grafana in compose with one provisioned dashboard. Ported from wallet-go; no tracing yet (that stays in 26). Done: consumer-group lag is NOT available from this Redpanda (v24.3.18 exposes none), so the dashboard shows records produced vs fetched and real lag by code moves to 26 |
| 18 | `internal/elastic.WorkerPool`: min/max, growth by queue and wait, shrinking with hysteresis, `aggregate_id` affinity, contiguous offset watermark. Unit tests without Kafka, under `-race` |
| 19 | Elastic connections to the participant: AIMD in-flight request limiter + `http.Transport` following the limit; pool integration into the processor |
| 20 | Slack and accumulated debt |

**Done when:** with the participant configured as "processes but does not answer", the
instruction goes through `unknown` and ends in `settled` with no manual intervention, and
the test proves that **no instruction was sent twice** on that path.

**Presentable milestone 2.** Here the project's thesis is proven. Everything after this is
engineering quality on top of an idea that already works.

**Trap:** resending the instruction instead of querying the status. Under `unknown`,
resending can charge twice — a query is the only safe operation. That distinction is ADR 5
and it is the best short answer the project gives you.

---

### Phase E — projection (sessions 21-25)

| Session | Deliverable |
|---|---|
| 21 | `cmd/projector`: Kafka consumer, Mongo projection, idempotency by `version` |
| 22 | Tests for redelivery of the same event and for an out-of-order event |
| 23 | Redis: status cache with version; `GET /instructions?status=...` through the projection |
| 24 | Full replay: a command that discards the projection and rebuilds it from the event store |
| 25 | Slack |

**Done when:** you delete the Mongo collection, run `make replay`, and the projection comes
back identical — with no message resent to the participant. That test is the proof of ADR 2.

**Trap:** the projector needing state of its own beyond the version. If it needs to query
Postgres to decide what to do with an event, the projection stopped being a pure function of
the events.

---

### Phase F — observability and measurement (sessions 26-29)

| Session | Deliverable |
|---|---|
| 26 | OTel: a trace crossing api → relay → Kafka → processor/projector. The Prometheus metrics and the Grafana dashboard already exist since 17b; here they gain the projector and the pool, and consumer lag by code (`kadm`), because the Redpanda in use exposes none (found in 17b) |
| 27 | Load harness, lag instrumentation, deterministic seed |
| 28 | Experiment 7.1: projection lag with 1, 2 and 4 consumers, two load levels |
| 29 | Experiments 7.2 (replay with 10k/100k/1M) and 7.4 (participant saturation, fixed pool against elastic). 7.3 only if time is left |

**Done when:** `docs/bench/` has the raw data and anyone can repeat the run from what is
written.

**Trap:** context propagation through Kafka does not come for free. The `traceparent` has
to travel in the message headers and be extracted on the other side, otherwise you get two
disconnected traces instead of one. It is the part that most often ends up half done.

---

### Phase G — induced failure and documentation (sessions 30-33)

| Session | Deliverable |
|---|---|
| 30 | Scripts 1-4: Kafka goes down and comes back, Mongo goes down, the processor dies midway through routing, `kill -9` between the append and the publish |
| 31 | Scripts 5-6 and `docs/resilience-report.md` with the hypotheses written beforehand |
| 32 | `docs/performance-report.md` + the 10 ADRs |
| 33 | Final README, known limitations, slack |

**The protocol, same as the previous project:** every failure becomes a hypothesis written
**beforehand**, the behavior observed afterwards, and the divergence preserved in the text.
A wrong hypothesis is not corrected retroactively — it is what proves the test was real.

The most valuable script is `kill -9` between the event append and the publish: it is the
scenario the outbox exists to cover, and it is where you demonstrate that redelivery works.

---

## 4. Risks and contingency

| Risk | Signal | Response |
|---|---|---|
| **The machine cannot handle the stack** (Postgres + Kafka + Mongo + Redis + 5 processes) | Swap, slowness, unstable numbers | Use **Redpanda** in place of Kafka for development and testing: protocol-compatible, starts in ~1 s against ~20 s, uses a fraction of the memory. Keep real Kafka only for the measurement runs |
| Testcontainers with Kafka makes the suite slow | Suite above 3 min | Container shared per package, with a new topic per test; or Redpanda |
| Phase D overruns | Session 16 does not close by week 11 | Cut `ReconciliationExhausted` with escalation and keep only the attempt limit |
| The elastic pool reorders or loses a message when resizing | Property test fails: same aggregate out of order, or an offset committed ahead of an in-flight message | Pin the pool at a constant size, prove ordering and watermark, and only then turn elasticity on. If it does not close, experiment 7.4 compares only fixed pools and the limit is declared |
| Partition `pause`/`resume` gets tangled | The consumer stops and does not come back, or duplicates | Start by limiting with a token bucket before the send and only then pause the partition. The limiter alone already avoids overrunning the participant; the pause is what avoids piling up in memory |
| Metrics (17b) grow into a project of their own | The dashboard or the recorders take more than one session | Keep only the table of metrics in the 17b plan; drop the dashboard rows that no session after it reads. A metric that no decision uses does not get in |
| Noisy measurement | Variation above 15% between repetitions | Change the environment before adding repetitions. The wallet-go lesson: a generator competing with the services produces a non-monotonic curve |
| A hiring process opens before phase F | — | Stop at session 17, declare the state in the README, present it that way. The thesis is already proven |
| Scope inflating | You are writing a saga, or a second aggregate | Reread the yardstick: does this only exist because the boundary is asynchronous? A saga does not get in |

---

## 5. The eleven defense questions

One per week, answered out loud, timed at two minutes, with reference to your code. It is
the activity that turns the project into an offer.

1. The send timed out. What does the system do, and why can't it simply resend?
2. The same event reaches the projector twice. What happens?
3. Two events of the same aggregate arrive out of order. Now what?
4. Two concurrent commands on the same aggregate. Who wins, and how do you know?
5. Why can't the send to the participant live in the event consumer?
6. The client creates an instruction and reads it right after. Do they see it?
7. You deleted the projection by mistake. What is the procedure, and how long does it take?
8. Why partition by `aggregate_id` instead of guaranteeing global ordering?
9. The participant returned `429` and, on another instruction, timed out. Why do you treat the two in opposite ways?
10. Input arrives at four times what the participant accepts. What does the system do, and what does it not do?

11. The worker pool grows from 2 to 20 goroutines in the middle of the load. How do you guarantee that two events of the same aggregate do not run together or out of order, and that no offset is committed before its time?

Question 1 is the one the project exists to answer. Question 7 is the one you will answer
with a measured number, which almost no candidate can do.

---

## 6. Final checklist

- [ ] `go test -race ./...` green in CI
- [ ] Both event store invariants tested automatically
- [ ] Optimistic concurrency test with no flake in 20 runs
- [ ] Redelivery and out-of-order event tests in the projector
- [ ] The cruel case covered: the participant processes, does not answer, reconciliation discovers it
- [ ] `make replay` rebuilds the projection without resending anything to the participant
- [ ] A single trace crossing the five processes
- [ ] `docker compose up` works on a clean machine
- [ ] Under input above the output limit, nothing is lost and the consumer recovers
- [ ] Measurement reports with methodology and threats to validity
- [ ] 10 ADRs written
- [ ] Resilience report with at least one wrong hypothesis preserved
- [ ] README with known limitations
- [ ] Elastic pool property test: per-aggregate order and watermark preserved under resizing, `-race`, 20 runs
- [ ] You answer the eleven questions out loud, without looking, in two minutes each

The last item is the only one with no code attached, and it is the one that decides the
interview.
