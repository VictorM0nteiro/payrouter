-- 000001_init.up.sql
-- Cria o esquema inicial do payrouter: event store, outbox transacional, fila de
-- conciliação e chaves de idempotência.
-- Objetivo: ter no Postgres a única fonte da verdade (events) e as tabelas de apoio que
-- tornam a fronteira assíncrona segura (outbox, reconciliation_tasks, idempotency_keys).

-- O event store. Fonte única da verdade. Eventos são imutáveis: só INSERT.
CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_id    UUID    NOT NULL,
    aggregate_type  TEXT    NOT NULL,
    version         INT     NOT NULL CHECK (version > 0),
    event_type      TEXT    NOT NULL,
    payload         JSONB   NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Concorrência otimista: duas gravações da mesma versão colidem aqui.
    UNIQUE (aggregate_id, version)
);

CREATE INDEX events_aggregate_idx ON events (aggregate_id, version);

-- Outbox transacional: gravado na MESMA transação do evento (evita dual write).
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    event_id      BIGINT NOT NULL REFERENCES events(id),
    aggregate_id  UUID   NOT NULL,
    topic         TEXT   NOT NULL,
    payload       JSONB  NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

-- Índice parcial: o relay só varre o que ainda não foi publicado.
CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;

-- Fila de trabalho do process manager (conciliação de instruções em unknown).
CREATE TABLE reconciliation_tasks (
    aggregate_id     UUID PRIMARY KEY,
    attempts         INT  NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX reconciliation_due_idx ON reconciliation_tasks (next_attempt_at);

-- Idempotência do lado de comando, escopada por (scope, endpoint, key).
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
