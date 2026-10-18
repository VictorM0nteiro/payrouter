-- 000002_clients.up.sql
-- Cria a tabela de clientes da API.
-- Objetivo: tirar a credencial de dentro do ambiente. Cada cliente tem um id e o hash do
-- segredo (nunca o segredo). O segredo é aleatório de 32 bytes, então SHA-256 basta, sem
-- necessidade de hash lento. active permite desativar um cliente sem apagar o histórico.

CREATE TABLE clients (
    id          text        PRIMARY KEY,
    secret_hash bytea       NOT NULL,
    active      boolean     NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now()
);
