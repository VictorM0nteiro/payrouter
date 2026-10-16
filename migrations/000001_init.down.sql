-- 000001_init.down.sql
-- Desfaz a migration 000001: remove as quatro tabelas do payrouter.
-- Objetivo: permitir migrate down limpo em desenvolvimento e nos testes.
-- A ordem importa: outbox referencia events, então cai primeiro.

DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS reconciliation_tasks;
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS events;
