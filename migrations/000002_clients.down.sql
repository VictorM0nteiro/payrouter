-- 000002_clients.down.sql
-- Desfaz a migration 000002: remove a tabela de clientes.
-- Objetivo: permitir migrate down limpo em desenvolvimento e nos testes.

DROP TABLE IF EXISTS clients;
