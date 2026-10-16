-- invariants.sql
-- As duas invariantes do event store, como consultas que devem retornar ZERO linhas.
-- Objetivo: serem executadas pelos testes de integração (a partir da sessão 3, com
-- testcontainers) e no CI, provando que o event store nunca fica inconsistente.

-- Invariante 1: versões de um agregado são contíguas a partir de 1, sem buraco.
SELECT aggregate_id
FROM events
GROUP BY aggregate_id
HAVING COUNT(*) <> MAX(version) OR MIN(version) <> 1;

-- Invariante 2: todo evento tem exatamente uma linha de outbox (nem zero, nem duas).
SELECT e.id
FROM events e
LEFT JOIN outbox o ON o.event_id = e.id
GROUP BY e.id
HAVING COUNT(o.id) <> 1;
