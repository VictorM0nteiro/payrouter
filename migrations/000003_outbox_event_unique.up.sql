-- 000003_outbox_event_unique.up.sql
-- Torna estrutural o "no máximo uma linha de outbox por evento" e preenche o outbox dos
-- eventos gravados antes de o Append passar a escrevê-lo (sessões 5 a 8).
-- Objetivo: a invariante 2 (todo evento tem exatamente uma linha de outbox) valer também em
-- bancos de desenvolvimento antigos. O "pelo menos uma" vem do Append (um único INSERT com
-- CTE); o "no máximo uma" vem deste UNIQUE.

-- Eventos antigos entram no outbox com o mesmo envelope e tópico do Append. O relay os
-- publicará depois; é at-least-once e o consumidor é idempotente pela versão.
INSERT INTO outbox (event_id, aggregate_id, topic, payload)
SELECT e.id, e.aggregate_id, 'payrouter.instructions.events',
       jsonb_build_object('aggregate_id', e.aggregate_id, 'version', e.version,
                          'event_type', e.event_type, 'occurred_at', e.occurred_at,
                          'data', e.payload)
FROM events e
WHERE NOT EXISTS (SELECT 1 FROM outbox o WHERE o.event_id = e.id)
ORDER BY e.id;

ALTER TABLE outbox ADD CONSTRAINT outbox_event_id_key UNIQUE (event_id);
