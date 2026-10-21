-- 000003_outbox_event_unique.down.sql
-- Desfaz a migration 000003: remove o UNIQUE de outbox.event_id.
-- Objetivo: permitir migrate down limpo. As linhas de outbox preenchidas ficam: apagá-las
-- perderia mensagens que o relay ainda pode precisar publicar.

ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_event_id_key;
