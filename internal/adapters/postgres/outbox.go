// outbox.go
// Adapter Postgres da porta app.OutboxStore: reivindica mensagens pendentes do outbox com
// FOR UPDATE SKIP LOCKED, entrega ao publicador e só então as marca como publicadas.
// Objetivo: permitir vários relays em paralelo sem publicar a mesma linha duas vezes ao
// mesmo tempo e SEM reordenar um agregado. Só a linha pendente mais antiga de cada agregado
// é elegível; se outro relay a segura, as seguintes do agregado esperam.
package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// marktimeout é o prazo para marcar e confirmar depois de publicar. Roda sem herdar o cancelamento
// se o desligamento chegar entre o publish e o commit ainda vale marcar em vez de republicar a toa
const markTimeout = 5 * time.Second

// selectPending devolve a cabeça pendente de cada agregado. O NOT EXISTS enxerga a linha
// anterior ainda pendente (inclusive a travada por outro relay) e exclui as seguintes.
const selectPending = `
SELECT o.id, o.aggregate_id::text, o.topic,
       o.payload->>'event_type', (o.payload->>'version')::int, o.payload::text
FROM outbox o
WHERE o.published_at IS NULL
  AND NOT EXISTS (SELECT 1 FROM outbox p
                  WHERE p.aggregate_id = o.aggregate_id AND p.id < o.id AND p.published_at IS NULL)
ORDER BY o.id
LIMIT $1
FOR UPDATE OF o SKIP LOCKED`

const markPublished = `UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])`

// OutboxStore implementa app.OutboxStore sobre o Postgres.
// OutboxStore implementa app.OutboxStore sobre o Postgres.
type OutboxStore struct {
	pool *pgxpool.Pool
}

var _ app.OutboxStore = (*OutboxStore)(nil)

func NewOutboxStore(pool *pgxpool.Pool) *OutboxStore {
	return &OutboxStore{pool: pool}
}

// Drain: veja app.OutboxStore. A transação segura os locks durante o publish; se o processo
// morrer ali, o Postgres desfaz e as linhas voltam a ficar elegíveis (at-least-once).
func (s *OutboxStore) Drain(ctx context.Context, limit int, publish func(ctx context.Context, msgs []app.OutboxMessage) error) (int, error) {
	
}