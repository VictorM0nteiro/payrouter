// outbox.go
// Adapter Postgres da porta app.OutboxStore: reivindica mensagens pendentes do outbox com
// FOR UPDATE SKIP LOCKED, entrega ao publicador e só então as marca como publicadas.
// Objetivo: permitir vários relays em paralelo sem publicar a mesma linha duas vezes ao
// mesmo tempo e SEM reordenar um agregado. Só a linha pendente mais antiga de cada agregado
// é elegível; se outro relay a segura, as seguintes do agregado esperam.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
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
// Drain: veja app.OutboxStore. A transação segura os locks durante o publish; se o processo
// morrer ali, o Postgres desfaz e as linhas voltam a ficar elegíveis (at-least-once).
func (s *OutboxStore) Drain(ctx context.Context, limit int, publish func(ctx context.Context, msgs []app.OutboxMessage) error) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("iniciando transação do outbox: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	rows, err := tx.Query(ctx, selectPending, limit)
	if err != nil {
		return 0, fmt.Errorf("consultando outbox: %w", err)
	}
	var msgs []app.OutboxMessage
	for rows.Next() {
		var m app.OutboxMessage
		var payload string
		if err := rows.Scan(&m.ID, &m.AggregateID, &m.Topic, &m.EventType, &m.Version, &payload); err != nil {
			rows.Close()
			return 0, fmt.Errorf("lendo linha do outbox: %w", err)
		}
		m.Payload = []byte(payload)
		msgs = append(msgs, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterando outbox: %w", err)
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	if err := publish(ctx, msgs); err != nil {
		return 0, fmt.Errorf("publicando lote do outbox: %w", err)
	}

	markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), markTimeout)
	defer cancel()
	ids := make([]int64, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	if _, err := tx.Exec(markCtx, markPublished, ids); err != nil {
		return 0, fmt.Errorf("marcando publicadas: %w", err)
	}
	if err := tx.Commit(markCtx); err != nil {
		return 0, fmt.Errorf("confirmando outbox: %w", err)
	}
	return len(msgs), nil
}
