// eventstore.go
// Adapter Postgres da porta app.EventStore: append com versão esperada e load do histórico.
// Objetivo: implementar a concorrência otimista. Quem grava a versão N afirma ter lido a
// N-1; se outro comando gravou antes, o append falha com ErrConcurrencyConflict. Sem lock
// de linha e sem isolamento SERIALIZABLE: a garantia vem de duas coisas combinadas, o
// UNIQUE (aggregate_id, version) e a checagem de versão dentro do próprio INSERT.

package postgres

import (
        "context"
        "errors"
        "fmt"

        "github.com/jackc/pgx/v5/pgconn"
        "github.com/jackc/pgx/v5/pgxpool"

        "github.com/VictorM0nteiro/payrouter/internal/app"
        "github.com/VictorM0nteiro/payrouter/internal/domain"
)

const aggregateType = "PaymentInstruction"

// insertEvent só insere se a versão atual do agregado for exatamente $3 - 1.
//
// O UNIQUE sozinho não basta: com expectedVersion à frente da versão real, o INSERT
// passaria sem colidir e deixaria um buraco no histórico (versão 1, depois 8), quebrando
// a invariante de versões contíguas. A cláusula WHERE fecha esse caso. Já a corrida entre
// dois comandos que leem a mesma versão é decidida pelo UNIQUE: ambos passam no WHERE, e
// o segundo a gravar leva violação 23505.
const insertEvent = `
INSERT INTO events (aggregate_id, aggregate_type, version, event_type, payload)
SELECT $1::uuid, $2::text, $3::int, $4::text, $5::jsonb
WHERE COALESCE((SELECT MAX(version) FROM events WHERE aggregate_id = $1::uuid), 0) = $3::int - 1`

const selectEvents = `
SELECT event_type, payload
FROM events
WHERE aggregate_id = $1::uuid
ORDER BY version`

// EventStore implementa app.EventStore sobre o Postgres.
type EventStore struct {
        pool *pgxpool.Pool
}

var _ app.EventStore = (*EventStore)(nil)

func NewEventStore(pool *pgxpool.Pool) *EventStore {
        return &EventStore{pool: pool}
}

// Append grava os eventos numa transação. Qualquer falha desfaz todos.
func (s *EventStore) Append(ctx context.Context, id domain.InstructionID, expectedVersion int, events ...domain.Event) error {
        if expectedVersion < 0 {
                return fmt.Errorf("versão esperada inválida: %d", expectedVersion)
        }
        if len(events) == 0 {
                return nil
        }

        tx, err := s.pool.Begin(ctx)
        if err != nil {
                return fmt.Errorf("iniciando transação: %w", err)
        }
        // Após o Commit, o Rollback é no-op.
        defer func() { _ = tx.Rollback(ctx) }()

        for i, e := range events {
                payload, err := encode(e)
                if err != nil {
                        return err
                }
                version := expectedVersion + i + 1

                tag, err := tx.Exec(ctx, insertEvent, string(id), aggregateType, version, string(e.Type()), payload)
                if err != nil {
                        if isUniqueViolation(err) {
                                return app.ErrConcurrencyConflict
                        }
                        return fmt.Errorf("gravando evento %s v%d: %w", e.Type(), version, err)
                }
                if tag.RowsAffected() == 0 {
                        return app.ErrConcurrencyConflict
                }
        }

        if err := tx.Commit(ctx); err != nil {
                return fmt.Errorf("confirmando transação: %w", err)
        }
        return nil
}

// Load devolve o histórico do agregado em ordem de versão.
func (s *EventStore) Load(ctx context.Context, id domain.InstructionID) ([]domain.Event, error) {
        rows, err := s.pool.Query(ctx, selectEvents, string(id))
        if err != nil {
                return nil, fmt.Errorf("consultando eventos: %w", err)
        }
        defer rows.Close()

        var events []domain.Event
        for rows.Next() {
                var eventType string
                var payload []byte
                if err := rows.Scan(&eventType, &payload); err != nil {
                        return nil, fmt.Errorf("lendo linha de evento: %w", err)
                }
                e, err := decode(eventType, payload)
                if err != nil {
                        return nil, err
                }
                events = append(events, e)
        }
        if err := rows.Err(); err != nil {
                return nil, fmt.Errorf("iterando eventos: %w", err)
        }
        if len(events) == 0 {
                return nil, app.ErrNotFound
        }
        return events, nil
}

// isUniqueViolation reconhece a violação do UNIQUE (aggregate_id, version).
func isUniqueViolation(err error) bool {
        var pgErr *pgconn.PgError
        return errors.As(err, &pgErr) && pgErr.Code == "23505"
}