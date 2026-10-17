// idempotency.go
// Adapter Postgres da porta app.IdempotencyStore sobre a tabela idempotency_keys.
// Objetivo: reivindicar e completar uma chave de idempotência DENTRO da transação do
// comando. Como a linha só vira visível quando a transação confirma, ela nunca aparece pela
// metade: ou existe junto com o evento e a resposta, ou não existe. Duas requisições
// simultâneas com a mesma chave se serializam no índice único da tabela.

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	"github.com/jackc/pgx/v5"
)

const stateCompleted = "completed"

// claimKey tenta inserir a chave. Se outra transação já inseriu a mesma e ainda não
// confirmou, o INSERT espera por ela: se ela confirmar, DO NOTHING devolve zero linhas
// (a chave já existe); se ela desfizer, o INSERT segue e esta transação fica com a chave.

const claimKey = `
INSERT INTO idempotency_keys (scope, endpoint, key, request_fingerprint, state)
VALUES ($1, $2, $3, $4, 'in_progress')
ON CONFLICT (scope, endpoint, key) DO NOTHING`

const selectKey = `
SELECT request_fingerprint, state, COALESCE(aggregate_id::text, ''), response_body
FROM idempotency_keys
WHERE scope = $1 AND endpoint = $2 AND key = $3`

const completeKey = `
UPDATE idempotency_keys
SET state = 'completed', response_body = $4::jsonb, aggregate_id = $5::uuid
WHERE scope = $1 AND endpoint = $2 AND key = $3`

// IdempotencyStore implementa app.IdempotencyStore sobre o Postgres.
type IdempotencyStore struct {
	db dbtx
}

var _ app.IdempotencyStore = (*IdempotencyStore)(nil)

func NewIdempotencyStore(db dbtx) *IdempotencyStore {
	return &IdempotencyStore{db: db}
}

// Claim registra a chave ou devolve o que já está gravado para ela.
func (s *IdempotencyStore) Claim(ctx context.Context, key app.IdempotencyKey, fingerprint string) (app.IdempotencyRecord, bool, error) {
	tag, err := s.db.Exec(ctx, claimKey, key.Scope, key.Endpoint, key.Key, fingerprint)
	if err != nil {
		return app.IdempotencyRecord{}, false, fmt.Errorf("reivindicando chave de idempotência: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return app.IdempotencyRecord{}, true, nil
	}

	var (
		record      app.IdempotencyRecord
		state       string
		aggregateID string
	)
	err = s.db.QueryRow(ctx, selectKey, key.Scope, key.Endpoint, key.Key).
		Scan(&record.Fingerprint, &state, &aggregateID, &record.ResponseBody)
	if errors.Is(err, pgx.ErrNoRows) {
		// A chave sumiu entre o INSERT e o SELECT: outra transação a desfez. Só acontece
		// numa corrida rara; o chamador pode simplesmente repetir a requisição.
		return app.IdempotencyRecord{}, false, fmt.Errorf("chave de idempotência desapareceu durante a leitura, repita a requisição")
	}
	if err != nil {
		return app.IdempotencyRecord{}, false, fmt.Errorf("lendo chave de idempotência: %w", err)
	}
	if state != stateCompleted {
		return app.IdempotencyRecord{}, false, fmt.Errorf("chave de idempotência em estado inesperado %q", state)
	}
	record.AggregateID = domain.InstructionID(aggregateID)
	return record, false, nil
}

// Complete guarda a resposta da chave reivindicada por esta transação.
func (s *IdempotencyStore) Complete(ctx context.Context, key app.IdempotencyKey, aggregateID domain.InstructionID, responseBody []byte) error {
	tag, err := s.db.Exec(ctx, completeKey, key.Scope, key.Endpoint, key.Key, responseBody, string(aggregateID))
	if err != nil {
		return fmt.Errorf("completando chave de idempotência: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("completando chave de idempotência: %d linhas afetadas, esperada 1", tag.RowsAffected())
	}
	return nil
}
