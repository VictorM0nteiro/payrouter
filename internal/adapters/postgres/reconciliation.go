// reconciliation.go
// Adapter Postgres da porta app.ReconciliationStore sobre a tabela reconciliation_tasks.
// Objetivo: criar a tarefa de conciliação DENTRO da transação que grava RoutingTimedOut. A
// linha só fica visível quando a transação confirma, então "instrução em unknown" e "tarefa
// existe" são a mesma coisa: nunca uma sem a outra.

package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

// scheduleTask é idempotente: a chave primária é o aggregate_id, então repetir não duplica
// nem adianta a primeira consulta.
const scheduleTask = `
INSERT INTO reconciliation_tasks (aggregate_id, next_attempt_at)
VALUES ($1::uuid, $2)
ON CONFLICT (aggregate_id) DO NOTHING`

// ReconciliationStore implementa app.ReconciliationStore sobre o Postgres.
type ReconciliationStore struct {
	db dbtx
}

var _ app.ReconciliationStore = (*ReconciliationStore)(nil)

func NewReconciliationStore(db dbtx) *ReconciliationStore {
	return &ReconciliationStore{db: db}
}

// Schedule cria a tarefa da instrução, com a primeira consulta em at.
func (s *ReconciliationStore) Schedule(ctx context.Context, id domain.InstructionID, at time.Time) error {
	if _, err := s.db.Exec(ctx, scheduleTask, string(id), at); err != nil {
		return fmt.Errorf("agendando conciliação de %s: %w", id, err)
	}
	return nil
}
