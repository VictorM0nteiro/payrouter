// reconciliation.go
// Adapter Postgres da porta app.ReconciliationStore sobre a tabela reconciliation_tasks.
// Objetivo: criar a tarefa de conciliação DENTRO da transação que grava RoutingTimedOut. A
// linha só fica visível quando a transação confirma, então "instrução em unknown" e "tarefa
// existe" são a mesma coisa: nunca uma sem a outra. Também reivindica as tarefas vencidas
// com um lease (FOR UPDATE SKIP LOCKED), para vários processors não consultarem a mesma.

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

// claimTasks empurra o vencimento das tarefas vencidas em um lease e as devolve, num só
// comando. SKIP LOCKED faz quem chega depois pular as linhas que outro já está reivindicando.
const claimTasks = `
UPDATE reconciliation_tasks
SET next_attempt_at = now() + make_interval(secs => $2::float8)
WHERE aggregate_id IN (
    SELECT aggregate_id FROM reconciliation_tasks
    WHERE next_attempt_at <= now()
    ORDER BY next_attempt_at
    LIMIT $1
    FOR UPDATE SKIP LOCKED)
RETURNING aggregate_id::text, attempts`

const rescheduleTask = `
UPDATE reconciliation_tasks SET attempts = attempts + 1, next_attempt_at = $2
WHERE aggregate_id = $1::uuid`

const completeTask = `DELETE FROM reconciliation_tasks WHERE aggregate_id = $1::uuid`

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

// Claim reivindica até limit tarefas vencidas, deixando-as fora da fila por lease.
func (s *ReconciliationStore) Claim(ctx context.Context, limit int, lease time.Duration) ([]app.ReconciliationTask, error) {
	rows, err := s.db.Query(ctx, claimTasks, limit, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("reivindicando tarefas de conciliação: %w", err)
	}
	defer rows.Close()

	var tasks []app.ReconciliationTask
	for rows.Next() {
		var id string
		var attempts int
		if err := rows.Scan(&id, &attempts); err != nil {
			return nil, fmt.Errorf("lendo tarefa de conciliação: %w", err)
		}
		tasks = append(tasks, app.ReconciliationTask{ID: domain.InstructionID(id), Attempts: attempts})
	}
	return tasks, rows.Err()
}

// Reschedule soma uma tentativa e marca a próxima consulta para at.
func (s *ReconciliationStore) Reschedule(ctx context.Context, id domain.InstructionID, at time.Time) error {
	if _, err := s.db.Exec(ctx, rescheduleTask, string(id), at); err != nil {
		return fmt.Errorf("reagendando conciliação de %s: %w", id, err)
	}
	return nil
}

// Complete retira a tarefa da fila.
func (s *ReconciliationStore) Complete(ctx context.Context, id domain.InstructionID) error {
	if _, err := s.db.Exec(ctx, completeTask, string(id)); err != nil {
		return fmt.Errorf("encerrando conciliação de %s: %w", id, err)
	}
	return nil
}
