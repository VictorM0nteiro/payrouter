// reconciliation_test.go
// Testes do ReconciliationStore contra um Postgres real (testcontainers).
// Objetivo: provar que agendar a tarefa é idempotente (repetir não duplica nem adianta a
// primeira consulta) e que ela anda junto com a transação: se a transação desfaz, a tarefa
// some com o evento, e se confirma, os dois existem.

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

func taskCount(t *testing.T, id domain.InstructionID) (n int, next time.Time) {
	t.Helper()
	err := testPool.QueryRow(context.Background(),
		`SELECT count(*), COALESCE(max(next_attempt_at), now()) FROM reconciliation_tasks WHERE aggregate_id = $1::uuid`,
		string(id)).Scan(&n, &next)
	if err != nil {
		t.Fatal(err)
	}
	return n, next
}

func newAggregateID(t *testing.T) domain.InstructionID {
	t.Helper()
	var id string
	if err := testPool.QueryRow(context.Background(), `SELECT gen_random_uuid()::text`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return domain.InstructionID(id)
}

func TestScheduleEIdempotenteEMantemAPrimeiraConsulta(t *testing.T) {
	if setupErr != nil {
		t.Fatalf("Postgres de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
	ctx := context.Background()
	id := newAggregateID(t)
	first := time.Now().Add(time.Minute).UTC().Truncate(time.Microsecond)
	store := NewReconciliationStore(testPool)

	if err := store.Schedule(ctx, id, first); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := store.Schedule(ctx, id, first.Add(time.Hour)); err != nil {
		t.Fatalf("Schedule repetido: %v", err)
	}

	n, next := taskCount(t, id)
	if n != 1 || !next.Equal(first) {
		t.Errorf("tarefas=%d próxima=%v; esperado 1 e %v (a segunda chamada não muda nada)", n, next, first)
	}
}

func TestTarefaAcompanhaATransacao(t *testing.T) {
	if setupErr != nil {
		t.Fatalf("Postgres de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
	ctx := context.Background()
	uow := NewUnitOfWork(testPool)
	errBoom := errors.New("falha depois de agendar")

	rolledBack := newAggregateID(t)
	err := uow.Do(ctx, func(ctx context.Context, tx app.Tx) error {
		if err := tx.Reconciliation().Schedule(ctx, rolledBack, time.Now()); err != nil {
			return err
		}
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("erro = %v, esperado %v", err, errBoom)
	}
	if n, _ := taskCount(t, rolledBack); n != 0 {
		t.Errorf("tarefas depois do rollback = %d, esperado 0", n)
	}

	committed := newAggregateID(t)
	err = uow.Do(ctx, func(ctx context.Context, tx app.Tx) error {
		return tx.Reconciliation().Schedule(ctx, committed, time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := taskCount(t, committed); n != 1 {
		t.Errorf("tarefas depois do commit = %d, esperado 1", n)
	}
}
