// reconciliation_claim_test.go
// Testes da reivindicação de tarefas de conciliação contra um Postgres real (testcontainers).
// Objetivo: provar que Claim devolve só as tarefas vencidas, as tira da fila por um lease, e
// que dois processors reivindicando ao mesmo tempo recebem conjuntos disjuntos (SKIP LOCKED),
// de modo que a mesma instrução nunca é consultada por dois ao mesmo tempo. Também cobre
// Reschedule (soma uma tentativa e marca a próxima) e Complete (retira da fila).

package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

// claimMine reivindica e devolve só os ids de interesse do teste (o banco é compartilhado).
func claimMine(t *testing.T, limit int, lease time.Duration, mine map[domain.InstructionID]bool) []app.ReconciliationTask {
	t.Helper()
	var out []app.ReconciliationTask
	err := NewUnitOfWork(testPool).Do(context.Background(), func(ctx context.Context, tx app.Tx) error {
		tasks, err := tx.Reconciliation().Claim(ctx, limit, lease)
		for _, task := range tasks {
			if mine[task.ID] {
				out = append(out, task)
			}
		}
		return err
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return out
}

func TestClaimDevolveSoAsVencidasELevaAoLease(t *testing.T) {
	if setupErr != nil {
		t.Fatalf("Postgres de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
	ctx := context.Background()
	store := NewReconciliationStore(testPool)
	due, future := newAggregateID(t), newAggregateID(t)
	mine := map[domain.InstructionID]bool{due: true, future: true}
	if err := store.Schedule(ctx, due, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.Schedule(ctx, future, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	got := claimMine(t, 100, time.Minute, mine)

	if len(got) != 1 || got[0].ID != due || got[0].Attempts != 0 {
		t.Fatalf("reivindicadas = %+v, esperado só a vencida, com 0 tentativas", got)
	}
	// O lease tirou a tarefa da fila: reivindicar de novo não a devolve.
	if again := claimMine(t, 100, time.Minute, mine); len(again) != 0 {
		t.Errorf("segunda reivindicação = %+v, esperado vazia (a tarefa está sob lease)", again)
	}
}

func TestClaimConcorrenteDaConjuntosDisjuntos(t *testing.T) {
	if setupErr != nil {
		t.Fatalf("Postgres de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
	ctx := context.Background()
	store := NewReconciliationStore(testPool)
	const n = 40
	mine := make(map[domain.InstructionID]bool)
	for range n {
		id := newAggregateID(t)
		mine[id] = true
		if err := store.Schedule(ctx, id, time.Now().Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		counts  = make(map[domain.InstructionID]int)
		start   = make(chan struct{})
		workers = 6
	)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for _, task := range claimMine(t, 7, time.Minute, mine) {
				mu.Lock()
				counts[task.ID]++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	for id, c := range counts {
		if c != 1 {
			t.Errorf("tarefa %s reivindicada %d vezes, esperado 1", id, c)
		}
	}
	// Com 6 reivindicadores de até 7 tarefas cada, todas as 40 podem não sair numa rodada, mas
	// as que saíram são distintas; uma rodada final recolhe o resto sem repetir nenhuma.
	for _, task := range claimMine(t, 100, time.Minute, mine) {
		counts[task.ID]++
	}
	for id := range mine {
		if counts[id] != 1 {
			t.Errorf("tarefa %s no total reivindicada %d vezes, esperado exatamente 1", id, counts[id])
		}
	}
}

func TestRescheduleSomaTentativaECompleteRetira(t *testing.T) {
	if setupErr != nil {
		t.Fatalf("Postgres de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
	ctx := context.Background()
	store := NewReconciliationStore(testPool)
	id := newAggregateID(t)
	if err := store.Schedule(ctx, id, time.Now()); err != nil {
		t.Fatal(err)
	}
	next := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)

	if err := store.Reschedule(ctx, id, next); err != nil {
		t.Fatal(err)
	}
	if err := store.Reschedule(ctx, id, next); err != nil {
		t.Fatal(err)
	}

	var attempts int
	var got time.Time
	if err := testPool.QueryRow(ctx, `SELECT attempts, next_attempt_at FROM reconciliation_tasks WHERE aggregate_id = $1::uuid`, string(id)).Scan(&attempts, &got); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || !got.Equal(next) {
		t.Errorf("tentativas=%d próxima=%v; esperado 2 e %v", attempts, got, next)
	}

	if err := store.Complete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if n, _ := taskCount(t, id); n != 0 {
		t.Errorf("tarefas depois do Complete = %d, esperado 0", n)
	}
}
