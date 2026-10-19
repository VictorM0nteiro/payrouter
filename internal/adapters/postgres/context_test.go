// context_test.go
// Testes de que o prazo do contexto chega ao Postgres.
// Objetivo: provar que um contexto já expirado faz o store e a unidade de trabalho falharem
// com context.DeadlineExceeded, sem executar nada e sem travar. É o que permite à API
// responder 504 quando o prazo da requisição estoura: o ctx da requisição atravessa o caso
// de uso até o pgx.

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
)

func expiredContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)
	return ctx
}

func TestLoadComContextoExpiradoDevolveDeadlineExceeded(t *testing.T) {
	store := newStore(t)

	_, err := store.Load(expiredContext(t), newID(t))

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("erro = %v, esperado DeadlineExceeded", err)
	}
}

func TestUnitOfWorkComContextoExpiradoNaoExecutaNada(t *testing.T) {
	newStore(t) // falha com mensagem clara se o Docker não subiu
	called := false

	err := NewUnitOfWork(testPool).Do(expiredContext(t), func(context.Context, app.Tx) error {
		called = true
		return nil
	})

	if !errors.Is(err, context.DeadlineExceeded) || called {
		t.Errorf("erro = %v, fn chamada = %v", err, called)
	}
}

func TestContextoCanceladoNoMeioDaTransacaoDesfazTudo(t *testing.T) {
	newStore(t)
	id := newID(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := NewUnitOfWork(testPool).Do(ctx, func(ctx context.Context, tx app.Tx) error {
		if err := tx.Events().Append(ctx, id, 0, accepted(id)); err != nil {
			return err
		}
		cancel() // o prazo estoura depois do append e antes do commit
		return nil
	})

	if err == nil {
		t.Fatal("o commit com contexto cancelado deveria falhar")
	}
	if _, err := NewEventStore(testPool).Load(context.Background(), id); !errors.Is(err, app.ErrNotFound) {
		t.Errorf("nada deveria ter sido gravado, Load = %v", err)
	}
}
