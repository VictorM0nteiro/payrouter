// unitofwork.go
// Adapter Postgres da porta app.UnitOfWork: abre uma transação, entrega ao caso de uso os
// stores ligados a ela e confirma ou desfaz tudo junto.
// Objetivo: garantir que evento e chave de idempotência (e, na sessão 9, o outbox) sejam
// atômicos entre si. É a ferramenta contra o dual write: nunca existe um sem o outro.

package postgres

import (
	"context"
	"fmt"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/jackc/pgx/v5/pgxpool"
)

// UnitOfWork implementa app.UnitOfWork sobre um pool do Postgres.
type UnitOfWork struct {
	pool *pgxpool.Pool
}

var _ app.UnitOfWork = (*UnitOfWork)(nil)

func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork {
	return &UnitOfWork{pool: pool}
}

// txScope são os stores de uma transação aberta.
type txScope struct {
	events      *EventStore
	idempotency *IdempotencyStore
}

func (t *txScope) Events() app.EventStore            { return t.events }
func (t *txScope) Idempotency() app.IdempotencyStore { return t.idempotency }

// Do roda fn numa transação. Erro de fn (ou pânico) desfaz tudo.
func (u *UnitOfWork) Do(ctx context.Context, fn func(ctx context.Context, tx app.Tx) error) error {
	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("iniciando transação: %w", err)
	}
	// Após o Commit, o Rollback é no-op; em erro ou pânico, é ele que desfaz.
	defer func() { _ = tx.Rollback(ctx) }()

	scope := &txScope{events: NewEventStore(tx), idempotency: NewIdempotencyStore(tx)}
	if err := fn(ctx, scope); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("confirmando transação: %w", err)
	}
	return nil
}
