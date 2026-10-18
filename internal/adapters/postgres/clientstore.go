// clientstore.go
// Adapter Postgres da porta app.ClientStore, mais as operações de cadastro usadas pelo
// comando clientctl.
// Objetivo: guardar os clientes da API na tabela clients, só com o hash do segredo. Find
// serve ao endpoint de autenticação; Create e Deactivate servem à administração por linha de
// comando, já que não há endpoint de cadastro.

package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/VictorM0nteiro/payrouter/internal/app"
)

// uniqueViolation é o SQLSTATE de violação de UNIQUE/PRIMARY KEY.
const uniqueViolation = "23505"

// ClientStore implementa app.ClientStore sobre o Postgres.
type ClientStore struct {
	db dbtx
}

var _ app.ClientStore = (*ClientStore)(nil)

func NewClientStore(db dbtx) *ClientStore {
	return &ClientStore{db: db}
}

// Find devolve o cliente, ou app.ErrNotFound se não existir.
func (s *ClientStore) Find(ctx context.Context, id string) (app.Client, error) {
	c := app.Client{ID: id}
	err := s.db.QueryRow(ctx,
		`SELECT secret_hash, active FROM clients WHERE id = $1`, id,
	).Scan(&c.SecretHash, &c.Active)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Client{}, app.ErrNotFound
	}
	if err != nil {
		return app.Client{}, err
	}
	return c, nil
}

// Create cadastra um cliente ativo. Devolve app.ErrClientExists se o id já existir.
func (s *ClientStore) Create(ctx context.Context, id string, secretHash []byte) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO clients (id, secret_hash) VALUES ($1, $2)`, id, secretHash)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return app.ErrClientExists
	}
	return err
}

// Deactivate desativa o cliente. Devolve app.ErrNotFound se ele não existir. Tokens já
// emitidos continuam valendo até expirar.
func (s *ClientStore) Deactivate(ctx context.Context, id string) error {
	tag, err := s.db.Exec(ctx, `UPDATE clients SET active = false WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return app.ErrNotFound
	}
	return nil
}
