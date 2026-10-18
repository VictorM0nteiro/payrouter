// main.go
// Comando clientctl: administra os clientes da API pela linha de comando.
// Objetivo: não há endpoint de cadastro, então é aqui que um cliente nasce ou é desativado.
// "create" gera um segredo aleatório de 32 bytes, grava só o hash e imprime o segredo UMA
// vez; depois disso ele não pode ser recuperado, só substituído por um cliente novo.
// Usa DATABASE_URL, como a API.
//
//	clientctl create <client_id>
//	clientctl deactivate <client_id>

package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/VictorM0nteiro/payrouter/internal/adapters/postgres"
	"github.com/VictorM0nteiro/payrouter/internal/app"
)

const usage = "uso: clientctl create <client_id> | clientctl deactivate <client_id>"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "clientctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 2 || args[1] == "" {
		return errors.New(usage)
	}
	command, id := args[0], args[1]
	if command != "create" && command != "deactivate" {
		return errors.New(usage)
	}

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL não definida")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("conectando ao Postgres: %w", err)
	}
	defer pool.Close()
	store := postgres.NewClientStore(pool)

	if command == "deactivate" {
		return deactivate(ctx, store, id)
	}
	return create(ctx, store, id)
}

func create(ctx context.Context, store *postgres.ClientStore, id string) error {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return fmt.Errorf("gerando segredo: %w", err)
	}
	secret := base64.RawURLEncoding.EncodeToString(raw)

	if err := store.Create(ctx, id, app.HashSecret(secret)); err != nil {
		if errors.Is(err, app.ErrClientExists) {
			return fmt.Errorf("o cliente %q já existe", id)
		}
		return err
	}
	fmt.Printf("client_id:     %s\nclient_secret: %s\n\nGuarde o segredo agora: ele não será mostrado de novo.\n", id, secret)
	return nil
}

func deactivate(ctx context.Context, store *postgres.ClientStore, id string) error {
	if err := store.Deactivate(ctx, id); err != nil {
		if errors.Is(err, app.ErrNotFound) {
			return fmt.Errorf("o cliente %q não existe", id)
		}
		return err
	}
	fmt.Printf("cliente %q desativado; tokens já emitidos valem até expirar (até 15 min)\n", id)
	return nil
}
