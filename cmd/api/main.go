// main.go
// Ponto de entrada do processo api: lê a configuração do ambiente (DATABASE_URL, JWT_SECRET,
// ADDR), liga as peças e sobe o servidor HTTP.
// Objetivo: ser a única "composition root" da API. É aqui, e só aqui, que o Postgres, os
// casos de uso e o adapter HTTP se encontram. Timeouts, graceful shutdown e logs
// estruturados entram nas sessões 7 e 8; esta versão sobe e atende.

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	httpapi "github.com/VictorM0nteiro/payrouter/internal/adapters/http"
	jwtauth "github.com/VictorM0nteiro/payrouter/internal/adapters/jwt"
	"github.com/VictorM0nteiro/payrouter/internal/adapters/postgres"
	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

// tokenTTL é a validade do token de acesso. Sem refresh: o cliente autentica de novo.
const tokenTTL = 15 * time.Minute

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

func run() error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL não definida")
	}
	// O segredo de assinatura é configuração de infraestrutura (não é credencial de cliente).
	// Os clientes ficam no banco e são cadastrados com clientctl.
	signer, err := jwtauth.New([]byte(os.Getenv("JWT_SECRET")), tokenTTL, nil)
	if err != nil {
		return fmt.Errorf("JWT_SECRET: %w", err)
	}
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("criando pool do Postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("conectando ao Postgres: %w", err)
	}

	accept := app.NewAcceptInstruction(postgres.NewUnitOfWork(pool), func() domain.InstructionID {
		return domain.InstructionID(uuid.NewString())
	})
	get := app.NewGetInstruction(postgres.NewEventStore(pool))
	authn := app.NewAuthenticateClient(postgres.NewClientStore(pool), signer)

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewHandler(accept, get, authn, signer),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("api ouvindo em %s", addr)
	return srv.ListenAndServe()
}
