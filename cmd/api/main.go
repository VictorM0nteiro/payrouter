// main.go
// Ponto de entrada do processo api: lê a configuração do ambiente (DATABASE_URL, JWT_SECRET,
// ADDR, LOG_FORMAT, REQUEST_TIMEOUT), liga as peças e sobe o servidor HTTP.
// Objetivo: ser a única "composition root" da API. É aqui, e só aqui, que o Postgres, os
// casos de uso e o adapter HTTP se encontram, e onde todos os prazos ficam explícitos:
// nenhum valor padrão do net/http ou do pgx decide por nós. Graceful shutdown entra na
// sessão 8; esta versão sobe e atende.

package main

import (
	"context"
	"fmt"
	"log/slog"
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
	"github.com/VictorM0nteiro/payrouter/internal/logging"
)

const (
	// tokenTTL é a validade do token de acesso. Sem refresh: o cliente autentica de novo.
	tokenTTL = 15 * time.Minute

	// defaultRequestTimeout é o prazo de uma requisição inteira (REQUEST_TIMEOUT altera). Ao
	// estourar, a resposta é 504: o resultado é desconhecido e o cliente repete com a mesma
	// Idempotency-Key.
	defaultRequestTimeout = 5 * time.Second

	// Prazos do servidor HTTP. WriteTimeout é maior que o prazo da requisição para que a
	// resposta de erro (504) ainda consiga ser escrita.
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	writeTimeout      = 15 * time.Second
	idleTimeout       = 60 * time.Second

	// Prazos do pool do Postgres.
	connectTimeout    = 5 * time.Second
	maxConnLifetime   = 30 * time.Minute
	healthCheckPeriod = 30 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

func run() error {
	slog.SetDefault(logging.New(os.Getenv("LOG_FORMAT"), os.Stdout))

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
	requestTimeout, err := durationFromEnv("REQUEST_TIMEOUT", defaultRequestTimeout)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("DATABASE_URL: %w", err)
	}
	poolCfg.ConnConfig.ConnectTimeout = connectTimeout
	poolCfg.MaxConnLifetime = maxConnLifetime
	poolCfg.HealthCheckPeriod = healthCheckPeriod
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
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
	api := httpapi.NewHandler(accept, get, authn, signer)

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewRoot(api, pool, requestTimeout),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}
	slog.Info("api ouvindo", "addr", addr, "request_timeout", requestTimeout.String())
	return srv.ListenAndServe()
}

// durationFromEnv lê uma duração (ex.: "5s") da variável, ou devolve def se ela estiver vazia.
// Valor inválido ou não positivo impede a partida, em vez de cair num padrão em silêncio.
func durationFromEnv(name string, def time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s: %q não é uma duração positiva (ex.: 5s)", name, raw)
	}
	return d, nil
}
