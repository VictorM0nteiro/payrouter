// main.go
// Ponto de entrada do processo api: lê a configuração do ambiente (DATABASE_URL, JWT_SECRET,
// ADDR, LOG_FORMAT, REQUEST_TIMEOUT, API_RATE_RPS, API_RATE_BURST, METRICS_ADDR), liga as peças e sobe o servidor HTTP.
// Objetivo: ser a única "composition root" da API. É aqui, e só aqui, que o Postgres, os
// casos de uso e o adapter HTTP se encontram, e onde todos os prazos ficam explícitos:
// nenhum valor padrão do net/http ou do pgx decide por nós. Também desliga com elegância:
// ao receber SIGINT/SIGTERM, para de aceitar conexões, deixa as requisições em andamento
// terminarem e só então fecha o pool do Postgres.

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	httpapi "github.com/VictorM0nteiro/payrouter/internal/adapters/http"
	jwtauth "github.com/VictorM0nteiro/payrouter/internal/adapters/jwt"
	"github.com/VictorM0nteiro/payrouter/internal/adapters/postgres"
	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	"github.com/VictorM0nteiro/payrouter/internal/logging"
	"github.com/VictorM0nteiro/payrouter/internal/metrics"
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

	defaultMetricsAddr = ":9093"

	// shutdownTimeout é quanto o desligamento espera as requisições em andamento. Fica abaixo
	// do stop_grace_period do compose (15s), senão o SIGKILL chegaria antes.
	shutdownTimeout = 10 * time.Second

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
	rateRPS, err := floatFromEnv("API_RATE_RPS", 0)
	if err != nil {
		return err
	}
	rateBurst, err := intFromEnv("API_RATE_BURST", 0)
	if err != nil {
		return err
	}
	if rateBurst == 0 {
		// Sem API_RATE_BURST, a rajada é de uma requisição por RPS (no mínimo 1).
		rateBurst = max(1, int(math.Ceil(rateRPS)))
	}

	// METRICS_ADDR definida e vazia desliga as métricas; ausente usa o padrão.
	metricsAddr, ok := os.LookupEnv("METRICS_ADDR")
	if !ok {
		metricsAddr = defaultMetricsAddr
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("DATABASE_URL: %w", err)
	}
	poolCfg.ConnConfig.ConnectTimeout = connectTimeout
	poolCfg.MaxConnLifetime = maxConnLifetime
	poolCfg.HealthCheckPeriod = healthCheckPeriod
	pool, err := pgxpool.NewWithConfig(startCtx, poolCfg)
	if err != nil {
		return fmt.Errorf("criando pool do Postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(startCtx); err != nil {
		return fmt.Errorf("conectando ao Postgres: %w", err)
	}

	accept := app.NewAcceptInstruction(postgres.NewUnitOfWork(pool), func() domain.InstructionID {
		return domain.InstructionID(uuid.NewString())
	})
	get := app.NewGetInstruction(postgres.NewEventStore(pool))
	authn := app.NewAuthenticateClient(postgres.NewClientStore(pool), signer)
	api := httpapi.NewHandler(accept, get, authn, signer, httpapi.WithRateLimit(rateRPS, rateBurst))

	reg := metrics.NewRegistry()
	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewRoot(api, pool, requestTimeout, httpapi.WithMetrics(metrics.NewHTTP(reg))),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}
	// Abrir o listener aqui, e não dentro do servidor, faz um endereço ocupado falhar já na
	// partida, antes de qualquer outra coisa.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("escutando em %s: %w", addr, err)
	}

	// SIGINT (Ctrl-C) e SIGTERM (docker stop) iniciam o desligamento. Depois do primeiro, o
	// stop() devolve o tratamento padrão: um segundo sinal derruba o processo na hora.
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-sigCtx.Done(); stop() }()

	metricsDone := make(chan struct{})
	go func() {
		defer close(metricsDone)
		// As métricas são opcionais: se a porta não abrir, a API segue sem elas.
		if err := metrics.Serve(sigCtx, metricsAddr, reg); err != nil {
			slog.Error("api: servidor de métricas falhou", "addr", metricsAddr, "error", err)
		}
	}()
	// Se a API cair sozinha, sem sinal, o stop() também encerra o servidor de métricas.
	defer func() { stop(); <-metricsDone }()

	slog.Info("api ouvindo", "addr", addr, "request_timeout", requestTimeout.String(), "metrics_addr", metricsAddr)
	return serve(sigCtx, srv, ln, shutdownTimeout)
}

// serve atende em ln até o contexto terminar e então desliga com elegância: fecha o
// listener (nenhuma conexão nova), deixa as requisições em andamento terminarem por até
// shutdownTimeout e só depois devolve. O pool do Postgres é fechado por quem chamou, depois
// daqui, para nenhuma requisição ainda em curso perder o banco.
//
// Uma requisição que não termina no prazo é cortada. Para o cliente isso é um resultado
// desconhecido, o mesmo caso do 504: ele repete com a mesma Idempotency-Key.
func serve(ctx context.Context, srv *http.Server, ln net.Listener, shutdownTimeout time.Duration) error {
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()

	select {
	case err := <-errc:
		return err // o servidor caiu sozinho, antes de qualquer sinal
	case <-ctx.Done():
	}

	slog.Info("encerrando: aguardando requisições em andamento", "timeout", shutdownTimeout.String())
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		_ = srv.Close() // prazo estourado: corta o que sobrou
		return fmt.Errorf("desligando o servidor: %w", err)
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	slog.Info("api encerrada")
	return nil
}

// floatFromEnv lê um número não negativo da variável, ou devolve def se ela estiver vazia. Zero
// desliga o limite.
func floatFromEnv(name string, def float64) (float64, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil || f < 0 {
		return 0, fmt.Errorf("%s: %q não é um número não negativo", name, raw)
	}
	return f, nil
}

// intFromEnv lê um inteiro não negativo da variável, ou devolve def se ela estiver vazia.
func intFromEnv(name string, def int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s: %q não é um inteiro não negativo", name, raw)
	}
	return n, nil
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
