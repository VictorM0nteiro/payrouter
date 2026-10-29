// main.go
// Ponto de entrada do processo processor: lê a configuração do ambiente (DATABASE_URL,
// KAFKA_BROKERS, PARTICIPANT_URL, PARTICIPANT_TIMEOUT, RECONCILE_FIRST_DELAY, RECONCILE_BATCH, RECONCILE_POLL, RECONCILE_LEASE,
// RECONCILE_BACKOFF_BASE, RECONCILE_BACKOFF_MAX, KAFKA_GROUP, LOG_FORMAT), liga o
// consumidor Kafka ao caso de uso e roda até receber SIGINT/SIGTERM.
// Objetivo: ser a composition root do processor, o lado do comando que produz o efeito
// colateral (enviar ao participante). No desligamento termina a mensagem em andamento antes
// de fechar o consumidor e o pool, para não deixar um envio feito sem a resposta gravada.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/adapters/kafka"
	"github.com/VictorM0nteiro/payrouter/internal/adapters/participant"
	"github.com/VictorM0nteiro/payrouter/internal/adapters/postgres"
	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/logging"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	instructionsTopic  = "payrouter.instructions.events"
	defaultGroup       = "payrouter-processor"
	defaultPayTimeout  = 3 * time.Second
	defaultFirstDelay  = 5 * time.Second
	defaultBatch       = 50
	defaultPoll        = time.Second
	defaultLease       = 30 * time.Second
	defaultBackoffBase = 5 * time.Second
	defaultBackoffMax  = time.Minute
	connectTimeout     = 5 * time.Second
	maxConnLifetime    = 30 * time.Minute
	healthCheckPeriod  = 30 * time.Second
	startupPingTimeout = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "processor:", err)
		os.Exit(1)
	}
}

func run() error {
	slog.SetDefault(logging.New(os.Getenv("LOG_FORMAT"), os.Stdout))

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL não definida")
	}
	brokers := splitList(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS não definida (ex.: redpanda:9092)")
	}
	participantURL := os.Getenv("PARTICIPANT_URL")
	if participantURL == "" {
		return fmt.Errorf("PARTICIPANT_URL não definida (ex.: http://participant:9090)")
	}
	payTimeout, err := durationFromEnv("PARTICIPANT_TIMEOUT", defaultPayTimeout)
	if err != nil {
		return err
	}
	firstDelay, err := durationFromEnv("RECONCILE_FIRST_DELAY", defaultFirstDelay)
	if err != nil {
		return err
	}
	reconcile, err := reconcileConfig()
	if err != nil {
		return err
	}
	group := os.Getenv("KAFKA_GROUP")
	if group == "" {
		group = defaultGroup
	}

	startCtx, cancel := context.WithTimeout(context.Background(), startupPingTimeout)
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

	consumer, err := kafka.NewConsumer(brokers, group, instructionsTopic)
	if err != nil {
		return err
	}
	defer consumer.Close()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	uow := postgres.NewUnitOfWork(pool)
	client := participant.NewClient(participantURL, payTimeout)
	useCase := app.NewProcessInstruction(uow, client, firstDelay)
	reconciler := app.NewReconcile(uow, client, reconcile)
	slog.Info("processor iniciado", "brokers", brokers, "group", group, "participant", participantURL,
		"participant_timeout", payTimeout.String(), "reconcile_first_delay", firstDelay.String(),
		"reconcile_poll", reconcile.Poll.String(), "reconcile_backoff_base", reconcile.BackoffBase.String(),
		"reconcile_backoff_max", reconcile.BackoffMax.String())

	// A conciliação roda ao lado do consumidor; os dois param com o mesmo sinal.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = reconciler.Run(sigCtx)
	}()
	err = consumer.Run(sigCtx, useCase.Handle)
	stop()
	wg.Wait()
	slog.Info("processor encerrado")
	return err
}

// reconcileConfig lê os parâmetros do laço de conciliação; valor inválido impede a partida.
func reconcileConfig() (app.ReconcileConfig, error) {
	var cfg app.ReconcileConfig
	var err error
	if cfg.Batch, err = intFromEnv("RECONCILE_BATCH", defaultBatch); err != nil {
		return cfg, err
	}
	if cfg.Poll, err = durationFromEnv("RECONCILE_POLL", defaultPoll); err != nil {
		return cfg, err
	}
	if cfg.Lease, err = durationFromEnv("RECONCILE_LEASE", defaultLease); err != nil {
		return cfg, err
	}
	if cfg.BackoffBase, err = durationFromEnv("RECONCILE_BACKOFF_BASE", defaultBackoffBase); err != nil {
		return cfg, err
	}
	if cfg.BackoffMax, err = durationFromEnv("RECONCILE_BACKOFF_MAX", defaultBackoffMax); err != nil {
		return cfg, err
	}
	if cfg.BackoffMax < cfg.BackoffBase {
		return cfg, fmt.Errorf("RECONCILE_BACKOFF_MAX (%s) menor que RECONCILE_BACKOFF_BASE (%s)", cfg.BackoffMax, cfg.BackoffBase)
	}
	return cfg, nil
}

// intFromEnv lê um inteiro positivo da variável, ou devolve def se ela estiver vazia.
func intFromEnv(name string, def int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s: %q não é um inteiro positivo", name, raw)
	}
	return n, nil
}

// splitList separa uma lista por vírgula, ignorando espaços e itens vazios.
func splitList(raw string) []string {
	var out []string
	for _, s := range strings.Split(raw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// durationFromEnv lê uma duração (ex.: "3s") da variável, ou devolve def se ela estiver vazia.
// Valor inválido ou não positivo impede a partida, em vez de cair num padrão em silêncio.
func durationFromEnv(name string, def time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s: %q não é uma duração positiva (ex.: 3s)", name, raw)
	}
	return d, nil
}
