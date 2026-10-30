// main.go
// Ponto de entrada do processo processor: lê a configuração do ambiente (DATABASE_URL,
// KAFKA_BROKERS, PARTICIPANT_URL, PARTICIPANT_TIMEOUT, RECONCILE_FIRST_DELAY, RECONCILE_BATCH, RECONCILE_POLL, RECONCILE_LEASE,
// RECONCILE_BACKOFF_BASE, RECONCILE_BACKOFF_MAX,  RECONCILE_MAX_ATTEMPTS, OUTPUT_TPS, OUTPUT_BURST, KAFKA_GROUP, METRICS_ADDR, LOG_FORMAT), liga o
// consumidor Kafka ao caso de uso e roda até receber SIGINT/SIGTERM.
// Objetivo: ser a composition root do processor, o lado do comando que produz o efeito
// colateral (enviar ao participante). No desligamento termina a mensagem em andamento antes
// de fechar o consumidor e o pool, para não deixar um envio feito sem a resposta gravada.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
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
	"github.com/VictorM0nteiro/payrouter/internal/metrics"
	"github.com/VictorM0nteiro/payrouter/internal/ratelimit"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	instructionsTopic  = "payrouter.instructions.events"
	defaultGroup       = "payrouter-processor"
	defaultMetricsAddr = ":9092"
	defaultPayTimeout  = 3 * time.Second
	defaultFirstDelay  = 5 * time.Second
	defaultBatch       = 50
	defaultPoll        = time.Second
	defaultLease       = 30 * time.Second
	defaultBackoffBase = 5 * time.Second
	defaultBackoffMax  = time.Minute
	defaultMaxAttempts = 10
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
	outputTPS, err := floatFromEnv("OUTPUT_TPS", 0)
	if err != nil {
		return err
	}
	outputBurst, err := intFromEnv("OUTPUT_BURST", 0)
	if err != nil {
		return err
	}
	group := os.Getenv("KAFKA_GROUP")
	if group == "" {
		group = defaultGroup
	}
	// METRICS_ADDR definida e vazia desliga as métricas; ausente usa o padrão.
	metricsAddr, ok := os.LookupEnv("METRICS_ADDR")
	if !ok {
		metricsAddr = defaultMetricsAddr
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
	if outputTPS > 0 {
		// Sem OUTPUT_BURST, a rajada é de uma ficha por TPS (no mínimo 1).
		if outputBurst == 0 {
			outputBurst = max(1, int(math.Ceil(outputTPS)))
		}
		useCase.WithLimiter(ratelimit.New(outputTPS, outputBurst))
	}
	reconciler := app.NewReconcile(uow, client, reconcile)

	reg := metrics.NewRegistry()
	pm := metrics.NewProcessor(reg)
	metrics.RegisterBacklog(reg, postgres.NewBacklogReader(pool).Read)
	useCase.WithMetrics(pm)
	reconciler.WithMetrics(pm)
	consumer.WithMetrics(pm)

	slog.Info("processor iniciado", "brokers", brokers, "group", group, "participant", participantURL,
		"participant_timeout", payTimeout.String(), "reconcile_first_delay", firstDelay.String(),
		"reconcile_poll", reconcile.Poll.String(), "reconcile_backoff_base", reconcile.BackoffBase.String(),
		"reconcile_backoff_max", reconcile.BackoffMax.String(), "reconcile_max_attempts", reconcile.MaxAttempts,
		"output_tps", outputTPS, "output_burst", outputBurst, "metrics_addr", metricsAddr)

	// A conciliação roda ao lado do consumidor; os dois param com o mesmo sinal.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = reconciler.Run(sigCtx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		// As métricas são opcionais: se a porta não abrir, o processor segue sem elas.
		if err := metrics.Serve(sigCtx, metricsAddr, reg); err != nil {
			slog.Error("processor: servidor de métricas falhou", "addr", metricsAddr, "error", err)
		}
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
	if cfg.MaxAttempts, err = intFromEnv("RECONCILE_MAX_ATTEMPTS", defaultMaxAttempts); err != nil {
		return cfg, err
	}
	if cfg.BackoffMax < cfg.BackoffBase {
		return cfg, fmt.Errorf("RECONCILE_BACKOFF_MAX (%s) menor que RECONCILE_BACKOFF_BASE (%s)", cfg.BackoffMax, cfg.BackoffBase)
	}
	return cfg, nil
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
