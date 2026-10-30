// main.go
// Ponto de entrada do processo relay: lê a configuração do ambiente (DATABASE_URL,
// KAFKA_BROKERS, RELAY_BATCH, RELAY_POLL, METRICS_ADDR, LOG_FORMAT), liga o outbox do Postgres ao produtor
// Kafka e roda o laço de publicação até receber SIGINT/SIGTERM.
// Objetivo: ser a composition root do relay. Publica o outbox e só depois o marca como
// publicado (at-least-once, assumido). No desligamento termina a rodada em andamento antes de
// fechar o produtor e o pool, para não abandonar um lote já publicado sem marcar.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/adapters/kafka"
	"github.com/VictorM0nteiro/payrouter/internal/adapters/postgres"
	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/logging"
	"github.com/VictorM0nteiro/payrouter/internal/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultBatch       = 100
	defaultPoll        = 500 * time.Millisecond
	defaultMetricsAddr = ":9091"

	// Prazos do pool do Postgres, os mesmos da API.
	connectTimeout    = 5 * time.Second
	maxConnLifetime   = 30 * time.Minute
	healthCheckPeriod = 30 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "relay:", err)
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
	batch, err := intFromEnv("RELAY_BATCH", defaultBatch)
	if err != nil {
		return err
	}
	poll, err := durationFromEnv("RELAY_POLL", defaultPoll)
	if err != nil {
		return err
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

	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	// SIGINT e SIGTERM encerram o laço; Run só devolve depois da rodada em andamento.
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-sigCtx.Done(); stop() }()

	// METRICS_ADDR definida e vazia desliga as métricas; ausente usa o padrão.
	metricsAddr, ok := os.LookupEnv("METRICS_ADDR")
	if !ok {
		metricsAddr = defaultMetricsAddr
	}
	reg := metrics.NewRegistry()
	metrics.RegisterBacklog(reg, postgres.NewBacklogReader(pool).Read)
	relay := app.NewRelay(postgres.NewOutboxStore(pool), producer, batch, poll).WithMetrics(metrics.NewRelay(reg))
	metricsDone := make(chan struct{})
	go func() {
		defer close(metricsDone)
		// As métricas são opcionais: se a porta não abrir, o relay segue sem elas.
		if err := metrics.Serve(sigCtx, metricsAddr, reg); err != nil {
			slog.Error("relay: servidor de métricas falhou", "addr", metricsAddr, "error", err)
		}
	}()
	slog.Info("relay iniciado", "brokers", brokers, "batch", batch, "poll", poll.String(), "metrics_addr", metricsAddr)
	err = relay.Run(sigCtx)
	stop()
	<-metricsDone
	slog.Info("relay encerrado")
	return err
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

// durationFromEnv lê uma duração (ex.: "500ms") da variável, ou devolve def se ela estiver
// vazia. Valor inválido ou não positivo impede a partida, em vez de cair num padrão em silêncio.
func durationFromEnv(name string, def time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s: %q não é uma duração positiva (ex.: 500ms)", name, raw)
	}
	return d, nil
}
