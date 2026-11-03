// main.go
// Ponto de entrada do processo projector: lê a configuração do ambiente (MONGO_URL, MONGO_DB,
// KAFKA_BROKERS, KAFKA_GROUP, PROJECTOR_WORKERS_MIN, PROJECTOR_WORKERS_MAX, PROJECTOR_QUEUE,
// METRICS_ADDR, LOG_FORMAT), liga o consumidor Kafka ao caso de uso ProjectEvent e roda até
// receber SIGINT/SIGTERM.
// Objetivo: ser a composition root do lado de leitura. Ele só lê o tópico e grava no Mongo: não
// fala com o Postgres nem com o participante, e não tem efeito colateral para fora; por isso
// pode ser parado, repetido ou reconstruído do zero sem perigo.

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
	"github.com/VictorM0nteiro/payrouter/internal/adapters/mongo"
	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/elastic"
	"github.com/VictorM0nteiro/payrouter/internal/logging"
	"github.com/VictorM0nteiro/payrouter/internal/metrics"
)

const (
	instructionsTopic  = "payrouter.instructions.events"
	defaultGroup       = "projector"
	defaultDatabase    = "payrouter"
	defaultMetricsAddr = ":9095"
	defaultQueue       = 64
	startupTimeout     = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "projector:", err)
		os.Exit(1)
	}
}

func run() error {
	slog.SetDefault(logging.New(os.Getenv("LOG_FORMAT"), os.Stdout))

	mongoURL := os.Getenv("MONGO_URL")
	if mongoURL == "" {
		return fmt.Errorf("MONGO_URL não definida (ex.: mongodb://mongo:27017)")
	}
	brokers := splitList(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS não definida (ex.: redpanda:9092)")
	}
	workers, err := workersConfig()
	if err != nil {
		return err
	}
	database := envOr("MONGO_DB", defaultDatabase)
	group := envOr("KAFKA_GROUP", defaultGroup)
	// METRICS_ADDR definida e vazia desliga as métricas; ausente usa o padrão.
	metricsAddr, ok := os.LookupEnv("METRICS_ADDR")
	if !ok {
		metricsAddr = defaultMetricsAddr
	}

	startCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()
	store, err := mongo.Connect(startCtx, mongoURL, database)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close(context.Background()) }()
	if err := store.EnsureIndexes(startCtx); err != nil {
		return fmt.Errorf("criando índices: %w", err)
	}

	consumer, err := kafka.NewConsumer(brokers, group, instructionsTopic)
	if err != nil {
		return err
	}
	defer consumer.Close()

	reg := metrics.NewRegistry()
	pm := metrics.NewProcessor(reg)
	useCase := app.NewProjectEvent(store).WithMetrics(metrics.NewProjector(reg))
	consumer.WithMetrics(pm).WithWorkers(workers)
	metrics.RegisterPool(reg, func() (int, int, int, int, int) {
		s := consumer.PoolStats()
		return s.Size, s.Target, s.Ready, s.Pending, s.Running
	})

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("projector iniciado", "brokers", brokers, "group", group, "database", database,
		"workers_min", workers.Min, "workers_max", workers.Max, "queue", workers.Queue, "metrics_addr", metricsAddr)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// As métricas são opcionais: se a porta não abrir, o projector segue sem elas.
		if err := metrics.Serve(sigCtx, metricsAddr, reg); err != nil {
			slog.Error("projector: servidor de métricas falhou", "addr", metricsAddr, "error", err)
		}
	}()
	err = consumer.Run(sigCtx, useCase.Handle)
	stop()
	wg.Wait()
	slog.Info("projector encerrado")
	return err
}

// workersConfig lê o tamanho do pool de workers. PROJECTOR_WORKERS_MAX ausente vale o piso. A
// ordem por agregado já é garantida pelo pool, então mais workers só paralelizam agregados diferentes.
func workersConfig() (elastic.Config, error) {
	var cfg elastic.Config
	var err error
	if cfg.Min, err = intFromEnv("PROJECTOR_WORKERS_MIN", 1); err != nil {
		return cfg, err
	}
	if cfg.Max, err = intFromEnv("PROJECTOR_WORKERS_MAX", cfg.Min); err != nil {
		return cfg, err
	}
	if cfg.Queue, err = intFromEnv("PROJECTOR_QUEUE", defaultQueue); err != nil {
		return cfg, err
	}
	if cfg.Max < cfg.Min {
		return cfg, fmt.Errorf("PROJECTOR_WORKERS_MAX (%d) menor que PROJECTOR_WORKERS_MIN (%d)", cfg.Max, cfg.Min)
	}
	return cfg, nil
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
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
