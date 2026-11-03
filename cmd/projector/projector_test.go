// projector_test.go
// Teste ponta a ponta do projetor: event store (Postgres) -> outbox -> relay -> Redpanda ->
// consumidor -> ProjectEvent -> Mongo, tudo real, e a configuração de workers.
// Objetivo: provar que o documento no Mongo converge para o MESMO estado que o domínio reconstrói
// do event store (a fonte autoritativa), em históricos de formatos diferentes, mesmo com vários
// workers e várias partições; e que o projetor é um leitor puro, sem nunca falar com o Postgres.

package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/adapters/kafka"
	"github.com/VictorM0nteiro/payrouter/internal/adapters/mongo"
	"github.com/VictorM0nteiro/payrouter/internal/adapters/postgres"
	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	"github.com/VictorM0nteiro/payrouter/internal/elastic"
	"github.com/VictorM0nteiro/payrouter/internal/testutil/kafkatest"
	"github.com/VictorM0nteiro/payrouter/internal/testutil/mongotest"
	"github.com/VictorM0nteiro/payrouter/internal/testutil/pgtest"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	testPool    *pgxpool.Pool
	testBrokers []string
	testMongo   string
	setupErr    error
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var stopPG, stopKafka, stopMongo func()
	testPool, stopPG, setupErr = pgtest.Start(ctx)
	if setupErr == nil {
		testBrokers, stopKafka, setupErr = kafkatest.Start(ctx)
	}
	if setupErr == nil {
		setupErr = kafkatest.CreateTopic(ctx, testBrokers, instructionsTopic, 3)
	}
	if setupErr == nil {
		testMongo, stopMongo, setupErr = mongotest.Start(ctx)
	}
	code := m.Run()
	for _, stop := range []func(){stopMongo, stopKafka, stopPG} {
		if stop != nil {
			stop()
		}
	}
	os.Exit(code)
}

// trail grava no event store, em ordem, os eventos que o domínio produz a partir do Accept.
func trail(t *testing.T, i int, steps ...func(*domain.PaymentInstruction) (domain.Event, error)) domain.InstructionID {
	t.Helper()
	var raw string
	if err := testPool.QueryRow(context.Background(), `SELECT gen_random_uuid()::text`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	id := domain.InstructionID(raw)
	p, first, err := domain.Accept(id, domain.Money(1000+i), "d", "c", fmt.Sprintf("e%d", i))
	if err != nil {
		t.Fatal(err)
	}
	store := postgres.NewEventStore(testPool)
	if err := store.Append(context.Background(), id, 0, first); err != nil {
		t.Fatal(err)
	}
	for _, step := range steps {
		ev, err := step(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Append(context.Background(), id, p.Version()-1, ev); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

func TestProjecaoConvergeParaOEstadoDoEventStore(t *testing.T) {
	if setupErr != nil {
		t.Fatalf("infraestrutura de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	route := func(p *domain.PaymentInstruction) (domain.Event, error) { return p.Route() }
	settle := func(p *domain.PaymentInstruction) (domain.Event, error) { return p.Settle() }
	reject := func(p *domain.PaymentInstruction) (domain.Event, error) { return p.Reject("insufficient_funds") }
	throttle := func(p *domain.PaymentInstruction) (domain.Event, error) { return p.Throttle() }
	timeout := func(p *domain.PaymentInstruction) (domain.Event, error) { return p.RecordTimeout() }
	query := func(p *domain.PaymentInstruction) (domain.Event, error) {
		return p.RecordStatusQuery(domain.QueryUnknown)
	}
	exhaust := func(p *domain.PaymentInstruction) (domain.Event, error) { return p.Exhaust(3) }

	var ids []domain.InstructionID
	for i := range 6 { // várias instruções de cada formato, para cair em todas as partições
		ids = append(ids,
			trail(t, i),
			trail(t, i, route, settle),
			trail(t, i, route, reject),
			trail(t, i, route, throttle, route, settle),
			trail(t, i, route, timeout, query, query, exhaust),
		)
	}

	producer, err := kafka.NewProducer(testBrokers)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	relay := app.NewRelay(postgres.NewOutboxStore(testPool), producer, 100, 20*time.Millisecond)
	go func() { _ = relay.Run(ctx) }()

	store, err := mongo.Connect(ctx, testMongo, "e2e")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close(context.Background()) }()
	consumer, err := kafka.NewConsumer(testBrokers, fmt.Sprintf("projector-e2e-%d", time.Now().UnixNano()), instructionsTopic)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	consumer.WithWorkers(elastic.Config{Min: 4, Max: 4, Queue: 32})
	go func() { _ = consumer.Run(ctx, app.NewProjectEvent(store).Handle) }()

	for _, id := range ids {
		events, err := postgres.NewEventStore(testPool).Load(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		want, err := domain.Rehydrate(events)
		if err != nil {
			t.Fatal(err)
		}
		waitDoc(t, ctx, store, id, want)
	}
}

// waitDoc espera o documento alcançar a versão do agregado e confere cada campo contra o domínio.
func waitDoc(t *testing.T, ctx context.Context, store *mongo.ProjectionStore, id domain.InstructionID, want *domain.PaymentInstruction) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		doc, found, err := store.Get(ctx, string(id))
		if err != nil {
			t.Fatal(err)
		}
		if found && doc.Version == want.Version() {
			if doc.Status != string(want.Status()) || doc.Attempts != want.Attempts() ||
				doc.AmountCents != want.Amount().Cents() || doc.Debtor != want.Debtor() ||
				doc.Creditor != want.Creditor() || doc.EndToEndID != want.EndToEndID() {
				t.Errorf("%s: documento %+v difere do domínio (status %s, tentativas %d, valor %d)",
					id, doc, want.Status(), want.Attempts(), want.Amount().Cents())
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: o documento não chegou à versão %d em 60s (found=%v, doc=%+v)", id, want.Version(), found, doc)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestConfigDeWorkers(t *testing.T) {
	t.Setenv("PROJECTOR_WORKERS_MIN", "2")
	t.Setenv("PROJECTOR_WORKERS_MAX", "")
	t.Setenv("PROJECTOR_QUEUE", "")
	cfg, err := workersConfig()
	if err != nil || cfg.Min != 2 || cfg.Max != 2 || cfg.Queue != defaultQueue {
		t.Errorf("padrão = %+v, %v; esperado 2/2/%d", cfg, err, defaultQueue)
	}

	t.Setenv("PROJECTOR_WORKERS_MAX", "1")
	if _, err := workersConfig(); err == nil {
		t.Error("MAX menor que MIN deveria ser erro")
	}
	t.Setenv("PROJECTOR_WORKERS_MAX", "x")
	if _, err := workersConfig(); err == nil {
		t.Error("valor não numérico deveria ser erro")
	}
}
