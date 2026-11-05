// redelivery_test.go
// Testes ponta a ponta do projetor contra a entrega real do Kafka: a mesma mensagem de novo
// (redelivery) e a mensagem fora de ordem (reordering), com Redpanda e Mongo de verdade.
// Objetivo: responder pelas perguntas de defesa 2 ("o mesmo evento chega duas vezes ao projetor")
// e 3 ("dois eventos do mesmo agregado chegam fora de ordem"). O desfecho esperado é sempre o mesmo:
// o documento converge para o estado que o histórico em ordem produz, sem inflar tentativas e sem
// travar a partição.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/adapters/kafka"
	"github.com/VictorM0nteiro/payrouter/internal/adapters/mongo"
	"github.com/VictorM0nteiro/payrouter/internal/adapters/postgres"
	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	"github.com/VictorM0nteiro/payrouter/internal/elastic"
	"github.com/VictorM0nteiro/payrouter/internal/metrics"
	"github.com/VictorM0nteiro/payrouter/internal/testutil/kafkatest"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// event é um evento do histórico de um agregado, como o event store o publicaria.
type event struct {
	version   int
	eventType string
	data      string
}

// history devolve um histórico válido de seis eventos que termina em settled:
// accepted, attempted, throttled, attempted, timed out, settled. Tem duas tentativas.
func history() []event {
	return []event{
		{1, "InstructionAccepted", `{"id":"x","amount_cents":4200,"debtor":"d","creditor":"c","end_to_end_id":"e"}`},
		{2, "RoutingAttempted", `{}`},
		{3, "RoutingThrottled", `{}`},
		{4, "RoutingAttempted", `{}`},
		{5, "RoutingTimedOut", `{}`},
		{6, "ParticipantAccepted", `{}`},
	}
}

// message monta o registro do tópico para o evento do agregado id.
func message(id string, e event) app.OutboxMessage {
	body, _ := json.Marshal(map[string]any{
		"aggregate_id": id, "version": e.version, "event_type": e.eventType,
		"occurred_at": time.Date(2026, 11, 3, 12, 0, e.version, 0, time.UTC), "data": json.RawMessage(e.data),
	})
	return app.OutboxMessage{AggregateID: id, Topic: instructionsTopic, EventType: e.eventType, Version: e.version, Payload: body}
}

// publish grava os eventos no tópico, um registro por vez, na ordem dada.
func publish(t *testing.T, ctx context.Context, id string, events ...event) {
	t.Helper()
	producer, err := kafka.NewProducer(testBrokers)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	for _, e := range events {
		if err := producer.Publish(ctx, []app.OutboxMessage{message(id, e)}); err != nil {
			t.Fatal(err)
		}
	}
}

// projector sobe um projetor completo sobre um banco novo e devolve a store e o registro de métricas.
func projector(t *testing.T, ctx context.Context) (*mongo.ProjectionStore, *prometheus.Registry) {
	t.Helper()
	if setupErr != nil {
		t.Fatalf("infraestrutura de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
	store, err := mongo.Connect(ctx, testMongo, fmt.Sprintf("r%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	consumer, err := kafka.NewConsumer(testBrokers, fmt.Sprintf("projector-r-%d", time.Now().UnixNano()), instructionsTopic)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(consumer.Close)
	consumer.WithWorkers(elastic.Config{Min: 4, Max: 4, Queue: 32})
	reg := metrics.NewRegistry()
	useCase := app.NewProjectEvent(store).WithMetrics(metrics.NewProjector(reg))
	go func() { _ = consumer.Run(ctx, useCase.Handle) }()
	return store, reg
}

// waitVersion espera o documento chegar à versão e o devolve.
func waitVersion(t *testing.T, ctx context.Context, store *mongo.ProjectionStore, id string, version int) mongo.Instruction {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		doc, found, err := store.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if found && doc.Version == version {
			return doc
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: o documento não chegou à versão %d em 20s (found=%v, doc=%+v)", id, version, found, doc)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func newID() string { return uuid.NewString() }

// TestEventosEmOrdemInversaConvergem: os seis eventos de um agregado entram no tópico do último para
// o primeiro, todos na mesma partição (mesma chave). O documento tem de chegar ao mesmo estado final
// que a ordem normal, e um agregado novo, depois, tem de ser projetado normalmente.
func TestEventosEmOrdemInversaConvergem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	store, _ := projector(t, ctx)

	id := newID()
	h := history()
	for i := len(h) - 1; i >= 0; i-- {
		publish(t, ctx, id, h[i])
	}
	fresh := newID()
	publish(t, ctx, fresh, h[0])

	doc := waitVersion(t, ctx, store, id, 6)
	if doc.Status != "settled" || doc.Attempts != 2 || doc.AmountCents != 4200 {
		t.Errorf("documento = %+v, esperado settled com 2 tentativas e 4200 centavos", doc)
	}
	if len(doc.Pending) != 0 {
		t.Errorf("sobrou pending: %v", doc.Pending)
	}
	waitVersion(t, ctx, store, fresh, 1)
}

// counterOf lê projector_events_total{result} do registro.
func counterOf(t *testing.T, reg *prometheus.Registry, result string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, fam := range families {
		if fam.GetName() != "payrouter_projector_events_total" {
			continue
		}
		for _, m := range fam.GetMetric() {
			if m.GetLabel()[0].GetValue() == result {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// handled soma os registros que o projetor já tratou (cada registro dá exatamente um destes quatro).
func handled(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()
	return counterOf(t, reg, "applied") + counterOf(t, reg, "duplicate") + counterOf(t, reg, "buffered") + counterOf(t, reg, "invalid")
}

// expected reconstrói do event store o estado que o domínio calcula para o agregado.
func expected(t *testing.T, ctx context.Context, id domain.InstructionID) *domain.PaymentInstruction {
	t.Helper()
	events, err := postgres.NewEventStore(testPool).Load(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	p, err := domain.Rehydrate(events)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// shapes grava no event store instruções de formatos diferentes e devolve os ids.
func shapes(t *testing.T, copies int) []domain.InstructionID {
	t.Helper()
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
	for i := range copies {
		ids = append(ids,
			trail(t, i),
			trail(t, i, route, settle),
			trail(t, i, route, reject),
			trail(t, i, route, throttle, route, settle),
			trail(t, i, route, timeout, query, query, exhaust),
		)
	}
	return ids
}

// TestRepublicarOTopicoInteiroNaoMudaNada: depois que a projeção convergiu, TODOS os registros do tópico
// são publicados de novo, embaralhados (o pior redelivery que o Kafka pode fazer). Os documentos têm de
// ficar idênticos, as tentativas não podem inflar e a partição tem de seguir andando.
func TestRepublicarOTopicoInteiroNaoMudaNada(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	ids := shapes(t, 4)
	store, reg := projector(t, ctx)

	producer, err := kafka.NewProducer(testBrokers)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	relay := app.NewRelay(postgres.NewOutboxStore(testPool), producer, 100, 20*time.Millisecond)
	go func() { _ = relay.Run(ctx) }()
	for _, id := range ids {
		waitDoc(t, ctx, store, id, expected(t, ctx, id))
	}
	before := map[domain.InstructionID]mongo.Instruction{}
	for _, id := range ids {
		doc, _, _ := store.Get(ctx, string(id))
		before[id] = doc
	}

	records, err := kafkatest.ReadAll(ctx, testBrokers, instructionsTopic, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	total := float64(len(records))
	rand.New(rand.NewSource(7)).Shuffle(len(records), func(i, j int) { records[i], records[j] = records[j], records[i] })
	for _, r := range records {
		version, _ := strconv.Atoi(r.Headers["version"])
		msg := app.OutboxMessage{AggregateID: r.Key, Topic: instructionsTopic, EventType: r.Headers["event_type"], Version: version, Payload: r.Value}
		if err := producer.Publish(ctx, []app.OutboxMessage{msg}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 60*time.Second, "o projetor tratar os registros republicados", func() bool { return handled(t, reg) >= 2*total })

	for _, id := range ids {
		doc, _, _ := store.Get(ctx, string(id))
		if fmt.Sprint(doc) != fmt.Sprint(before[id]) {
			t.Errorf("%s: documento mudou com o redelivery: antes %+v, depois %+v", id, before[id], doc)
		}
		if len(doc.Pending) != 0 {
			t.Errorf("%s: sobrou pending %v", id, doc.Pending)
		}
	}
	if dup := counterOf(t, reg, "duplicate"); dup < total {
		t.Errorf("duplicate = %v, esperado ao menos %v (todo registro republicado já estava aplicado)", dup, total)
	}
	fresh := newID()
	publish(t, ctx, fresh, history()[0])
	waitVersion(t, ctx, store, fresh, 1) // a partição segue andando
}

// waitFor espera cond ficar verdadeira.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("esperando %s: não aconteceu em %s", what, d)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// flakyPublisher publica o lote e DEPOIS falha nas primeiras vezes, como um relay que cai entre publicar
// e marcar: as linhas ficam pendentes e saem de novo.
type flakyPublisher struct {
	inner *kafka.Producer
	calls atomic.Int32
	fail  int32
}

func (f *flakyPublisher) Publish(ctx context.Context, msgs []app.OutboxMessage) error {
	if err := f.inner.Publish(ctx, msgs); err != nil {
		return err
	}
	if f.calls.Add(1) <= f.fail {
		return errors.New("caiu entre publicar e marcar")
	}
	return nil
}

// TestRelayQueRepublicaPorCairAntesDeMarcarNaoDuplicaEfeito: o at-least-once como o relay de verdade o
// produz. Os lotes saem duas vezes; os documentos têm de bater com o domínio e o projetor tem de ter
// visto duplicatas.
func TestRelayQueRepublicaPorCairAntesDeMarcarNaoDuplicaEfeito(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ids := shapes(t, 3)
	store, reg := projector(t, ctx)

	producer, err := kafka.NewProducer(testBrokers)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	relay := app.NewRelay(postgres.NewOutboxStore(testPool), &flakyPublisher{inner: producer, fail: 4}, 5, 20*time.Millisecond)
	go func() { _ = relay.Run(ctx) }()

	for _, id := range ids {
		waitDoc(t, ctx, store, id, expected(t, ctx, id))
	}
	waitFor(t, 30*time.Second, "o projetor ver duplicatas", func() bool { return counterOf(t, reg, "duplicate") > 0 })
}

// producePartition grava um evento direto numa partição, para simular o agregado repartido.
func producePartition(t *testing.T, ctx context.Context, partition int32, id string, e event) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(testBrokers...), kgo.DefaultProduceTopic(instructionsTopic), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	m := message(id, e)
	rec := &kgo.Record{Key: []byte(id), Value: m.Payload, Partition: partition, Headers: []kgo.RecordHeader{
		{Key: "event_type", Value: []byte(e.eventType)},
		{Key: "version", Value: []byte(strconv.Itoa(e.version))},
	}}
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// TestPartesDoAgregadoEmParticoesDiferentesChegandoDesordenadas: o caso real de reordenação, quando o
// número de partições muda e a mesma chave passa a cair em outra. Os eventos 4 a 6 estão na partição 2 e
// chegam antes dos eventos 1 a 3, da partição 0.
func TestPartesDoAgregadoEmParticoesDiferentesChegandoDesordenadas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	store, reg := projector(t, ctx)

	id := newID()
	h := history()
	for _, e := range h[3:] {
		producePartition(t, ctx, 2, id, e)
	}
	waitFor(t, 20*time.Second, "o projetor guardar os adiantados", func() bool { return counterOf(t, reg, "buffered") >= 3 })
	for _, e := range h[:3] {
		producePartition(t, ctx, 0, id, e)
	}

	doc := waitVersion(t, ctx, store, id, 6)
	if doc.Status != "settled" || doc.Attempts != 2 || len(doc.Pending) != 0 {
		t.Errorf("documento = %+v, esperado settled com 2 tentativas e sem pending", doc)
	}
	if got := counterOf(t, reg, "drained"); got < 3 {
		t.Errorf("drained = %v, esperado ao menos 3 (os guardados aplicados em cadeia)", got)
	}
}
