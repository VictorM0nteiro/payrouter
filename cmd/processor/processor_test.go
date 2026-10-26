// processor_test.go
// Testes de integração do processor contra um Postgres e um Redpanda reais (testcontainers).
// Objetivo: provar a sessão 12. O caso de uso envia no máximo uma vez por instrução, mesmo com
// a mensagem repetida ou com vários processors disputando a mesma; um envio sem resposta deixa
// a instrução em in_flight e nunca é reenviado; e, de ponta a ponta (outbox, relay, Kafka,
// processor, participante), uma instrução aceita chega a settled ou rejected sozinha.

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/VictorM0nteiro/payrouter/internal/adapters/kafka"
	pclient "github.com/VictorM0nteiro/payrouter/internal/adapters/participant"
	"github.com/VictorM0nteiro/payrouter/internal/adapters/postgres"
	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	sim "github.com/VictorM0nteiro/payrouter/internal/participant"
	"github.com/VictorM0nteiro/payrouter/internal/testutil/kafkatest"
	"github.com/VictorM0nteiro/payrouter/internal/testutil/pgtest"
)

var (
	testPool    *pgxpool.Pool
	testBrokers []string
	setupErr    error
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var stopPG, stopKafka func()
	testPool, stopPG, setupErr = pgtest.Start(ctx)
	if setupErr == nil {
		testBrokers, stopKafka, setupErr = kafkatest.Start(ctx)
	}
	if setupErr == nil {
		setupErr = kafkatest.CreateTopic(ctx, testBrokers, instructionsTopic, 3)
	}
	code := m.Run()
	if stopKafka != nil {
		stopKafka()
	}
	if stopPG != nil {
		stopPG()
	}
	os.Exit(code)
}

func requireInfra(t *testing.T) {
	t.Helper()
	if setupErr != nil {
		t.Fatalf("infraestrutura de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
}

// fakeParticipant conta os envios e devolve o que o teste mandar.
type fakeParticipant struct {
	calls  atomic.Int32
	result app.PaymentResult
	err    error
	delay  time.Duration
}

func (f *fakeParticipant) Pay(_ context.Context, _ app.PaymentRequest) (app.PaymentResult, error) {
	f.calls.Add(1)
	time.Sleep(f.delay)
	return f.result, f.err
}

// newAccepted grava um InstructionAccepted (versão 1) e devolve o id.
func newAccepted(t *testing.T, e2e string) domain.InstructionID {
	t.Helper()
	var raw string
	if err := testPool.QueryRow(context.Background(), `SELECT gen_random_uuid()::text`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	id := domain.InstructionID(raw)
	ev := domain.InstructionAccepted{ID: id, Amount: 15000, Debtor: "d", Creditor: "c", EndToEndID: e2e}
	if err := postgres.NewEventStore(testPool).Append(context.Background(), id, 0, ev); err != nil {
		t.Fatal(err)
	}
	return id
}

func load(t *testing.T, id domain.InstructionID) *domain.PaymentInstruction {
	t.Helper()
	events, err := postgres.NewEventStore(testPool).Load(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	p, err := domain.Rehydrate(events)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func acceptedMsg(id domain.InstructionID) app.Message {
	return app.Message{AggregateID: string(id), EventType: "InstructionAccepted", Version: 1}
}

func newUseCase(p app.Participant) *app.ProcessInstruction {
	return app.NewProcessInstruction(postgres.NewUnitOfWork(testPool), p)
}

func TestAceitaViraSettled(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "e2e-1")
	fake := &fakeParticipant{result: app.PaymentResult{Status: app.PaymentAccepted}}

	if err := newUseCase(fake).Handle(context.Background(), acceptedMsg(id)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	p := load(t, id)
	if p.Status() != domain.StatusSettled || p.Version() != 3 || fake.calls.Load() != 1 {
		t.Errorf("status=%s versão=%d envios=%d; esperado settled, 3, 1", p.Status(), p.Version(), fake.calls.Load())
	}
}

func TestRejeitadaViraRejectedComOCodigo(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "e2e-2")
	fake := &fakeParticipant{result: app.PaymentResult{Status: app.PaymentRejected, Code: "insufficient_funds"}}

	if err := newUseCase(fake).Handle(context.Background(), acceptedMsg(id)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if p := load(t, id); p.Status() != domain.StatusRejected {
		t.Errorf("status = %s, esperado rejected", p.Status())
	}
	var payload string
	if err := testPool.QueryRow(context.Background(),
		`SELECT payload::text FROM events WHERE aggregate_id = $1 AND event_type = 'ParticipantRejected'`, string(id)).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != `{"code": "insufficient_funds"}` {
		t.Errorf("payload = %s", payload)
	}
}

func TestMensagemRepetidaNaoReenvia(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "e2e-3")
	fake := &fakeParticipant{result: app.PaymentResult{Status: app.PaymentAccepted}}
	uc := newUseCase(fake)

	for range 3 {
		if err := uc.Handle(context.Background(), acceptedMsg(id)); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}

	if fake.calls.Load() != 1 {
		t.Errorf("envios = %d, esperado 1: a repetição não pode reenviar", fake.calls.Load())
	}
}

func TestDoisProcessorsNaMesmaMensagemEnviamUmaVezSo(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "e2e-4")
	// O atraso mantém todos os processors na disputa ao mesmo tempo.
	fake := &fakeParticipant{result: app.PaymentResult{Status: app.PaymentAccepted}, delay: 100 * time.Millisecond}
	uc := newUseCase(fake)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := uc.Handle(context.Background(), acceptedMsg(id)); err != nil {
				t.Errorf("Handle: %v", err)
			}
		}()
	}
	wg.Wait()

	if fake.calls.Load() != 1 {
		t.Errorf("envios = %d, esperado exatamente 1", fake.calls.Load())
	}
	if p := load(t, id); p.Status() != domain.StatusSettled {
		t.Errorf("status = %s, esperado settled", p.Status())
	}
}

func TestEnvioSemRespostaDeixaEmInFlightENuncaReenvia(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "e2e-5")
	fake := &fakeParticipant{err: fmt.Errorf("%w: timeout", app.ErrOutcomeUnknown)}
	uc := newUseCase(fake)

	err := uc.Handle(context.Background(), acceptedMsg(id))
	if !errors.Is(err, app.ErrOutcomeUnknown) {
		t.Fatalf("erro = %v, esperado ErrOutcomeUnknown", err)
	}
	// A mensagem é entregue de novo (at-least-once): não pode haver segundo envio.
	if err := uc.Handle(context.Background(), acceptedMsg(id)); err != nil {
		t.Fatalf("segunda entrega: %v", err)
	}

	if p := load(t, id); p.Status() != domain.StatusInFlight {
		t.Errorf("status = %s, esperado in_flight", p.Status())
	}
	if fake.calls.Load() != 1 {
		t.Errorf("envios = %d, esperado 1", fake.calls.Load())
	}
}

func TestErroDoParticipanteSemTipoTambemViraResultadoDesconhecido(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "e2e-6")
	fake := &fakeParticipant{err: errors.New("conexão resetada")}

	err := newUseCase(fake).Handle(context.Background(), acceptedMsg(id))

	if !errors.Is(err, app.ErrOutcomeUnknown) {
		t.Errorf("erro = %v, esperado ErrOutcomeUnknown: depois do envio, qualquer falha é desconhecida", err)
	}
}

// flakyUoW falha as chamadas de failFrom a failTo (a 1ª é a reivindicação, a 2ª em diante é a
// gravação da resposta) e deixa as outras passarem para o Postgres.
type flakyUoW struct {
	inner            app.UnitOfWork
	calls            atomic.Int32
	failFrom, failTo int32
}

func (f *flakyUoW) Do(ctx context.Context, fn func(ctx context.Context, tx app.Tx) error) error {
	if n := f.calls.Add(1); n >= f.failFrom && n <= f.failTo {
		return errors.New("conexão resetada")
	}
	return f.inner.Do(ctx, fn)
}

func TestFalhaTransitoriaAoGravarARespostaNaoPerdeOResultado(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "e2e-8")
	fake := &fakeParticipant{result: app.PaymentResult{Status: app.PaymentAccepted}}
	uc := app.NewProcessInstruction(&flakyUoW{inner: postgres.NewUnitOfWork(testPool), failFrom: 2, failTo: 2}, fake)

	if err := uc.Handle(context.Background(), acceptedMsg(id)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if p := load(t, id); p.Status() != domain.StatusSettled || fake.calls.Load() != 1 {
		t.Errorf("status=%s envios=%d; esperado settled e 1", p.Status(), fake.calls.Load())
	}
}

func TestGravacaoDaRespostaQueNuncaFuncionaViraResultadoDesconhecido(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "e2e-9")
	fake := &fakeParticipant{result: app.PaymentResult{Status: app.PaymentAccepted}}
	uc := app.NewProcessInstruction(&flakyUoW{inner: postgres.NewUnitOfWork(testPool), failFrom: 2, failTo: 99}, fake)

	err := uc.Handle(context.Background(), acceptedMsg(id))

	// Não pode virar erro transitório: a repetição seria pulada e o log mentiria.
	if !errors.Is(err, app.ErrOutcomeUnknown) {
		t.Fatalf("erro = %v, esperado ErrOutcomeUnknown", err)
	}
	if p := load(t, id); p.Status() != domain.StatusInFlight || fake.calls.Load() != 1 {
		t.Errorf("status=%s envios=%d; esperado in_flight e 1", p.Status(), fake.calls.Load())
	}
}

func TestOutrosEventosEVersaoDefasadaSaoIgnorados(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "e2e-7")
	fake := &fakeParticipant{result: app.PaymentResult{Status: app.PaymentAccepted}}
	uc := newUseCase(fake)

	other := app.Message{AggregateID: string(id), EventType: "RoutingAttempted", Version: 2}
	stale := app.Message{AggregateID: string(id), EventType: "InstructionAccepted", Version: 5}
	for _, m := range []app.Message{other, stale} {
		if err := uc.Handle(context.Background(), m); err != nil {
			t.Fatalf("Handle(%+v): %v", m, err)
		}
	}

	if fake.calls.Load() != 0 || load(t, id).Status() != domain.StatusAccepted {
		t.Errorf("envios=%d status=%s; esperado 0 e accepted", fake.calls.Load(), load(t, id).Status())
	}
}

func TestAgregadoInexistenteEErroTransitorio(t *testing.T) {
	requireInfra(t)
	fake := &fakeParticipant{}

	err := newUseCase(fake).Handle(context.Background(),
		app.Message{AggregateID: "00000000-0000-0000-0000-000000000000", EventType: "InstructionAccepted", Version: 1})

	if !errors.Is(err, app.ErrNotFound) || errors.Is(err, app.ErrOutcomeUnknown) {
		t.Errorf("erro = %v, esperado ErrNotFound (a mensagem será repetida)", err)
	}
	if fake.calls.Load() != 0 {
		t.Errorf("envios = %d, esperado 0", fake.calls.Load())
	}
}

// TestPontaAPontaPeloKafka percorre o caminho inteiro: evento + outbox no Postgres, relay,
// Redpanda, consumidor, caso de uso, participante simulado de verdade (HTTP).
func TestPontaAPontaPeloKafka(t *testing.T) {
	requireInfra(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := httptest.NewServer(sim.New(sim.Config{LateDelay: 1500 * time.Millisecond}))
	defer srv.Close()

	ids := map[string]domain.InstructionID{
		"accept": newAccepted(t, "accept:1"),
		"reject": newAccepted(t, "reject:1"),
		"late":   newAccepted(t, "late:1"),
	}
	ours := make(map[string]bool)
	for _, id := range ids {
		ours[string(id)] = true
	}

	// Relay: outbox -> Kafka.
	producer, err := kafka.NewProducer(testBrokers)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	relay := app.NewRelay(postgres.NewOutboxStore(testPool), producer, 100, 20*time.Millisecond)
	relayDone := make(chan error, 1)
	go func() { relayDone <- relay.Run(ctx) }()

	// Processor: Kafka -> caso de uso -> participante. O prazo (500ms) é menor que o atraso do late.
	consumer, err := kafka.NewConsumer(testBrokers, fmt.Sprintf("e2e-%d", time.Now().UnixNano()), instructionsTopic)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	uc := newUseCase(pclient.NewClient(srv.URL, 500*time.Millisecond))
	handle := func(ctx context.Context, m app.Message) error {
		if !ours[m.AggregateID] { // o tópico é compartilhado com os outros testes do pacote
			return nil
		}
		return uc.Handle(ctx, m)
	}
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- consumer.Run(ctx, handle) }()

	waitStatus(t, ids["accept"], domain.StatusSettled)
	waitStatus(t, ids["reject"], domain.StatusRejected)
	waitStatus(t, ids["late"], domain.StatusInFlight)

	// O late processou no participante, o cliente desistiu, e nada foi reenviado.
	time.Sleep(2 * time.Second) // passa o LateDelay e uma janela para um reenvio indevido aparecer
	for name, id := range ids {
		if got := participantReceived(t, srv.URL, id); got != 1 {
			t.Errorf("%s: o participante recebeu %d vezes, esperado exatamente 1", name, got)
		}
	}
	if p := load(t, ids["late"]); p.Status() != domain.StatusInFlight {
		t.Errorf("late: status = %s, esperado in_flight até a conciliação (sessão 13)", p.Status())
	}

	cancel()
	if err := <-relayDone; err != nil {
		t.Errorf("relay: %v", err)
	}
	if err := <-consumerDone; err != nil {
		t.Errorf("consumer: %v", err)
	}
}

func waitStatus(t *testing.T, id domain.InstructionID, want domain.Status) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if load(t, id).Status() == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("instrução %s não chegou a %s em 30s (está em %s)", id, want, load(t, id).Status())
}

func participantReceived(t *testing.T, baseURL string, id domain.InstructionID) int {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, baseURL+"/payments/"+string(id), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body struct {
		Received int `json:"received"`
	}
	if err := decodeJSON(resp, &body); err != nil {
		t.Fatal(err)
	}
	return body.Received
}
