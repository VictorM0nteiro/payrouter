// ratelimit_test.go
// Testes do limite de saída e da contrapressão, contra Postgres e Redpanda reais e o participante
// simulado de verdade (HTTP) com um TPS real.
// Objetivo: provar a sessão 17. Com o limite de saída ligado o participante nunca leva 429 e toda
// instrução termina settled (o lag cresce, nada é descartado); com o limite desligado ou maior
// que o do participante os 429 acontecem, a instrução volta a accepted, é reenviada, e termina
// settled com received == 1. Também prova que, cheio o limite, o caso de uso não grava nada.

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/adapters/kafka"
	pclient "github.com/VictorM0nteiro/payrouter/internal/adapters/participant"
	"github.com/VictorM0nteiro/payrouter/internal/adapters/postgres"
	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	sim "github.com/VictorM0nteiro/payrouter/internal/participant"
	"github.com/VictorM0nteiro/payrouter/internal/ratelimit"
)

// fakeLimiter manda esperar nas primeiras n reservas e depois libera.
type fakeLimiter struct {
	waits int
	after time.Duration
}

func (f *fakeLimiter) Reserve() time.Duration {
	if f.waits > 0 {
		f.waits--
		return f.after
	}
	return 0
}

func TestLimiteCheioNaoGravaNadaENaoEnvia(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "bp-1")
	fake := &fakeParticipant{result: app.PaymentResult{Status: app.PaymentAccepted}}
	lim := &fakeLimiter{waits: 2, after: 150 * time.Millisecond}
	uc := newUseCase(fake).WithLimiter(lim)

	for range 2 {
		err := uc.Handle(context.Background(), acceptedMsg(id))
		var bp *app.BackpressureError
		if !errors.As(err, &bp) || bp.After != 150*time.Millisecond {
			t.Fatalf("erro = %v, esperado *BackpressureError de 150ms", err)
		}
	}

	if got := join(eventTypes(t, id)); got != "InstructionAccepted" {
		t.Errorf("eventos = %s, esperado só InstructionAccepted: esperar não grava nada", got)
	}
	if fake.calls.Load() != 0 {
		t.Errorf("envios = %d, esperado 0", fake.calls.Load())
	}
	// Com o limite liberado, a MESMA mensagem segue o caminho normal.
	if err := uc.Handle(context.Background(), acceptedMsg(id)); err != nil {
		t.Fatal(err)
	}
	if got := load(t, id).Status(); got != domain.StatusSettled {
		t.Errorf("status = %s, esperado settled", got)
	}
}

// pipeline sobe relay, consumidor e conciliação para as instruções dadas e devolve quem encerra.
func pipeline(t *testing.T, participantURL string, ids []domain.InstructionID, limiter app.Limiter) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ours := make(map[string]bool)
	for _, id := range ids {
		ours[string(id)] = true
	}

	producer, err := kafka.NewProducer(testBrokers)
	if err != nil {
		t.Fatal(err)
	}
	relay := app.NewRelay(postgres.NewOutboxStore(testPool), producer, 100, 20*time.Millisecond)
	relayDone := make(chan error, 1)
	go func() { relayDone <- relay.Run(ctx) }()

	consumer, err := kafka.NewConsumer(testBrokers, fmt.Sprintf("rl-%d", time.Now().UnixNano()), instructionsTopic)
	if err != nil {
		t.Fatal(err)
	}
	client := pclient.NewClient(participantURL, 2*time.Second)
	uc := app.NewProcessInstruction(postgres.NewUnitOfWork(testPool), client, 200*time.Millisecond)
	if limiter != nil {
		uc.WithLimiter(limiter)
	}
	reconciler := app.NewReconcile(postgres.NewUnitOfWork(testPool), client, app.ReconcileConfig{
		Batch: 10, Lease: 30 * time.Second, Poll: 100 * time.Millisecond,
		BackoffBase: 200 * time.Millisecond, BackoffMax: time.Second, MaxAttempts: 3,
	})
	reconcilerDone := make(chan error, 1)
	go func() { reconcilerDone <- reconciler.Run(ctx) }()
	handle := func(ctx context.Context, m app.Message) error {
		if !ours[m.AggregateID] { // o tópico é compartilhado com os outros testes do pacote
			return nil
		}
		return uc.Handle(ctx, m)
	}
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- consumer.Run(ctx, handle) }()

	return func() {
		cancel()
		if err := <-relayDone; err != nil {
			t.Errorf("relay: %v", err)
		}
		if err := <-consumerDone; err != nil {
			t.Errorf("consumer: %v", err)
		}
		if err := <-reconcilerDone; err != nil {
			t.Errorf("reconciler: %v", err)
		}
		consumer.Close()
		producer.Close()
	}
}

func newBatch(t *testing.T, prefix string, n int) []domain.InstructionID {
	t.Helper()
	ids := make([]domain.InstructionID, n)
	for i := range ids {
		ids[i] = newAccepted(t, fmt.Sprintf("%s-%d", prefix, i))
	}
	return ids
}

func participantThrottled(t *testing.T, baseURL string, id domain.InstructionID) int {
	t.Helper()
	return participantCounter(t, baseURL, id, "throttled")
}

func TestLimiteDeSaidaSegueOTPSDoParticipanteESemNenhum429(t *testing.T) {
	requireInfra(t)
	// O participante aguenta 4/s (rajada 1). O processor se limita a 3/s, abaixo disso.
	srv := httptest.NewServer(sim.New(sim.Config{TPS: 4, Burst: 1}))
	defer srv.Close()
	const n = 9
	ids := newBatch(t, "rl-ok", n)

	start := time.Now()
	stop := pipeline(t, srv.URL, ids, ratelimit.New(3, 1))
	defer stop()
	for _, id := range ids {
		waitStatus(t, id, domain.StatusSettled)
	}
	elapsed := time.Since(start)

	for _, id := range ids {
		if got := participantReceived(t, srv.URL, id); got != 1 {
			t.Errorf("%s: o participante recebeu %d, esperado 1", id, got)
		}
		if got := participantThrottled(t, srv.URL, id); got != 0 {
			t.Errorf("%s: levou %d 429, esperado 0: o limite de saída vem antes do participante", id, got)
		}
		if got, want := join(eventTypes(t, id)), "InstructionAccepted,RoutingAttempted,ParticipantAccepted"; got != want {
			t.Errorf("%s: eventos = %s, esperado %s", id, got, want)
		}
	}
	// 9 envios a 3/s, com rajada de 1: pelo menos (9-1)/3 s. Prova que o limite SEGUROU o consumo.
	if min := 2500 * time.Millisecond; elapsed < min {
		t.Errorf("terminou em %v, esperado >= %v: o limite não segurou a fila", elapsed, min)
	}
}

func TestSemLimiteDeSaidaOs429AcontecemEATudoAindaTerminaSettled(t *testing.T) {
	requireInfra(t)
	// O processor não se limita e o participante aguenta 3/s: haverá 429. Cada um é "não
	// processado", a instrução volta a accepted, é reenviada, e termina settled.
	srv := httptest.NewServer(sim.New(sim.Config{TPS: 3, Burst: 1}))
	defer srv.Close()
	ids := newBatch(t, "rl-429", 6)

	stop := pipeline(t, srv.URL, ids, nil)
	defer stop()
	for _, id := range ids {
		waitStatus(t, id, domain.StatusSettled)
	}

	total := 0
	for _, id := range ids {
		if got := participantReceived(t, srv.URL, id); got != 1 {
			t.Errorf("%s: o participante recebeu %d, esperado 1 (admitido uma vez só)", id, got)
		}
		total += participantThrottled(t, srv.URL, id)
	}
	if total == 0 {
		t.Errorf("nenhum 429 aconteceu, esperado pelo menos um (o limite do participante é menor que a carga)")
	}
}

// participantCounter lê um contador numérico do GET /payments/{id} do participante.
func participantCounter(t *testing.T, baseURL string, id domain.InstructionID, field string) int {
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
	var body map[string]any
	if err := decodeJSON(resp, &body); err != nil {
		t.Fatal(err)
	}
	v, _ := body[field].(float64)
	return int(v)
}
