// relay_test.go
// Testes do caso de uso Relay com fakes em memória, sem Docker.
// Objetivo: provar o contrato que importa à tese: a mensagem só sai do outbox DEPOIS de o
// broker confirmar; publicação que falha deixa tudo pendente e é repetida; o laço não
// descarta nem trava no desligamento.

package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeStore guarda mensagens pendentes e imita o contrato de Drain: marca só se publish der certo.
type fakeStore struct {
	mu      sync.Mutex
	pending []OutboxMessage
}

func (s *fakeStore) Drain(ctx context.Context, limit int, publish func(ctx context.Context, msgs []OutboxMessage) error) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := min(limit, len(s.pending))
	if n == 0 {
		return 0, nil
	}
	if err := publish(ctx, s.pending[:n]); err != nil {
		return 0, err
	}
	s.pending = s.pending[n:]
	return n, nil
}

func (s *fakeStore) pendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// fakePublisher registra o que recebeu e falha as primeiras failures chamadas.
type fakePublisher struct {
	mu       sync.Mutex
	failures int
	calls    int
	got      []OutboxMessage
}

func (p *fakePublisher) Publish(_ context.Context, msgs []OutboxMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.failures > 0 {
		p.failures--
		return errors.New("broker fora do ar")
	}
	p.got = append(p.got, msgs...)
	return nil
}

func (p *fakePublisher) snapshot() (calls, received int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls, len(p.got)
}

func messages(n int) []OutboxMessage {
	out := make([]OutboxMessage, n)
	for i := range out {
		out[i] = OutboxMessage{ID: int64(i + 1), AggregateID: "agg", Topic: "t", Version: i + 1}
	}
	return out
}

func TestRelayRunOncePublicaEEntaoEsvaziaOPendente(t *testing.T) {
	store := &fakeStore{pending: messages(3)}
	pub := &fakePublisher{}
	r := NewRelay(store, pub, 10, time.Millisecond)

	n, err := r.RunOnce(context.Background())

	if err != nil || n != 3 {
		t.Fatalf("RunOnce = %d, %v; esperado 3, nil", n, err)
	}
	if _, received := pub.snapshot(); received != 3 || store.pendingCount() != 0 {
		t.Errorf("recebidas=%d pendentes=%d; esperado 3 e 0", received, store.pendingCount())
	}
}

func TestRelayRunOnceRespeitaOTamanhoDoLote(t *testing.T) {
	store := &fakeStore{pending: messages(5)}
	r := NewRelay(store, &fakePublisher{}, 2, time.Millisecond)

	n, err := r.RunOnce(context.Background())

	if err != nil || n != 2 || store.pendingCount() != 3 {
		t.Fatalf("n=%d err=%v pendentes=%d; esperado 2, nil, 3", n, err, store.pendingCount())
	}
}

func TestRelayPublicacaoQueFalhaNaoMarcaNada(t *testing.T) {
	store := &fakeStore{pending: messages(3)}
	pub := &fakePublisher{failures: 1}
	r := NewRelay(store, pub, 10, time.Millisecond)

	n, err := r.RunOnce(context.Background())

	if err == nil || n != 0 {
		t.Fatalf("RunOnce = %d, %v; esperado erro e 0", n, err)
	}
	if store.pendingCount() != 3 {
		t.Errorf("pendentes = %d, esperado 3: falha de publicação não pode consumir o outbox", store.pendingCount())
	}
}

func TestRelayRunRepeteDepoisDeErroENaoPerdeNada(t *testing.T) {
	store := &fakeStore{pending: messages(4)}
	pub := &fakePublisher{failures: 2}
	r := NewRelay(store, pub, 2, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for store.pendingCount() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Run = %v, esperado nil no desligamento", err)
	}
	if calls, received := pub.snapshot(); received != 4 || calls < 3 {
		t.Errorf("chamadas=%d recebidas=%d; esperado 4 recebidas e retentativas após as 2 falhas", calls, received)
	}
}

func TestRelayRunSaiLimpoComOutboxVazioQuandoOContextoTermina(t *testing.T) {
	r := NewRelay(&fakeStore{}, &fakePublisher{}, 10, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run = %v, esperado nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run não saiu depois do cancelamento, mesmo dormindo no intervalo")
	}
}
