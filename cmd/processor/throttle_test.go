// throttle_test.go
// Testes do 429 no processor contra um Postgres real (testcontainers), com um participante falso.
// Objetivo: provar que 429 e timeout recebem reações OPOSTAS. O 429 é "definitivamente não
// processado": a instrução volta a accepted (RoutingThrottled, sem unknown e sem tarefa de
// conciliação) e a mensagem desse evento a ENVIA de novo. O timeout é "não sei": a instrução vira
// unknown e uma mensagem repetida nunca reenvia. A idempotência continua sendo pela versão.

package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

// scriptedParticipant responde ao Pay conforme um roteiro por instrução e conta os envios.
type scriptedParticipant struct {
	mu    sync.Mutex
	sends map[string]int
	reply func(attempt int) (app.PaymentResult, error) // attempt começa em 1
}

func (s *scriptedParticipant) Pay(_ context.Context, req app.PaymentRequest) (app.PaymentResult, error) {
	s.mu.Lock()
	if s.sends == nil {
		s.sends = make(map[string]int)
	}
	s.sends[req.InstructionID]++
	n := s.sends[req.InstructionID]
	s.mu.Unlock()
	return s.reply(n)
}

func (s *scriptedParticipant) Status(context.Context, string) (app.PaymentResult, error) {
	return app.PaymentResult{}, errors.New("não usado")
}

func (s *scriptedParticipant) sent(id domain.InstructionID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sends[string(id)]
}

func throttledMsg(id domain.InstructionID, version int) app.Message {
	return app.Message{AggregateID: string(id), EventType: "RoutingThrottled", Version: version}
}

func TestA429DevolveAAcceptedEAMensagemDoEventoReenvia(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "th-1")
	p := &scriptedParticipant{reply: func(attempt int) (app.PaymentResult, error) {
		if attempt == 1 {
			return app.PaymentResult{}, &app.ThrottledError{RetryAfter: 10 * time.Millisecond}
		}
		return app.PaymentResult{Status: app.PaymentAccepted}, nil
	}}
	uc := newUseCase(p)

	if err := uc.Handle(context.Background(), acceptedMsg(id)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// 429: nada foi processado. Volta a accepted, sem unknown e sem tarefa de conciliação.
	if got := load(t, id); got.Status() != domain.StatusAccepted || got.Version() != 3 {
		t.Fatalf("status=%s versão=%d; esperado accepted, 3", got.Status(), got.Version())
	}
	if got, want := join(eventTypes(t, id)), "InstructionAccepted,RoutingAttempted,RoutingThrottled"; got != want {
		t.Fatalf("eventos = %s, esperado %s", got, want)
	}
	if n, _, _ := tasks(t, id); n != 0 {
		t.Errorf("tarefas = %d, esperado 0: 429 não é unknown", n)
	}

	// A mensagem do RoutingThrottled (versão 3) reenvia, e agora o participante aceita.
	if err := uc.Handle(context.Background(), throttledMsg(id, 3)); err != nil {
		t.Fatalf("Handle(RoutingThrottled): %v", err)
	}
	if got := load(t, id).Status(); got != domain.StatusSettled {
		t.Errorf("status = %s, esperado settled", got)
	}
	want := "InstructionAccepted,RoutingAttempted,RoutingThrottled,RoutingAttempted,ParticipantAccepted"
	if got := join(eventTypes(t, id)); got != want {
		t.Errorf("eventos = %s, esperado %s", got, want)
	}
	if p.sent(id) != 2 {
		t.Errorf("envios = %d, esperado 2: o 429 autoriza o reenvio", p.sent(id))
	}
}

func TestMensagemDeRoutingThrottledRepetidaOuDefasadaNaoReenvia(t *testing.T) {
	requireInfra(t)
	id := newAccepted(t, "th-2")
	p := &scriptedParticipant{reply: func(attempt int) (app.PaymentResult, error) {
		if attempt == 1 {
			return app.PaymentResult{}, &app.ThrottledError{}
		}
		return app.PaymentResult{Status: app.PaymentAccepted}, nil
	}}
	uc := newUseCase(p)
	if err := uc.Handle(context.Background(), acceptedMsg(id)); err != nil {
		t.Fatal(err)
	}
	if err := uc.Handle(context.Background(), throttledMsg(id, 3)); err != nil {
		t.Fatal(err)
	}
	sent := p.sent(id)

	// Repetida (a instrução já avançou) e defasada (versão que não é a atual): nada a fazer.
	for _, m := range []app.Message{throttledMsg(id, 3), throttledMsg(id, 2), acceptedMsg(id)} {
		if err := uc.Handle(context.Background(), m); err != nil {
			t.Fatalf("Handle(%+v): %v", m, err)
		}
	}

	if p.sent(id) != sent {
		t.Errorf("envios = %d, esperado %d: mensagem repetida nunca reenvia", p.sent(id), sent)
	}
}

// TestA429ETimeoutTemReacoesOpostas é a pergunta 9 do projeto: duas falhas que parecem iguais no
// log e não podem ser tratadas igual.
func TestA429ETimeoutTemReacoesOpostas(t *testing.T) {
	requireInfra(t)
	idThrottled := newAccepted(t, "th-3a")
	idTimeout := newAccepted(t, "th-3b")
	byID := map[string]error{
		string(idThrottled): &app.ThrottledError{},
		string(idTimeout):   app.ErrOutcomeUnknown,
	}
	both := &routedParticipant{errs: byID, scriptedParticipant: &scriptedParticipant{}}
	uc := newUseCase(both)

	for _, id := range []domain.InstructionID{idThrottled, idTimeout} {
		if err := uc.Handle(context.Background(), acceptedMsg(id)); err != nil {
			t.Fatalf("Handle(%s): %v", id, err)
		}
	}

	if got := load(t, idThrottled).Status(); got != domain.StatusAccepted {
		t.Errorf("429: status = %s, esperado accepted (reenviar é seguro)", got)
	}
	if got := load(t, idTimeout).Status(); got != domain.StatusUnknown {
		t.Errorf("timeout: status = %s, esperado unknown (só consultar é seguro)", got)
	}
	if n, _, _ := tasks(t, idThrottled); n != 0 {
		t.Errorf("429: tarefas = %d, esperado 0", n)
	}
	if n, _, _ := tasks(t, idTimeout); n != 1 {
		t.Errorf("timeout: tarefas = %d, esperado 1", n)
	}

	// Repetir a mensagem original: nenhum dos dois é reenviado por ela (a versão já avançou).
	for _, id := range []domain.InstructionID{idThrottled, idTimeout} {
		if err := uc.Handle(context.Background(), acceptedMsg(id)); err != nil {
			t.Fatal(err)
		}
		if both.sent(id) != 1 {
			t.Errorf("%s: envios = %d, esperado 1", id, both.sent(id))
		}
	}
}

// routedParticipant devolve um erro fixo por instrução.
type routedParticipant struct {
	*scriptedParticipant
	errs map[string]error
}

func (r *routedParticipant) Pay(_ context.Context, req app.PaymentRequest) (app.PaymentResult, error) {
	r.mu.Lock()
	if r.sends == nil {
		r.sends = make(map[string]int)
	}
	r.sends[req.InstructionID]++
	r.mu.Unlock()
	return app.PaymentResult{}, r.errs[req.InstructionID]
}
