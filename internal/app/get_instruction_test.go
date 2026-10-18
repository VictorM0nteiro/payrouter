// get_instruction_test.go
// Testes do caso de uso GetInstruction com um event store falso, sem banco e sem Docker.
// Objetivo: provar que o estado devolvido vem da reconstrução do histórico, que uma
// instrução inexistente vira ErrNotFound e que um histórico que o domínio recusa não passa
// por estado válido.

package app

import (
	"context"
	"errors"
	"testing"

	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

// fakeEvents é um EventStore que devolve sempre o mesmo histórico (ou erro) no Load.
type fakeEvents struct {
	events []domain.Event
	err    error
}

func (f fakeEvents) Append(context.Context, domain.InstructionID, int, ...domain.Event) error {
	return errors.New("não usado neste teste")
}

func (f fakeEvents) Load(context.Context, domain.InstructionID) ([]domain.Event, error) {
	return f.events, f.err
}

func TestGetInstructionReconstroiOEstadoDoHistorico(t *testing.T) {
	uc := NewGetInstruction(fakeEvents{events: []domain.Event{
		domain.InstructionAccepted{ID: "id-1", Amount: 15000, Debtor: "d", Creditor: "c", EndToEndID: "e2e"},
		domain.RoutingAttempted{},
		domain.RoutingTimedOut{},
	}})

	view, err := uc.Execute(context.Background(), "id-1")
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	want := InstructionView{
		ID: "id-1", Status: domain.StatusUnknown, Version: 3, AmountCents: 15000,
		Debtor: "d", Creditor: "c", EndToEndID: "e2e", Attempts: 1,
	}
	if view != want {
		t.Errorf("view = %+v, esperada %+v", view, want)
	}
}

func TestGetInstructionInexistenteEhNotFound(t *testing.T) {
	uc := NewGetInstruction(fakeEvents{err: ErrNotFound})

	if _, err := uc.Execute(context.Background(), "id-x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("erro = %v, esperado ErrNotFound", err)
	}
}

func TestGetInstructionHistoricoCorrompidoEErroInterno(t *testing.T) {
	// Começa por RoutingAttempted: o domínio recusa. Não pode virar 404 nem estado válido.
	uc := NewGetInstruction(fakeEvents{events: []domain.Event{domain.RoutingAttempted{}}})

	_, err := uc.Execute(context.Background(), "id-1")
	var illegal domain.ErrIllegalTransition
	if !errors.As(err, &illegal) {
		t.Fatalf("erro = %v, esperado envolver ErrIllegalTransition", err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Error("histórico corrompido não pode ser confundido com instrução inexistente")
	}
}

func TestGetInstructionPropagaErroDoStore(t *testing.T) {
	boom := errors.New("banco fora do ar")
	uc := NewGetInstruction(fakeEvents{err: boom})

	if _, err := uc.Execute(context.Background(), "id-1"); !errors.Is(err, boom) {
		t.Fatalf("erro = %v, esperado o do store", err)
	}
}
