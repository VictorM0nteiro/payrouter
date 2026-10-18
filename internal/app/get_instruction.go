// get_instruction.go
// Caso de uso "consultar instrução": reconstrói o agregado a partir do event store.
// Objetivo: dar a leitura AUTORITATIVA de uma instrução. O estado devolvido é sempre o que o
// histórico de eventos diz agora, nunca uma projeção atrasada. Isso dissolve o
// read-your-writes: quem acabou de criar uma instrução a vê imediatamente. Para um único
// agregado a reconstrução é barata; já a listagem lê a projeção (eventual), em outra fase.

package app

import (
	"context"
	"fmt"

	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

// InstructionView é o estado de uma instrução, pronto para a borda serializar.
type InstructionView struct {
	ID          domain.InstructionID
	Status      domain.Status
	Version     int
	AmountCents int64
	Debtor      string
	Creditor    string
	EndToEndID  string
	Attempts    int
}

// GetInstruction é o caso de uso.
type GetInstruction struct {
	events EventStore
}

func NewGetInstruction(events EventStore) *GetInstruction {
	return &GetInstruction{events: events}
}

// Execute devolve o estado atual, ou ErrNotFound se a instrução não existe.
func (uc *GetInstruction) Execute(ctx context.Context, id domain.InstructionID) (InstructionView, error) {
	events, err := uc.events.Load(ctx, id)
	if err != nil {
		return InstructionView{}, err
	}
	p, err := domain.Rehydrate(events)
	if err != nil {
		// Histórico que o domínio recusa é corrupção do event store, não erro do cliente.
		return InstructionView{}, fmt.Errorf("histórico corrompido da instrução %s: %w", id, err)
	}
	return InstructionView{
		ID:          p.ID(),
		Status:      p.Status(),
		Version:     p.Version(),
		AmountCents: p.Amount().Cents(),
		Debtor:      p.Debtor(),
		Creditor:    p.Creditor(),
		EndToEndID:  p.EndToEndID(),
		Attempts:    p.Attempts(),
	}, nil
}
