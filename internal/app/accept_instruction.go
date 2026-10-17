// accept_instruction.go
// Caso de uso "aceitar instrução": valida, cria o agregado e grava evento + chave de
// idempotência numa única transação.
// Objetivo: tornar o POST /instructions repetível com segurança. A mesma Idempotency-Key com
// o mesmo corpo devolve a resposta original sem criar nada; com corpo diferente é um erro;
// uma entrada inválida nunca consome a chave. Sem HTTP aqui: o caso de uso recebe dados
// já extraídos e devolve um resultado, e quem traduz para status é o adapter.
package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

// acceptEndpoint nomeia a operação dentro da chave de idempotência.
const acceptEndpoint = "POST /instructions"

var (
	ErrIdempotencyKeyRequired = errors.New("Idempotency-Key é obrigatória")
	ErrIdempotencyKeyReuse    = errors.New("Idempotency-Key já foi usada com outro corpo")
)

// AcceptInstructionInput são os dados do pedido, já extraídos da borda.
type AcceptInstructionInput struct {
	Scope          string // quem pede (id da API key)
	IdempotencyKey string
	Debtor         string
	Creditor       string
	AmountCents    int64
	EndToEndID     string
}

// AcceptInstructionResult é o que o chamador precisa devolver ao cliente.
type AcceptInstructionResult struct {
	ID       domain.InstructionID
	Status   domain.Status
	Replayed bool // true quando é a repetição de uma requisição já atendida
}

// acceptedResponse é a resposta guardada na tabela de idempotência.
type acceptedResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// AcceptInstruction é o caso de uso.
type AcceptInstruction struct {
	uow   UnitOfWork
	newID func() domain.InstructionID
}

// NewAcceptInstruction recebe o gerador de id por parâmetro, para o teste ser determinístico.
func NewAcceptInstruction(uow UnitOfWork, newID func() domain.InstructionID) *AcceptInstruction {
	return &AcceptInstruction{uow: uow, newID: newID}
}

// Execute aceita a instrução ou repete a resposta de uma requisição idêntica anterior.
func (uc *AcceptInstruction) Execute(ctx context.Context, in AcceptInstructionInput) (AcceptInstructionResult, error) {
	if in.IdempotencyKey == "" {
		return AcceptInstructionResult{}, ErrIdempotencyKeyRequired
	}
	key := IdempotencyKey{Scope: in.Scope, Endpoint: acceptEndpoint, Key: in.IdempotencyKey}
	fp := fingerprint(in)

	var result AcceptInstructionResult
	err := uc.uow.Do(ctx, func(ctx context.Context, tx Tx) error {
		record, claimed, err := tx.Idempotency().Claim(ctx, key, fp)
		if err != nil {
			return err
		}
		if !claimed {
			// Chave já usada: mesmo corpo repete a resposta, corpo diferente é conflito.
			if record.Fingerprint != fp {
				return ErrIdempotencyKeyReuse
			}
			var stored acceptedResponse
			if err := json.Unmarshal(record.ResponseBody, &stored); err != nil {
				return fmt.Errorf("resposta guardada da chave está ilegível: %w", err)
			}
			result = AcceptInstructionResult{
				ID:       domain.InstructionID(stored.ID),
				Status:   domain.Status(stored.Status),
				Replayed: true,
			}
			return nil
		}

		amount, err := domain.NewMoney(in.AmountCents)
		if err != nil {
			return err
		}
		p, event, err := domain.Accept(uc.newID(), amount, in.Debtor, in.Creditor, in.EndToEndID)
		if err != nil {
			return err
		}
		if err := tx.Events().Append(ctx, p.ID(), 0, event); err != nil {
			return err
		}

		body, err := json.Marshal(acceptedResponse{ID: string(p.ID()), Status: string(p.Status())})
		if err != nil {
			return err
		}
		if err := tx.Idempotency().Complete(ctx, key, p.ID(), body); err != nil {
			return err
		}
		result = AcceptInstructionResult{ID: p.ID(), Status: p.Status()}
		return nil
	})
	if err != nil {
		return AcceptInstructionResult{}, err
	}
	return result, nil
}

// fingerprint resume o conteúdo do pedido num SHA-256. É calculado sobre os campos já
// extraídos, numa ordem e formato fixos, e não sobre o corpo bruto: espaços, ordem das
// chaves e outras variações do JSON não mudam o resultado, só o conteúdo muda.
func fingerprint(in AcceptInstructionInput) string {
	canonical := fmt.Sprintf("debtor=%q;creditor=%q;amount_cents=%d;end_to_end_id=%q",
		in.Debtor, in.Creditor, in.AmountCents, in.EndToEndID)
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}
