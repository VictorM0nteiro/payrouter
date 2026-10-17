// accept_instruction_test.go
// Testes do caso de uso AcceptInstruction que não precisam de banco.
// Objetivo: provar o cálculo do fingerprint (estável para o mesmo conteúdo, diferente quando
// qualquer campo muda) e a recusa de requisição sem Idempotency-Key. Os comportamentos que
// dependem de transação (repetição, conflito, atomicidade) são testados contra o Postgres
// real, em internal/adapters/postgres.

package app

import (
	"context"
	"errors"
	"testing"
)

func baseInput() AcceptInstructionInput {
	return AcceptInstructionInput{
		Scope: "cliente-1", IdempotencyKey: "k1",
		Debtor: "d", Creditor: "c", AmountCents: 15000, EndToEndID: "e2e",
	}
}

func TestFingerprint(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(in *AcceptInstructionInput)
		same   bool
	}{
		{"mesmo_conteudo_gera_o_mesmo_fingerprint", func(in *AcceptInstructionInput) {}, true},
		{"escopo_e_chave_nao_entram_no_fingerprint", func(in *AcceptInstructionInput) { in.Scope = "outro"; in.IdempotencyKey = "k2" }, true},
		{"devedor_diferente_muda", func(in *AcceptInstructionInput) { in.Debtor = "x" }, false},
		{"credor_diferente_muda", func(in *AcceptInstructionInput) { in.Creditor = "x" }, false},
		{"valor_diferente_muda", func(in *AcceptInstructionInput) { in.AmountCents = 15001 }, false},
		{"end_to_end_diferente_muda", func(in *AcceptInstructionInput) { in.EndToEndID = "x" }, false},
	}
	base := fingerprint(baseInput())
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := baseInput()
			tc.mutate(&in)
			if got := fingerprint(in) == base; got != tc.same {
				t.Errorf("fingerprint igual ao base = %v, esperado %v", got, tc.same)
			}
		})
	}
}

func TestFingerprintNaoConfundeCamposAdjacentes(t *testing.T) {
	// "ab"+"c" e "a"+"bc" não podem colidir: o formato usa aspas e separadores.
	a := AcceptInstructionInput{Debtor: "ab", Creditor: "c", AmountCents: 1, EndToEndID: "e"}
	b := AcceptInstructionInput{Debtor: "a", Creditor: "bc", AmountCents: 1, EndToEndID: "e"}
	if fingerprint(a) == fingerprint(b) {
		t.Error("campos adjacentes colidiram no fingerprint")
	}
}

func TestExecuteSemIdempotencyKeyEErro(t *testing.T) {
	// Falha antes de tocar na UnitOfWork, então ela pode ser nula.
	uc := NewAcceptInstruction(nil, nil)
	in := baseInput()
	in.IdempotencyKey = ""

	_, err := uc.Execute(context.Background(), in)
	if !errors.Is(err, ErrIdempotencyKeyRequired) {
		t.Fatalf("erro = %v, esperado ErrIdempotencyKeyRequired", err)
	}
}
