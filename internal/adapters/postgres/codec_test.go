// codec_test.go
// Testes do codec de eventos, sem banco e sem Docker.
// Objetivo: provar que todo evento sobrevive ao ciclo encode -> decode idêntico e que um
// tipo desconhecido ou um payload quebrado falha em vez de virar um evento vazio. Um erro
// aqui corromperia silenciosamente o histórico de uma instrução.

package postgres

import (
	"testing"

	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

func TestCodecIdaEVolta(t *testing.T) {
	tests := []struct {
		name  string
		event domain.Event
	}{
		{"instruction_accepted", domain.InstructionAccepted{ID: "id-1", Amount: 15000, Debtor: "d", Creditor: "c", EndToEndID: "e"}},
		{"routing_attempted", domain.RoutingAttempted{}},
		{"participant_accepted", domain.ParticipantAccepted{}},
		{"participant_rejected", domain.ParticipantRejected{Code: "AM04"}},
		{"routing_timed_out", domain.RoutingTimedOut{}},
		{"status_queried", domain.StatusQueried{Result: domain.QueryAccepted}},
		{"routing_throttled", domain.RoutingThrottled{}},
		{"reconciliation_exhausted", domain.ReconciliationExhausted{Attempts: 5}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := encode(tc.event)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			got, err := decode(string(tc.event.Type()), payload)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got != tc.event {
				t.Errorf("decode(encode(e)) = %#v, esperado %#v", got, tc.event)
			}
		})
	}
}

func TestCodecCobreTodosOsOitoEventos(t *testing.T) {
	// Se um evento novo for criado no domínio e esquecido no codec, este teste lembra.
	all := []domain.EventType{
		domain.EventInstructionAccepted, domain.EventRoutingAttempted,
		domain.EventParticipantAccepted, domain.EventParticipantRejected,
		domain.EventRoutingTimedOut, domain.EventStatusQueried,
		domain.EventRoutingThrottled, domain.EventReconciliationExhausted,
	}
	for _, et := range all {
		if _, err := decode(string(et), []byte(`{}`)); err != nil {
			t.Errorf("decode não reconhece %s: %v", et, err)
		}
	}
}

func TestDecodeRejeitaEntradaInvalida(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		payload   string
	}{
		{"tipo_desconhecido_e_erro", "Inventado", `{}`},
		{"payload_quebrado_e_erro", "InstructionAccepted", `{nao-e-json`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decode(tc.eventType, []byte(tc.payload)); err == nil {
				t.Fatal("esperava erro")
			}
		})
	}
}

type eventoDesconhecido struct{}

func (eventoDesconhecido) Type() domain.EventType { return "Desconhecido" }

func TestEncodeRejeitaEventoDesconhecido(t *testing.T) {
	if _, err := encode(eventoDesconhecido{}); err == nil {
		t.Fatal("esperava erro ao codificar evento fora do domínio")
	}
}
