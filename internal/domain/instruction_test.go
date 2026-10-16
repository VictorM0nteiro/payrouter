// instruction_test.go
// Testes da máquina de estados do agregado PaymentInstruction, sem banco e sem Docker.
// Objetivo: provar a matriz completa estado x comando (as transições legais e todas as
// ilegais), a reconstrução por Rehydrate e as validações de entrada. A matriz esperada é
// escrita à mão aqui, independente da tabela de produção, para um erro nela não passar
// despercebido.

package domain

import (
	"errors"
	"testing"
)

func mustMoney(t *testing.T, cents int64) Money {
	t.Helper()
	m, err := NewMoney(cents)
	if err != nil {
		t.Fatalf("NewMoney(%d): %v", cents, err)
	}
	return m
}

// newAccepted devolve uma instrução recém-aceita (versão 1).
func newAccepted(t *testing.T) *PaymentInstruction {
	t.Helper()
	p, _, err := Accept("id-1", mustMoney(t, 15000), "debtor", "creditor", "e2e-1")
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	return p
}

// command é uma operação do agregado que pode emitir evento.
type command func(p *PaymentInstruction) (Event, error)

var commands = map[string]command{
	"Route":             func(p *PaymentInstruction) (Event, error) { return p.Route() },
	"Settle":            func(p *PaymentInstruction) (Event, error) { return p.Settle() },
	"Reject":            func(p *PaymentInstruction) (Event, error) { return p.Reject("AM04") },
	"RecordTimeout":     func(p *PaymentInstruction) (Event, error) { return p.RecordTimeout() },
	"RecordStatusQuery": func(p *PaymentInstruction) (Event, error) { return p.RecordStatusQuery(QueryUnknown) },
	"Throttle":          func(p *PaymentInstruction) (Event, error) { return p.Throttle() },
	"Exhaust":           func(p *PaymentInstruction) (Event, error) { return p.Exhaust(5) },
}

// reach leva uma instrução nova até o estado pedido pelo caminho mais curto.
func reach(t *testing.T, target Status) *PaymentInstruction {
	t.Helper()
	p := newAccepted(t)
	steps := map[Status][]string{
		StatusAccepted: {},
		StatusInFlight: {"Route"},
		StatusUnknown:  {"Route", "RecordTimeout"},
		StatusSettled:  {"Route", "Settle"},
		StatusRejected: {"Route", "Reject"},
		StatusFailed:   {"Route", "RecordTimeout", "Exhaust"},
	}[target]
	for _, name := range steps {
		if _, err := commands[name](p); err != nil {
			t.Fatalf("preparando %s, comando %s: %v", target, name, err)
		}
	}
	if p.Status() != target {
		t.Fatalf("esperava estado %s, obteve %s", target, p.Status())
	}
	return p
}

func TestMatrizDeTransicoes(t *testing.T) {
	// Para cada estado, os comandos legais e o estado resultante. Todo o resto é ilegal.
	legal := map[Status]map[string]Status{
		StatusAccepted: {"Route": StatusInFlight},
		StatusInFlight: {
			"Settle":        StatusSettled,
			"Reject":        StatusRejected,
			"RecordTimeout": StatusUnknown,
			"Throttle":      StatusAccepted,
		},
		StatusUnknown: {
			"RecordStatusQuery": StatusUnknown,
			"Settle":            StatusSettled,
			"Reject":            StatusRejected,
			"Exhaust":           StatusFailed,
		},
		StatusSettled:  {},
		StatusRejected: {},
		StatusFailed:   {},
	}

	for state, allowed := range legal {
		for name, cmd := range commands {
			t.Run(string(state)+"/"+name, func(t *testing.T) {
				p := reach(t, state)
				versionBefore := p.Version()

				ev, err := cmd(p)

				if want, ok := allowed[name]; ok {
					if err != nil {
						t.Fatalf("esperava transição legal, obteve erro: %v", err)
					}
					if ev == nil {
						t.Fatal("transição legal deve devolver o evento")
					}
					if p.Status() != want {
						t.Errorf("estado = %s, esperado %s", p.Status(), want)
					}
					if p.Version() != versionBefore+1 {
						t.Errorf("versão = %d, esperada %d", p.Version(), versionBefore+1)
					}
					return
				}

				var illegal ErrIllegalTransition
				if !errors.As(err, &illegal) {
					t.Fatalf("esperava ErrIllegalTransition, obteve %v", err)
				}
				if illegal.From != state {
					t.Errorf("From = %s, esperado %s", illegal.From, state)
				}
				if p.Status() != state || p.Version() != versionBefore {
					t.Errorf("transição ilegal alterou o agregado: estado %s, versão %d", p.Status(), p.Version())
				}
			})
		}
	}
}

func TestEstadosTerminaisNaoAdmitemNada(t *testing.T) {
	for _, s := range []Status{StatusSettled, StatusRejected, StatusFailed} {
		if !s.IsTerminal() {
			t.Errorf("%s deveria ser terminal", s)
		}
	}
	for _, s := range []Status{StatusNew, StatusAccepted, StatusInFlight, StatusUnknown} {
		if s.IsTerminal() {
			t.Errorf("%s não deveria ser terminal", s)
		}
	}
}

func TestRehydrate(t *testing.T) {
	accepted := InstructionAccepted{ID: "id-1", Amount: 15000, Debtor: "d", Creditor: "c", EndToEndID: "e"}

	tests := []struct {
		name         string
		events       []Event
		wantErr      error // errors.Is
		wantIllegal  bool  // errors.As ErrIllegalTransition
		wantStatus   Status
		wantVersion  int
		wantAttempts int
	}{
		{
			name:    "historico_vazio_e_erro",
			events:  nil,
			wantErr: ErrEmptyHistory,
		},
		{
			name:        "primeiro_evento_diferente_de_accepted_e_ilegal",
			events:      []Event{RoutingAttempted{}},
			wantIllegal: true,
		},
		{
			name: "caso_cruel_timeout_consulta_e_resolve",
			events: []Event{
				accepted,
				RoutingAttempted{},
				RoutingTimedOut{},
				StatusQueried{Result: QueryUnknown},
				StatusQueried{Result: QueryAccepted},
				ParticipantAccepted{},
			},
			wantStatus:   StatusSettled,
			wantVersion:  6,
			wantAttempts: 1,
		},
		{
			name: "throttle_volta_para_accepted_e_conta_duas_tentativas",
			events: []Event{
				accepted,
				RoutingAttempted{},
				RoutingThrottled{},
				RoutingAttempted{},
			},
			wantStatus:   StatusInFlight,
			wantVersion:  4,
			wantAttempts: 2,
		},
		{
			name: "evento_depois_de_terminal_e_ilegal",
			events: []Event{
				accepted,
				RoutingAttempted{},
				ParticipantAccepted{},
				RoutingTimedOut{},
			},
			wantIllegal: true,
		},
		{
			name: "conciliacao_esgotada_termina_em_failed",
			events: []Event{
				accepted,
				RoutingAttempted{},
				RoutingTimedOut{},
				ReconciliationExhausted{Attempts: 5},
			},
			wantStatus:   StatusFailed,
			wantVersion:  4,
			wantAttempts: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Rehydrate(tc.events)

			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("erro = %v, esperado %v", err, tc.wantErr)
				}
				return
			}
			if tc.wantIllegal {
				var illegal ErrIllegalTransition
				if !errors.As(err, &illegal) {
					t.Fatalf("esperava ErrIllegalTransition, obteve %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("erro inesperado: %v", err)
			}
			if p.Status() != tc.wantStatus {
				t.Errorf("estado = %s, esperado %s", p.Status(), tc.wantStatus)
			}
			if p.Version() != tc.wantVersion {
				t.Errorf("versão = %d, esperada %d", p.Version(), tc.wantVersion)
			}
			if p.Attempts() != tc.wantAttempts {
				t.Errorf("tentativas = %d, esperadas %d", p.Attempts(), tc.wantAttempts)
			}
			if p.ID() != "id-1" || p.Amount().Cents() != 15000 {
				t.Errorf("dados do accepted não foram aplicados: id=%s valor=%d", p.ID(), p.Amount().Cents())
			}
		})
	}
}

func TestRehydrateReproduzOEstadoDosComandos(t *testing.T) {
	// O que os comandos produzem, aplicado de novo por Rehydrate, dá o mesmo agregado.
	p := newAccepted(t)
	var events []Event
	events = append(events, InstructionAccepted{
		ID: "id-1", Amount: 15000, Debtor: "debtor", Creditor: "creditor", EndToEndID: "e2e-1",
	})
	for _, name := range []string{"Route", "RecordTimeout", "RecordStatusQuery", "Settle"} {
		ev, err := commands[name](p)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		events = append(events, ev)
	}

	got, err := Rehydrate(events)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	if got.Status() != p.Status() || got.Version() != p.Version() || got.Attempts() != p.Attempts() {
		t.Errorf("rehydrate divergiu: %s/v%d/%d contra %s/v%d/%d",
			got.Status(), got.Version(), got.Attempts(), p.Status(), p.Version(), p.Attempts())
	}
}

func TestAccept(t *testing.T) {
	tests := []struct {
		name     string
		id       InstructionID
		amount   Money
		debtor   string
		creditor string
		e2e      string
		wantErr  error
	}{
		{"valido", "id", 100, "d", "c", "e", nil},
		{"valor_zero_e_invalido", "id", 0, "d", "c", "e", ErrInvalidAmount},
		{"valor_negativo_e_invalido", "id", -1, "d", "c", "e", ErrInvalidAmount},
		{"id_vazio_e_invalido", "", 100, "d", "c", "e", ErrInvalidParties},
		{"devedor_vazio_e_invalido", "id", 100, "", "c", "e", ErrInvalidParties},
		{"credor_vazio_e_invalido", "id", 100, "d", "", "e", ErrInvalidParties},
		{"end_to_end_vazio_e_invalido", "id", 100, "d", "c", "", ErrInvalidParties},
		{"devedor_igual_ao_credor_e_invalido", "id", 100, "x", "x", "e", ErrInvalidParties},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, ev, err := Accept(tc.id, tc.amount, tc.debtor, tc.creditor, tc.e2e)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("erro = %v, esperado %v", err, tc.wantErr)
			}
			if tc.wantErr != nil {
				if p != nil || ev != nil {
					t.Error("em erro, agregado e evento devem ser nulos")
				}
				return
			}
			if p.Status() != StatusAccepted || p.Version() != 1 {
				t.Errorf("estado/versão = %s/%d, esperado accepted/1", p.Status(), p.Version())
			}
			if ev.Type() != EventInstructionAccepted {
				t.Errorf("evento = %s", ev.Type())
			}
		})
	}
}

func TestValidacoesDeComando(t *testing.T) {
	t.Run("reject_sem_codigo_e_invalido", func(t *testing.T) {
		p := reach(t, StatusInFlight)
		if _, err := p.Reject(""); !errors.Is(err, ErrInvalidRejectionCode) {
			t.Fatalf("erro = %v", err)
		}
		if p.Status() != StatusInFlight {
			t.Error("comando inválido não pode mudar o estado")
		}
	})
	t.Run("consulta_com_resultado_invalido_e_erro", func(t *testing.T) {
		p := reach(t, StatusUnknown)
		if _, err := p.RecordStatusQuery("talvez"); !errors.Is(err, ErrInvalidQueryResult) {
			t.Fatalf("erro = %v", err)
		}
	})
}

func TestNewMoney(t *testing.T) {
	tests := []struct {
		name    string
		cents   int64
		wantErr error
	}{
		{"positivo_e_valido", 1, nil},
		{"zero_e_invalido", 0, ErrInvalidAmount},
		{"negativo_e_invalido", -100, ErrInvalidAmount},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, err := NewMoney(tc.cents)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("erro = %v, esperado %v", err, tc.wantErr)
			}
			if err == nil && m.Cents() != tc.cents {
				t.Errorf("Cents() = %d, esperado %d", m.Cents(), tc.cents)
			}
		})
	}
}
