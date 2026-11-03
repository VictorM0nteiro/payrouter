// projection_test.go
// Testes da projeção, sem banco e sem Docker.
// Objetivo: provar a sessão 21. Cada evento vira o Patch certo só pelo que ele carrega; o que não é
// um evento coerente é recusado como inválido; e, o mais importante, a tabela evento -> status da
// projeção (que duplica a do domínio de propósito) dá o MESMO status que domain.Rehydrate em todo
// prefixo de todos os caminhos da máquina de estados: se alguém mudar uma tabela e esquecer a
// outra, este teste quebra. Também cobre o caso de uso ProjectEvent contra uma store falsa.

package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

var t0 = time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)

// raw monta o registro do tópico como o event store o grava: envelope com o payload em data.
func raw(version int, eventType string, data string) []byte {
	return []byte(fmt.Sprintf(`{"aggregate_id":"i-1","version":%d,"event_type":%q,"occurred_at":%q,"data":%s}`,
		version, eventType, t0.Format(time.RFC3339Nano), data))
}

// dataOf devolve o JSON de data de um evento, com os mesmos nomes que o codec do Postgres grava.
func dataOf(t *testing.T, e domain.Event) string {
	t.Helper()
	var v any
	switch ev := e.(type) {
	case domain.InstructionAccepted:
		v = map[string]any{"id": string(ev.ID), "amount_cents": ev.Amount.Cents(), "debtor": ev.Debtor, "creditor": ev.Creditor, "end_to_end_id": ev.EndToEndID}
	case domain.ParticipantRejected:
		v = map[string]any{"code": ev.Code}
	case domain.StatusQueried:
		v = map[string]any{"result": string(ev.Result)}
	case domain.ReconciliationExhausted:
		v = map[string]any{"attempts": ev.Attempts}
	default:
		v = map[string]any{}
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestPatchDeCadaEvento(t *testing.T) {
	cases := []struct {
		name      string
		version   int
		eventType string
		data      string
		status    domain.Status
		attempt   bool
		code      string
	}{
		{"accepted", 1, "InstructionAccepted", `{"id":"i-1","amount_cents":15000,"debtor":"a","creditor":"b","end_to_end_id":"e1"}`, domain.StatusAccepted, false, ""},
		{"attempted", 2, "RoutingAttempted", `{}`, domain.StatusInFlight, true, ""},
		{"participant accepted", 3, "ParticipantAccepted", `{}`, domain.StatusSettled, false, ""},
		{"participant rejected", 3, "ParticipantRejected", `{"code":"AC04"}`, domain.StatusRejected, false, "AC04"},
		{"timed out", 3, "RoutingTimedOut", `{}`, domain.StatusUnknown, false, ""},
		{"status queried não muda o status", 4, "StatusQueried", `{"result":"unknown"}`, "", false, ""},
		{"throttled volta a accepted", 3, "RoutingThrottled", `{}`, domain.StatusAccepted, false, ""},
		{"exhausted", 5, "ReconciliationExhausted", `{"attempts":10}`, domain.StatusFailed, false, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := PatchFor(raw(c.version, c.eventType, c.data))
			if err != nil {
				t.Fatal(err)
			}
			if p.AggregateID != "i-1" || p.Version != c.version || !p.OccurredAt.Equal(t0) {
				t.Errorf("identidade do patch = %s/%d em %v", p.AggregateID, p.Version, p.OccurredAt)
			}
			if p.Status != c.status || p.AddAttempt != c.attempt || p.RejectCode != c.code {
				t.Errorf("patch = status %q attempt %v code %q, esperado %q %v %q", p.Status, p.AddAttempt, p.RejectCode, c.status, c.attempt, c.code)
			}
		})
	}
}

func TestPatchDaInstrucaoGuardaOsCentavosSemFloat(t *testing.T) {
	p, err := PatchFor(raw(1, "InstructionAccepted", `{"id":"i-1","amount_cents":9007199254740993,"debtor":"a","creditor":"b","end_to_end_id":"e1"}`))
	if err != nil {
		t.Fatal(err)
	}
	// 2^53 + 1: um float64 arredondaria este valor.
	if p.Instruction == nil || p.Instruction.AmountCents != 9007199254740993 {
		t.Errorf("instrução = %+v, esperado 9007199254740993 centavos exatos", p.Instruction)
	}
}

func TestPatchRecusaOQueNaoEUmEventoCoerente(t *testing.T) {
	cases := map[string][]byte{
		"JSON quebrado":               []byte(`{`),
		"sem aggregate_id":            []byte(`{"version":1,"event_type":"InstructionAccepted","data":{}}`),
		"versão zero":                 raw(0, "InstructionAccepted", `{}`),
		"tipo desconhecido":           raw(2, "Teletransportado", `{}`),
		"versão 1 que não é accepted": raw(1, "RoutingAttempted", `{}`),
		"accepted depois da versão 1": raw(2, "InstructionAccepted", `{"id":"i-1","amount_cents":1}`),
		"dados do accepted quebrados": raw(1, "InstructionAccepted", `"texto"`),
		"dados do rejected quebrados": raw(3, "ParticipantRejected", `[1]`),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := PatchFor(payload); !errors.Is(err, ErrInvalidEvent) {
				t.Errorf("erro = %v, esperado ErrInvalidEvent", err)
			}
		})
	}
}

// model é o documento mínimo que a projeção mantém, aplicando Patches em ordem.
type model struct {
	status   domain.Status
	attempts int
}

func (m *model) apply(p Patch) {
	if p.Status != "" {
		m.status = p.Status
	}
	if p.AddAttempt {
		m.attempts++
	}
}

// history monta, pelos comandos do domínio, o histórico de uma instrução.
func history(t *testing.T, steps ...func(*domain.PaymentInstruction) (domain.Event, error)) []domain.Event {
	t.Helper()
	amount, err := domain.NewMoney(15000)
	if err != nil {
		t.Fatal(err)
	}
	p, first, err := domain.Accept("i-1", amount, "a", "b", "e1")
	if err != nil {
		t.Fatal(err)
	}
	events := []domain.Event{first}
	for i, step := range steps {
		e, err := step(p)
		if err != nil {
			t.Fatalf("passo %d: %v", i+1, err)
		}
		events = append(events, e)
	}
	return events
}

func route(p *domain.PaymentInstruction) (domain.Event, error)   { return p.Route() }
func settle(p *domain.PaymentInstruction) (domain.Event, error)  { return p.Settle() }
func timeout(p *domain.PaymentInstruction) (domain.Event, error) { return p.RecordTimeout() }
func throttle(p *domain.PaymentInstruction) (domain.Event, error) {
	return p.Throttle()
}
func reject(p *domain.PaymentInstruction) (domain.Event, error) { return p.Reject("AC04") }
func exhaust(p *domain.PaymentInstruction) (domain.Event, error) {
	return p.Exhaust(10)
}
func query(r domain.QueryResult) func(*domain.PaymentInstruction) (domain.Event, error) {
	return func(p *domain.PaymentInstruction) (domain.Event, error) { return p.RecordStatusQuery(r) }
}

func TestProjecaoDaOMesmoStatusQueODominioEmTodoPrefixoDeTodoCaminho(t *testing.T) {
	paths := map[string][]domain.Event{
		"aceita e liquida":                 history(t, route, settle),
		"aceita e rejeita":                 history(t, route, reject),
		"429 e reenvio":                    history(t, route, throttle, route, settle),
		"timeout resolvido para settled":   history(t, route, timeout, query(domain.QueryUnknown), settle),
		"timeout resolvido para rejected":  history(t, route, timeout, query(domain.QueryRejected), reject),
		"timeout que esgota a conciliação": history(t, route, timeout, query(domain.QueryUnknown), query(domain.QueryUnknown), exhaust),
		"429 duas vezes e depois timeout":  history(t, route, throttle, route, throttle, route, timeout, settle),
	}
	for name, events := range paths {
		t.Run(name, func(t *testing.T) {
			var m model
			for i, e := range events {
				p, err := PatchFor(raw(i+1, string(e.Type()), dataOf(t, e)))
				if err != nil {
					t.Fatalf("evento %d (%s): %v", i+1, e.Type(), err)
				}
				m.apply(p)

				want, err := domain.Rehydrate(events[:i+1])
				if err != nil {
					t.Fatal(err)
				}
				if m.status != want.Status() || m.attempts != want.Attempts() {
					t.Fatalf("depois do evento %d (%s): projeção %s/%d tentativas, domínio %s/%d",
						i+1, e.Type(), m.status, m.attempts, want.Status(), want.Attempts())
				}
			}
		})
	}
}

// fakeProjection devolve o que o teste mandar e guarda os patches recebidos.
type fakeProjection struct {
	outcome ProjectionOutcome
	err     error
	got     []Patch
}

func (f *fakeProjection) Apply(_ context.Context, p Patch) (ProjectionOutcome, error) {
	f.got = append(f.got, p)
	return f.outcome, f.err
}

func msg(version int, eventType, data string) Message {
	return Message{AggregateID: "i-1", EventType: eventType, Version: version, Payload: raw(version, eventType, data)}
}

func TestProjectEventAplicadoEDuplicadoAvancamOOffset(t *testing.T) {
	for _, outcome := range []ProjectionOutcome{ProjectionApplied, ProjectionDuplicate} {
		store := &fakeProjection{outcome: outcome}
		err := NewProjectEvent(store).Handle(context.Background(), msg(2, "RoutingAttempted", `{}`))
		if err != nil {
			t.Errorf("resultado %d: erro = %v, esperado nil", outcome, err)
		}
		if len(store.got) != 1 {
			t.Errorf("resultado %d: a store recebeu %d patches, esperado 1", outcome, len(store.got))
		}
	}
}

func TestProjectEventComBuracoDevolveErroParaOConsumidorRepetir(t *testing.T) {
	store := &fakeProjection{outcome: ProjectionGap}

	err := NewProjectEvent(store).Handle(context.Background(), msg(3, "ParticipantAccepted", `{}`))

	if !errors.Is(err, ErrGap) {
		t.Errorf("erro = %v, esperado ErrGap: a mensagem tem de ser repetida, não descartada", err)
	}
}

func TestProjectEventErroDaStoreEhTransitorioENaoViraGap(t *testing.T) {
	boom := errors.New("mongo fora do ar")
	store := &fakeProjection{err: boom}

	err := NewProjectEvent(store).Handle(context.Background(), msg(2, "RoutingAttempted", `{}`))

	if !errors.Is(err, boom) || errors.Is(err, ErrGap) {
		t.Errorf("erro = %v, esperado o erro da store, sem ser ErrGap", err)
	}
}

func TestProjectEventDescartaORegistroInvalidoSemTocarNaStore(t *testing.T) {
	cases := map[string]Message{
		"corpo quebrado":             {AggregateID: "i-1", EventType: "RoutingAttempted", Version: 2, Payload: []byte(`{`)},
		"chave diferente do corpo":   {AggregateID: "outra", EventType: "RoutingAttempted", Version: 2, Payload: raw(2, "RoutingAttempted", `{}`)},
		"versão diferente do header": {AggregateID: "i-1", EventType: "RoutingAttempted", Version: 9, Payload: raw(2, "RoutingAttempted", `{}`)},
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			store := &fakeProjection{outcome: ProjectionApplied}

			err := NewProjectEvent(store).Handle(context.Background(), m)

			if err != nil {
				t.Errorf("erro = %v, esperado nil: veneno não pode travar a marca d'água", err)
			}
			if len(store.got) != 0 {
				t.Errorf("a store recebeu %d patches de um registro inválido", len(store.got))
			}
		})
	}
}
