// instruction.go
// O agregado PaymentInstruction: estado, tabela de transições, Rehydrate e comandos.
// Objetivo: concentrar a máquina de estados num só lugar. O agregado DECIDE (valida a
// transição e devolve o evento) e nunca grava: quem persiste é o adapter. O estado é
// sempre consequência dos eventos aplicados, nunca atribuído diretamente.

package domain

import "fmt"

// Status é o estado atual da instrução, derivado dos eventos.
type Status string

const (
	StatusNew      Status = "new" // ainda sem nenhum evento
	StatusAccepted Status = "accepted"
	StatusInFlight Status = "in_flight"
	StatusUnknown  Status = "unknown"
	StatusSettled  Status = "settled"
	StatusRejected Status = "rejected"
	StatusFailed   Status = "failed"
)

// IsTerminal informa se o estado não admite mais nenhum evento.
func (s Status) IsTerminal() bool {
	return s == StatusSettled || s == StatusRejected || s == StatusFailed
}

// transitions é a máquina de estados inteira: estado atual -> evento permitido -> próximo
// estado. Estado ou evento ausente da tabela significa transição ilegal. Estados
// terminais não aparecem como chave.
var transitions = map[Status]map[EventType]Status{
	StatusNew: {
		EventInstructionAccepted: StatusAccepted,
	},
	StatusAccepted: {
		EventRoutingAttempted: StatusInFlight,
	},
	StatusInFlight: {
		EventParticipantAccepted: StatusSettled,
		EventParticipantRejected: StatusRejected,
		EventRoutingTimedOut:     StatusUnknown,
		EventRoutingThrottled:    StatusAccepted,
	},
	StatusUnknown: {
		EventStatusQueried:           StatusUnknown,
		EventParticipantAccepted:     StatusSettled,
		EventParticipantRejected:     StatusRejected,
		EventReconciliationExhausted: StatusFailed,
	},
}

func nextStatus(from Status, event Event) (Status, error) {
	to, ok := transitions[from][event.Type()]
	if !ok {
		return "", ErrIllegalTransition{From: from, Event: event.Type()}
	}
	return to, nil
}

// PaymentInstruction é o agregado. Campos privados: só mudam via apply.
type PaymentInstruction struct {
	id         InstructionID
	status     Status
	version    int
	amount     Money
	debtor     string
	creditor   string
	endToEndID string
	attempts   int
}

func (p *PaymentInstruction) ID() InstructionID { return p.id }
func (p *PaymentInstruction) Status() Status    { return p.status }
func (p *PaymentInstruction) Amount() Money     { return p.amount }
func (p *PaymentInstruction) Attempts() int     { return p.attempts }

// Version é a versão do último evento aplicado. Para usá-la como versão esperada no
// Append, leia-a ANTES de executar o comando: os comandos aplicam o evento no próprio
// agregado, então depois deles Version() já é a versão nova.
func (p *PaymentInstruction) Version() int { return p.version }

// apply muta o estado. Não valida: quem chama já validou a transição. Por isso nunca
// falha, o que permite reconstruir qualquer histórico válido.
func (p *PaymentInstruction) apply(e Event, to Status) {
	switch ev := e.(type) {
	case InstructionAccepted:
		p.id = ev.ID
		p.amount = ev.Amount
		p.debtor = ev.Debtor
		p.creditor = ev.Creditor
		p.endToEndID = ev.EndToEndID
	case RoutingAttempted:
		p.attempts++
	}
	p.status = to
	p.version++
}

// record valida a transição, aplica o evento e o devolve para o chamador gravar. Se a
// transição for ilegal, o agregado fica intacto.
func (p *PaymentInstruction) record(e Event) (Event, error) {
	to, err := nextStatus(p.status, e)
	if err != nil {
		return nil, err
	}
	p.apply(e, to)
	return e, nil
}

// Rehydrate reconstrói o agregado a partir do histórico, em ordem. Valida cada evento
// contra a mesma tabela dos comandos, então um histórico corrompido falha em vez de
// produzir um estado impossível.
func Rehydrate(events []Event) (*PaymentInstruction, error) {
	if len(events) == 0 {
		return nil, ErrEmptyHistory
	}
	p := &PaymentInstruction{status: StatusNew}
	for i, e := range events {
		to, err := nextStatus(p.status, e)
		if err != nil {
			return nil, fmt.Errorf("evento %d: %w", i+1, err)
		}
		p.apply(e, to)
	}
	return p, nil
}

// Accept cria uma instrução nova e devolve o evento InstructionAccepted para gravar.
func Accept(id InstructionID, amount Money, debtor, creditor, endToEndID string) (*PaymentInstruction, Event, error) {
	if amount <= 0 {
		return nil, nil, ErrInvalidAmount
	}
	if id == "" || debtor == "" || creditor == "" || endToEndID == "" || debtor == creditor {
		return nil, nil, ErrInvalidParties
	}
	p := &PaymentInstruction{status: StatusNew}
	e, err := p.record(InstructionAccepted{
		ID: id, Amount: amount, Debtor: debtor, Creditor: creditor, EndToEndID: endToEndID,
	})
	if err != nil {
		return nil, nil, err
	}
	return p, e, nil
}

// Route registra o despacho ao participante.
func (p *PaymentInstruction) Route() (Event, error) {
	return p.record(RoutingAttempted{})
}

// Settle registra que o participante confirmou.
func (p *PaymentInstruction) Settle() (Event, error) {
	return p.record(ParticipantAccepted{})
}

// Reject registra que o participante negou, com o código dele.
func (p *PaymentInstruction) Reject(code string) (Event, error) {
	if code == "" {
		return nil, ErrInvalidRejectionCode
	}
	return p.record(ParticipantRejected{Code: code})
}

// RecordTimeout registra a falta de resposta. A instrução passa a unknown: não se sabe se
// foi processada, e a única operação segura daí em diante é consultar o status.
func (p *PaymentInstruction) RecordTimeout() (Event, error) {
	return p.record(RoutingTimedOut{})
}

// RecordStatusQuery registra uma consulta de conciliação e o que o participante respondeu.
// Não muda o estado; se o resultado for conclusivo, o chamador emite Settle ou Reject.
func (p *PaymentInstruction) RecordStatusQuery(result QueryResult) (Event, error) {
	if !result.valid() {
		return nil, ErrInvalidQueryResult
	}
	return p.record(StatusQueried{Result: result})
}

// Throttle registra o 429: nada foi processado, então reenviar é seguro.
func (p *PaymentInstruction) Throttle() (Event, error) {
	return p.record(RoutingThrottled{})
}

// Exhaust registra que a conciliação esgotou as tentativas sem concluir.
func (p *PaymentInstruction) Exhaust(attempts int) (Event, error) {
	return p.record(ReconciliationExhausted{Attempts: attempts})
}
