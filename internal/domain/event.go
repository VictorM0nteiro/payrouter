// event.go
// Os oito eventos do agregado PaymentInstruction e o tipo Event.
// Objetivo: fixar o vocabulário do sistema. Evento é fato consumado, no passado
// (InstructionAccepted, nunca AcceptInstruction), imutável e sem comportamento: só dados.
// Versão e horário de ocorrência pertencem ao event store, não a estes tipos.

package domain

// InstructionID identifica uma instrução (na borda é um UUID em texto).
type InstructionID string

// EventType é o nome estável do evento, usado na tabela de transições e gravado em
// events.event_type.
type EventType string

const (
	EventInstructionAccepted     EventType = "InstructionAccepted"
	EventRoutingAttempted        EventType = "RoutingAttempted"
	EventParticipantAccepted     EventType = "ParticipantAccepted"
	EventParticipantRejected     EventType = "ParticipantRejected"
	EventRoutingTimedOut         EventType = "RoutingTimedOut"
	EventStatusQueried           EventType = "StatusQueried"
	EventRoutingThrottled        EventType = "RoutingThrottled"
	EventReconciliationExhausted EventType = "ReconciliationExhausted"
)

// Event é um fato ocorrido com a instrução.
type Event interface {
	Type() EventType
}

// QueryResult é o que o participante respondeu a uma consulta de status.
type QueryResult string

const (
	QueryAccepted QueryResult = "accepted"
	QueryRejected QueryResult = "rejected"
	QueryUnknown  QueryResult = "unknown"
)

func (r QueryResult) valid() bool {
	return r == QueryAccepted || r == QueryRejected || r == QueryUnknown
}

// InstructionAccepted: validada e persistida, nada enviado ainda.
type InstructionAccepted struct {
	ID         InstructionID
	Amount     Money
	Debtor     string
	Creditor   string
	EndToEndID string
}
