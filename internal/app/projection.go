// projection.go
// A projeção das instruções: o envelope lido do Kafka, o Patch que cada evento produz e o caso de
// uso ProjectEvent que o aplica numa ProjectionStore.
// Objetivo: o lado de leitura do sistema. A projeção é uma função PURA dos eventos: cada evento
// vira um Patch só pelo que ele mesmo carrega (tipo e dados), sem ler o estado anterior e sem
// consultar o Postgres; o único estado do projetor é a versão do documento, e é ela que torna a
// aplicação idempotente (o mesmo evento duas vezes não muda nada, um evento fora de ordem espera).
// Nenhum efeito colateral mora aqui: enviar ao participante é do lado do comando.

package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/domain"
	"github.com/VictorM0nteiro/payrouter/internal/metrics"
)

var (
	// ErrInvalidEvent: o registro não é um evento que a projeção entenda (JSON quebrado, tipo
	// desconhecido, versão incoerente). É definitivo: repetir não ajuda, então é descartado.
	ErrInvalidEvent = errors.New("evento inválido para a projeção")

	// ErrGap: falta um evento anterior a este (a versão do documento está atrás de version-1).
	// É transitório: o consumidor repete a mesma mensagem até o anterior chegar.
	ErrGap = errors.New("projeção: falta um evento anterior a este")
)

// ProjectionOutcome é o que a store fez com um Patch.
type ProjectionOutcome int

const (
	ProjectionApplied   ProjectionOutcome = iota + 1 // gravou: o documento avançou uma versão
	ProjectionDuplicate                              // o documento já estava nesta versão ou além
	ProjectionGap                                    // o documento ainda não chegou à versão anterior
)

// Envelope é o corpo de cada registro do tópico, como o event store o grava no outbox.
type Envelope struct {
	AggregateID string          `json:"aggregate_id"`
	Version     int             `json:"version"`
	EventType   string          `json:"event_type"`
	OccurredAt  time.Time       `json:"occurred_at"`
	Data        json.RawMessage `json:"data"`
}

// InstructionData são os dados que só o primeiro evento (InstructionAccepted) carrega.
type InstructionData struct {
	AmountCents int64 // centavos, nunca float
	Debtor      string
	Creditor    string
	EndToEndID  string
}

// Patch é o efeito de um evento no documento da instrução. Os campos vazios não mudam.
type Patch struct {
	AggregateID string
	Version     int
	OccurredAt  time.Time

	Status      domain.Status    // vazio = o status não muda (StatusQueried)
	Instruction *InstructionData // só na versão 1
	RejectCode  string           // vazio = não muda
	AddAttempt  bool             // soma uma tentativa de envio
}

// dataAccepted, dataRejected espelham os dados que o codec do Postgres grava em cada evento.
type dataAccepted struct {
	AmountCents int64  `json:"amount_cents"`
	Debtor      string `json:"debtor"`
	Creditor    string `json:"creditor"`
	EndToEndID  string `json:"end_to_end_id"`
}

type dataRejected struct {
	Code string `json:"code"`
}

// PatchFor decodifica um registro do tópico e devolve o efeito dele no documento. Devolve
// ErrInvalidEvent (embrulhado) se o registro não for um evento coerente.
//
// A tabela evento -> campos abaixo duplica a máquina de estados do domínio de propósito: o
// projetor não pode ler o estado anterior para decidir. O teste de conferência (projection_test.go)
// reaplica históricos reais do domínio e prova que as duas tabelas dão o mesmo status.
func PatchFor(payload []byte) (Patch, error) {
	var env Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return Patch{}, fmt.Errorf("%w: %w", ErrInvalidEvent, err)
	}
	if env.AggregateID == "" || env.Version <= 0 {
		return Patch{}, fmt.Errorf("%w: sem aggregate_id ou com versão %d", ErrInvalidEvent, env.Version)
	}
	p := Patch{AggregateID: env.AggregateID, Version: env.Version, OccurredAt: env.OccurredAt}
	eventType := domain.EventType(env.EventType)
	// A versão 1 é sempre InstructionAccepted, e só ela: é assim que o documento nasce.
	if (env.Version == 1) != (eventType == domain.EventInstructionAccepted) {
		return Patch{}, fmt.Errorf("%w: %s na versão %d", ErrInvalidEvent, env.EventType, env.Version)
	}

	switch eventType {
	case domain.EventInstructionAccepted:
		var d dataAccepted
		if err := json.Unmarshal(env.Data, &d); err != nil {
			return Patch{}, fmt.Errorf("%w: dados de %s: %w", ErrInvalidEvent, env.EventType, err)
		}
		p.Status = domain.StatusAccepted
		p.Instruction = &InstructionData{AmountCents: d.AmountCents, Debtor: d.Debtor, Creditor: d.Creditor, EndToEndID: d.EndToEndID}
	case domain.EventRoutingAttempted:
		p.Status, p.AddAttempt = domain.StatusInFlight, true
	case domain.EventParticipantAccepted:
		p.Status = domain.StatusSettled
	case domain.EventParticipantRejected:
		var d dataRejected
		if err := json.Unmarshal(env.Data, &d); err != nil {
			return Patch{}, fmt.Errorf("%w: dados de %s: %w", ErrInvalidEvent, env.EventType, err)
		}
		p.Status, p.RejectCode = domain.StatusRejected, d.Code
	case domain.EventRoutingTimedOut:
		p.Status = domain.StatusUnknown
	case domain.EventStatusQueried:
		// Só registra a consulta: o status não muda (o documento só avança de versão).
	case domain.EventRoutingThrottled:
		p.Status = domain.StatusAccepted
	case domain.EventReconciliationExhausted:
		p.Status = domain.StatusFailed
	default:
		return Patch{}, fmt.Errorf("%w: tipo %q", ErrInvalidEvent, env.EventType)
	}
	return p, nil
}

// ProjectionStore grava os Patches num documento por instrução.
type ProjectionStore interface {
	// Apply aplica o Patch se, e só se, o documento está exatamente na versão anterior (ou não
	// existe, na versão 1). Mesma versão de novo -> Duplicate; documento atrás -> Gap.
	Apply(ctx context.Context, p Patch) (ProjectionOutcome, error)
}

// ProjectEvent é o caso de uso do projetor.
type ProjectEvent struct {
	store   ProjectionStore
	metrics *metrics.Projector // nil = sem métricas
}

func NewProjectEvent(store ProjectionStore) *ProjectEvent {
	return &ProjectEvent{store: store}
}

// WithMetrics liga as métricas do projetor.
func (u *ProjectEvent) WithMetrics(m *metrics.Projector) *ProjectEvent {
	u.metrics = m
	return u
}

// Handle projeta um registro. Devolve nil quando o registro foi aplicado, já estava aplicado ou é
// inválido (o offset avança); devolve erro, que o consumidor repete no lugar, se a store falhou ou
// se falta um evento anterior (ErrGap).
func (u *ProjectEvent) Handle(ctx context.Context, msg Message) error {
	p, err := PatchFor(msg.Payload)
	if err == nil && (p.AggregateID != msg.AggregateID || p.Version != msg.Version) {
		err = fmt.Errorf("%w: o corpo (%s/%d) não bate com a chave e os headers (%s/%d)",
			ErrInvalidEvent, p.AggregateID, p.Version, msg.AggregateID, msg.Version)
	}
	if err != nil {
		slog.ErrorContext(ctx, "projector: registro inválido descartado", "aggregate_id", msg.AggregateID, "version", msg.Version, "error", err)
		u.metrics.Event("invalid")
		return nil
	}
	outcome, err := u.store.Apply(ctx, p)
	if err != nil {
		return fmt.Errorf("projetando %s/%d: %w", p.AggregateID, p.Version, err)
	}
	switch outcome {
	case ProjectionApplied:
		u.metrics.Event("applied")
	case ProjectionDuplicate:
		u.metrics.Event("duplicate")
	case ProjectionGap:
		u.metrics.Event("gap")
		return fmt.Errorf("projetando %s/%d: %w", p.AggregateID, p.Version, ErrGap)
	}
	return nil
}
