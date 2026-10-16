// codec.go
// Conversão entre eventos do domínio e o JSON gravado em events.payload.
// Objetivo: o formato em disco é decisão do adapter, não do domínio. Os tipos do domínio
// não têm tags JSON; aqui cada evento com dados ganha um DTO com nomes estáveis em
// snake_case. Renomear um campo Go não corrompe o histórico já gravado.

package postgres

import (
	"encoding/json"
	"fmt"

	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

type acceptedPayload struct {
	ID          string `json:"id"`
	AmountCents int64  `json:"amount_cents"`
	Debtor      string `json:"debtor"`
	Creditor    string `json:"creditor"`
	EndToEndID  string `json:"end_to_end_id"`
}

type rejectedPayload struct {
	Code string `json:"code"`
}

type queriedPayload struct {
	Result string `json:"result"`
}

type exhaustedPayload struct {
	Attempts int `json:"attempts"`
}

// emptyPayload é o JSON dos eventos que não carregam dados.
var emptyPayload = []byte(`{}`)

// encode devolve o payload JSON do evento.
func encode(event domain.Event) ([]byte, error) {
	switch ev := event.(type) {
	case domain.InstructionAccepted:
		return json.Marshal(acceptedPayload{
			ID:          string(ev.ID),
			AmountCents: ev.Amount.Cents(),
			Debtor:      ev.Debtor,
			Creditor:    ev.Creditor,
			EndToEndID:  ev.EndToEndID,
		})
	case domain.ParticipantRejected:
		return json.Marshal(rejectedPayload{Code: ev.Code})
	case domain.StatusQueried:
		return json.Marshal(queriedPayload{Result: string(ev.Result)})
	case domain.ReconciliationExhausted:
		return json.Marshal(exhaustedPayload{Attempts: ev.Attempts})
	case domain.RoutingAttempted, domain.ParticipantAccepted, domain.RoutingTimedOut, domain.RoutingThrottled:
		return emptyPayload, nil
	default:
		return nil, fmt.Errorf("evento de tipo desconhecido: %T", event)
	}
}

// decode reconstrói o evento a partir do tipo e do payload gravados.
func decode(eventType string, payload []byte) (domain.Event, error) {
	switch domain.EventType(eventType) {
	case domain.EventInstructionAccepted:
		var p acceptedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("payload de %s: %w", eventType, err)
		}
		return domain.InstructionAccepted{
			ID:         domain.InstructionID(p.ID),
			Amount:     domain.Money(p.AmountCents),
			Debtor:     p.Debtor,
			Creditor:   p.Creditor,
			EndToEndID: p.EndToEndID,
		}, nil
	case domain.EventParticipantRejected:
		var p rejectedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("payload de %s: %w", eventType, err)
		}
		return domain.ParticipantRejected{Code: p.Code}, nil
	case domain.EventStatusQueried:
		var p queriedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("payload de %s: %w", eventType, err)
		}
		return domain.StatusQueried{Result: domain.QueryResult(p.Result)}, nil
	case domain.EventReconciliationExhausted:
		var p exhaustedPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("payload de %s: %w", eventType, err)
		}
		return domain.ReconciliationExhausted{Attempts: p.Attempts}, nil
	case domain.EventRoutingAttempted:
		return domain.RoutingAttempted{}, nil
	case domain.EventParticipantAccepted:
		return domain.ParticipantAccepted{}, nil
	case domain.EventRoutingTimedOut:
		return domain.RoutingTimedOut{}, nil
	case domain.EventRoutingThrottled:
		return domain.RoutingThrottled{}, nil
	default:
		return nil, fmt.Errorf("tipo de evento desconhecido no event store: %q", eventType)
	}
}
