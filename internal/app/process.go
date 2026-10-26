// process.go
// Caso de uso do processor: reage a um InstructionAccepted, despacha a instrução ao
// participante e aplica a resposta como ParticipantAccepted ou ParticipantRejected.
// Objetivo: ser o lugar do efeito colateral no lado do comando. A idempotência é pela versão
// do agregado, sem tabela de deduplicação: o RoutingAttempted é gravado ANTES do envio com
// versão esperada e funciona como reivindicação, então uma mensagem repetida, ou dois
// processors na mesma mensagem, enviam no máximo uma vez. Se o resultado do envio não for
// conclusivo (timeout, erro de rede), nada é reenviado: a instrução fica em in_flight até a
// conciliação (sessões 13 e 14) consultar o status.

package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

const (
	// applyAttempts e applyPause limitam a repetição da gravação da resposta já recebida.
	applyAttempts = 3
	applyPause    = 200 * time.Millisecond
)

// ErrOutcomeUnknown: não se sabe se o participante processou a instrução (timeout, conexão
// caída, resposta ilegível). A única operação segura depois disso é consultar o status;
// reenviar pode cobrar duas vezes. Não confundir com um 429, que é "não processado".
var ErrOutcomeUnknown = errors.New("resultado do envio desconhecido")

// PaymentStatus é a decisão do participante sobre uma instrução.
type PaymentStatus string

const (
	PaymentAccepted PaymentStatus = "accepted"
	PaymentRejected PaymentStatus = "rejected"
)

// PaymentRequest é o que se envia ao participante. O valor é sempre int64 de centavos.
type PaymentRequest struct {
	InstructionID string
	AmountCents   int64
	Debtor        string
	Creditor      string
	EndToEndID    string
}

// PaymentResult é a resposta conclusiva do participante. Code só vale em PaymentRejected.
type PaymentResult struct {
	Status PaymentStatus
	Code   string
}

// Participant é a porta de saída para o sistema externo.
type Participant interface {
	// Pay envia a instrução. Devolve um resultado só se o participante respondeu de forma
	// conclusiva; qualquer outra coisa é ErrOutcomeUnknown.
	Pay(ctx context.Context, req PaymentRequest) (PaymentResult, error)
}

// Message é o que o processor lê de um registro do Kafka: a chave e os headers, nunca o
// corpo. Os dados da instrução vêm do event store, que é a fonte autoritativa.
type Message struct {
	AggregateID string
	EventType   string
	Version     int
}

// ProcessInstruction é o caso de uso.
type ProcessInstruction struct {
	uow         UnitOfWork
	participant Participant
}

func NewProcessInstruction(uow UnitOfWork, participant Participant) *ProcessInstruction {
	return &ProcessInstruction{uow: uow, participant: participant}
}

// Handle processa uma mensagem. Devolve nil quando a mensagem foi tratada ou não precisa de
// tratamento (outro tipo de evento, repetida, já reivindicada por outro processor). Devolve
// ErrOutcomeUnknown quando o envio foi feito e a resposta é desconhecida: o chamador NÃO deve
// repetir a mensagem. Qualquer outro erro é transitório e a mensagem pode ser repetida.
func (uc *ProcessInstruction) Handle(ctx context.Context, msg Message) error {
	if msg.EventType != string(domain.EventInstructionAccepted) {
		return nil
	}
	id := domain.InstructionID(msg.AggregateID)

	// Reivindica: relê o agregado e grava RoutingAttempted com a versão esperada. A
	// transação termina antes do envio, que acontece fora de qualquer transação.
	var (
		p       *domain.PaymentInstruction
		request PaymentRequest
		claimed bool
	)
	err := uc.uow.Do(ctx, func(ctx context.Context, tx Tx) error {
		events, err := tx.Events().Load(ctx, id)
		if err != nil {
			return err
		}
		p, err = domain.Rehydrate(events)
		if err != nil {
			return err
		}
		// Já avançou além desta mensagem: é repetição (ou outro processor chegou antes).
		if p.Status() != domain.StatusAccepted || p.Version() != msg.Version {
			return nil
		}
		request = PaymentRequest{
			InstructionID: string(p.ID()),
			AmountCents:   p.Amount().Cents(),
			Debtor:        p.Debtor(),
			Creditor:      p.Creditor(),
			EndToEndID:    p.EndToEndID(),
		}
		expected := p.Version() // lida ANTES do comando: depois dele já é a versão nova
		event, err := p.Route()
		if err != nil {
			return err
		}
		if err := tx.Events().Append(ctx, p.ID(), expected, event); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	if errors.Is(err, ErrConcurrencyConflict) {
		slog.InfoContext(ctx, "processor: instrução reivindicada por outro processor", "instruction_id", msg.AggregateID)
		return nil
	}
	if err != nil {
		return err
	}
	if !claimed {
		slog.InfoContext(ctx, "processor: mensagem repetida, nada a fazer", "instruction_id", msg.AggregateID, "version", msg.Version)
		return nil
	}

	// O efeito colateral. Daqui em diante um erro nunca autoriza reenviar.
	result, err := uc.participant.Pay(ctx, request)
	if err != nil {
		return fmt.Errorf("instrução %s: %w", id, asUnknown(err))
	}

	return uc.apply(ctx, p, result)
}

// apply grava a resposta do participante. p já está em in_flight (versão 2 na memória).
func (uc *ProcessInstruction) apply(ctx context.Context, p *domain.PaymentInstruction, result PaymentResult) error {
	expected := p.Version()
	var (
		event domain.Event
		err   error
	)
	switch result.Status {
	case PaymentAccepted:
		event, err = p.Settle()
	case PaymentRejected:
		event, err = p.Reject(result.Code)
	default:
		return fmt.Errorf("instrução %s: status %q do participante: %w", p.ID(), result.Status, ErrOutcomeUnknown)
	}
	if err != nil {
		return fmt.Errorf("instrução %s: %w", p.ID(), err)
	}

	// Repete só a gravação: reentregar a mensagem não ajudaria, pois a instrução já está em
	// in_flight e ela seria pulada como repetida, perdendo a resposta que já temos.
	for attempt := range applyAttempts {
		if attempt > 0 {
			time.Sleep(applyPause)
		}
		err = uc.uow.Do(ctx, func(ctx context.Context, tx Tx) error {
			return tx.Events().Append(ctx, p.ID(), expected, event)
		})
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrConcurrencyConflict) {
			// Outro processo já resolveu a instrução (a conciliação, por exemplo).
			slog.InfoContext(ctx, "processor: instrução já resolvida por outro caminho", "instruction_id", string(p.ID()))
			return nil
		}
	}
	// A instrução fica em in_flight; a conciliação consulta o participante e descobre o resultado.
	return fmt.Errorf("instrução %s: resposta do participante não gravada: %w: %w", p.ID(), ErrOutcomeUnknown, err)
}

// asUnknown garante que um erro do envio carregue ErrOutcomeUnknown: depois do Pay, não
// importa a causa, a requisição pode ter chegado.
func asUnknown(err error) error {
	if errors.Is(err, ErrOutcomeUnknown) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrOutcomeUnknown, err)
}
