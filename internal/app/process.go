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

// ErrThrottled: o participante respondeu 429. É "definitivamente NÃO processado", o oposto de
// ErrOutcomeUnknown: reenviar é seguro. Nunca é embrulhado com ErrOutcomeUnknown. O erro concreto
// é *ThrottledError, que carrega o Retry-After.
var ErrThrottled = errors.New("participante respondeu 429: não processado")

// PaymentStatus é a decisão do participante sobre uma instrução.
type PaymentStatus string

const (
	PaymentAccepted PaymentStatus = "accepted"
	PaymentRejected PaymentStatus = "rejected"
	// PaymentNotFound só existe na resposta a uma consulta: o participante não tem registro.
	// Não prova que nada foi processado, então nunca autoriza reenviar.
	PaymentNotFound PaymentStatus = "not_found"
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

	// Status consulta o que o participante sabe da instrução. É a única operação segura depois
	// de um timeout. Devolve accepted, rejected (com o código) ou not_found; qualquer falha é
	// ErrOutcomeUnknown.
	Status(ctx context.Context, id string) (PaymentResult, error)
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
	firstDelay  time.Duration // espera até a primeira consulta de conciliação
}

// ThrottledError é o 429 com o tempo que o participante pediu para esperar (0 se não pediu).
type ThrottledError struct{ RetryAfter time.Duration }

func (e *ThrottledError) Error() string {
	return fmt.Sprintf("%v (retry-after %s)", ErrThrottled, e.RetryAfter)
}

func (e *ThrottledError) Unwrap() error { return ErrThrottled }

func NewProcessInstruction(uow UnitOfWork, participant Participant, firstDelay time.Duration) *ProcessInstruction {
	return &ProcessInstruction{uow: uow, participant: participant, firstDelay: firstDelay}
}

// Handle processa uma mensagem. Devolve nil quando a mensagem foi tratada ou não precisa de
// tratamento (outro tipo de evento, repetida, já reivindicada por outro processor), inclusive
// quando o envio ficou sem resposta e isso foi gravado como unknown. Devolve ErrOutcomeUnknown
// só se nem o unknown pôde ser gravado: o chamador NÃO deve repetir a mensagem. Qualquer
// outro erro é transitório e a mensagem pode ser repetida.
func (uc *ProcessInstruction) Handle(ctx context.Context, msg Message) error {
	if msg.EventType != string(domain.EventInstructionAccepted) && msg.EventType != string(domain.EventRoutingThrottled) {
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
	var throttled *ThrottledError
	if errors.As(err, &throttled) {
		return uc.recordThrottle(ctx, p, throttled)
	}
	if err != nil {
		return uc.recordUnknown(ctx, p, asUnknown(err))
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

	err = uc.commit(ctx, p.ID(), expected, event, nil)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrConcurrencyConflict):
		// Outro processo já resolveu a instrução (a conciliação, por exemplo).
		slog.InfoContext(ctx, "processor: instrução já resolvida por outro caminho", "instruction_id", string(p.ID()))
		return nil
	}
	// A instrução fica em in_flight; a conciliação consulta o participante e descobre o resultado.
	return fmt.Errorf("instrução %s: resposta do participante não gravada: %w: %w", p.ID(), ErrOutcomeUnknown, err)
}

// recordUnknown grava que o envio ficou sem resposta conclusiva: o evento RoutingTimedOut e a
// tarefa de conciliação na MESMA transação. Daqui em diante a instrução é unknown e quem a
// resolve é a conciliação, consultando o status; nunca reenviando.
func (uc *ProcessInstruction) recordUnknown(ctx context.Context, p *domain.PaymentInstruction, cause error) error {
	expected := p.Version()
	event, err := p.RecordTimeout()
	if err != nil {
		return fmt.Errorf("instrução %s: %w", p.ID(), err)
	}
	at := time.Now().Add(uc.firstDelay)
	err = uc.commit(ctx, p.ID(), expected, event, func(ctx context.Context, tx Tx) error {
		return tx.Reconciliation().Schedule(ctx, p.ID(), at)
	})
	switch {
	case err == nil:
		slog.WarnContext(ctx, "processor: resultado desconhecido gravado, aguardando conciliação",
			"instruction_id", string(p.ID()), "first_attempt_at", at, "error", cause)
		return nil
	case errors.Is(err, ErrConcurrencyConflict):
		slog.InfoContext(ctx, "processor: instrução já resolvida por outro caminho", "instruction_id", string(p.ID()))
		return nil
	}
	// Nem o unknown foi gravado: fica em in_flight e a mensagem não será repetida.
	return fmt.Errorf("instrução %s: timeout não gravado: %w: %w", p.ID(), ErrOutcomeUnknown, err)
}

// commit grava o evento na versão esperada, junto com also (se houver) na mesma transação.
// Repete só a gravação, até applyAttempts vezes: reentregar a mensagem não ajudaria, pois a
// instrução já está em in_flight e ela seria pulada como repetida, perdendo o que já sabemos.
// Um conflito de versão encerra na hora e é devolvido ao chamador.
func (uc *ProcessInstruction) commit(ctx context.Context, id domain.InstructionID, expected int, event domain.Event,
	also func(ctx context.Context, tx Tx) error) error {
	var err error
	for attempt := range applyAttempts {
		if attempt > 0 {
			time.Sleep(applyPause)
		}
		err = uc.uow.Do(ctx, func(ctx context.Context, tx Tx) error {
			if err := tx.Events().Append(ctx, id, expected, event); err != nil {
				return err
			}
			if also == nil {
				return nil
			}
			return also(ctx, tx)
		})
		if err == nil || errors.Is(err, ErrConcurrencyConflict) {
			return err
		}
	}
	return err
}

// asUnknown garante que um erro do envio carregue ErrOutcomeUnknown: depois do Pay, não
// importa a causa, a requisição pode ter chegado.
func asUnknown(err error) error {
	if errors.Is(err, ErrOutcomeUnknown) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrOutcomeUnknown, err)
}

// recordThrottle grava o 429: RoutingThrottled devolve a instrução a accepted. Nada foi
// processado, então a mensagem que esse evento gera no outbox a reenvia. Antes de devolver,
// espera o Retry-After pedido (limitado), porque o handler em andamento segura a partição: é a
// contrapressão sobre um participante que mandou ir mais devagar.
func (uc *ProcessInstruction) recordThrottle(ctx context.Context, p *domain.PaymentInstruction, cause *ThrottledError) error {
	expected := p.Version()
	event, err := p.Throttle()
	if err != nil {
		return fmt.Errorf("instrução %s: %w", p.ID(), err)
	}
	err = uc.commit(ctx, p.ID(), expected, event, nil)
	switch {
	case err == nil:
		slog.WarnContext(ctx, "processor: participante deu 429, a instrução será reenviada",
			"instruction_id", string(p.ID()), "retry_after", cause.RetryAfter.String())
		sleep(ctx, min(cause.RetryAfter, maxThrottlePause))
		return nil
	case errors.Is(err, ErrConcurrencyConflict):
		slog.InfoContext(ctx, "processor: instrução já resolvida por outro caminho", "instruction_id", string(p.ID()))
		return nil
	}
	// A instrução fica em in_flight e a mensagem não será repetida (já foi reivindicada).
	return fmt.Errorf("instrução %s: 429 não gravado: %w: %w", p.ID(), ErrOutcomeUnknown, err)
}
