// reconcile.go
// Caso de uso da conciliação: varre as tarefas vencidas, consulta o status da instrução no
// participante e aplica a resposta, com backoff e jitter entre as tentativas.
// Objetivo: resolver o unknown SEM reenviar. Consultar o status é a única operação segura
// depois de um timeout: se o participante diz accepted ou rejected, a instrução é resolvida a
// partir da resposta dele; se não sabe (not_found, timeout, erro), a consulta é registrada e a
// tarefa volta à fila mais tarde. Cada tarefa é reivindicada com um lease, então vários
// processors nunca consultam a mesma, e uma queda no meio só adia a tarefa até o lease vencer.

package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

// ReconciliationTask é uma instrução em unknown à espera de consulta.
type ReconciliationTask struct {
	ID       domain.InstructionID
	Attempts int // consultas já feitas
}

// ReconcileConfig parametriza o laço. Todos os campos são obrigatórios e positivos.
type ReconcileConfig struct {
	Batch       int           // tarefas reivindicadas por rodada
	Lease       time.Duration // quanto uma tarefa reivindicada fica fora da fila
	Poll        time.Duration // espera entre rodadas vazias
	BackoffBase time.Duration // espera antes da 2ª consulta; dobra a cada tentativa
	BackoffMax  time.Duration // teto da espera
}

// Reconcile é o caso de uso.
type Reconcile struct {
	uow         UnitOfWork
	participant Participant
	cfg         ReconcileConfig
}

func NewReconcile(uow UnitOfWork, participant Participant, cfg ReconcileConfig) *Reconcile {
	return &Reconcile{uow: uow, participant: participant, cfg: cfg}
}

// Backoff é a espera depois da tentativa de número attempts (1 = a primeira consulta): dobra
// a cada tentativa a partir de base, limitada por max, com jitter. O jitter mantém metade da
// espera fixa e sorteia a outra metade com r em [0,1), para tarefas que falharam juntas não
// consultarem juntas e para a espera nunca cair perto de zero.
func Backoff(attempts int, base, max time.Duration, r float64) time.Duration {
	ceiling := base
	for i := 1; i < attempts && ceiling < max; i++ {
		ceiling *= 2
	}
	ceiling = min(ceiling, max)
	return ceiling/2 + time.Duration(r*float64(ceiling/2))
}

// RunOnce faz uma rodada: reivindica as tarefas vencidas e concilia cada uma. Devolve quantas
// reivindicou. O erro de uma tarefa é registrado e não interrompe as outras; ela volta à fila
// quando o lease vence.
func (r *Reconcile) RunOnce(ctx context.Context) (int, error) {
	var tasks []ReconciliationTask
	err := r.uow.Do(ctx, func(ctx context.Context, tx Tx) error {
		var err error
		tasks, err = tx.Reconciliation().Claim(ctx, r.cfg.Batch, r.cfg.Lease)
		return err
	})
	if err != nil {
		return 0, err
	}
	for _, t := range tasks {
		if ctx.Err() != nil {
			break
		}
		if err := r.reconcile(ctx, t); err != nil {
			slog.ErrorContext(ctx, "conciliação: tarefa falhou, será retomada quando o lease vencer",
				"instruction_id", string(t.ID), "error", err)
		}
	}
	return len(tasks), nil
}

// Run repete as rodadas até o contexto terminar. Rodada vazia espera Poll; erro espera com
// backoff, como o relay.
func (r *Reconcile) Run(ctx context.Context) error {
	wait := r.cfg.Poll
	for ctx.Err() == nil {
		n, err := r.RunOnce(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			slog.ErrorContext(ctx, "conciliação: rodada falhou, será repetida", "error", err, "retry_in", wait.String())
			sleep(ctx, wait)
			wait = min(wait*2, maxRelayBackoff)
		case n == 0:
			wait = r.cfg.Poll
			sleep(ctx, r.cfg.Poll)
		default:
			wait = r.cfg.Poll
		}
	}
	return nil
}

// reconcile consulta uma tarefa reivindicada e grava o que o participante respondeu.
func (r *Reconcile) reconcile(ctx context.Context, t ReconciliationTask) error {
	var p *domain.PaymentInstruction
	err := r.uow.Do(ctx, func(ctx context.Context, tx Tx) error {
		events, err := tx.Events().Load(ctx, t.ID)
		if err != nil {
			return err
		}
		p, err = domain.Rehydrate(events)
		return err
	})
	if err != nil {
		return err
	}
	// Resolvida por outro caminho (ex.: a resposta do envio chegou depois): a tarefa sobrou.
	if p.Status() != domain.StatusUnknown {
		return r.finish(ctx, t.ID)
	}

	// A consulta acontece fora de qualquer transação. Falha ou not_found não resolvem nada.
	res, qerr := r.participant.Status(ctx, string(t.ID))
	expected := p.Version() // lida ANTES dos comandos: depois deles já é a versão nova
	events, resolved, err := r.apply(p, res, qerr)
	if err != nil {
		return err
	}

	err = r.uow.Do(ctx, func(ctx context.Context, tx Tx) error {
		if err := tx.Events().Append(ctx, t.ID, expected, events...); err != nil {
			return err
		}
		if resolved {
			return tx.Reconciliation().Complete(ctx, t.ID)
		}
		next := time.Now().Add(Backoff(t.Attempts+1, r.cfg.BackoffBase, r.cfg.BackoffMax, rand.Float64()))
		return tx.Reconciliation().Reschedule(ctx, t.ID, next)
	})
	if errors.Is(err, ErrConcurrencyConflict) {
		// Outro processo resolveu a instrução entre a leitura e a gravação.
		slog.InfoContext(ctx, "conciliação: instrução já resolvida por outro caminho", "instruction_id", string(t.ID))
		return r.finish(ctx, t.ID)
	}
	if err == nil && resolved {
		slog.InfoContext(ctx, "conciliação: unknown resolvido pela consulta", "instruction_id", string(t.ID),
			"status", string(p.Status()), "attempts", t.Attempts+1)
	}
	return err
}

// apply registra a consulta no agregado e, se a resposta for conclusiva, o desfecho. Devolve
// os eventos a gravar e se a instrução ficou resolvida.
func (r *Reconcile) apply(p *domain.PaymentInstruction, res PaymentResult, qerr error) ([]domain.Event, bool, error) {
	result, resolve := domain.QueryUnknown, func() (domain.Event, error) { return nil, nil }
	if qerr == nil {
		switch res.Status {
		case PaymentAccepted:
			result, resolve = domain.QueryAccepted, p.Settle
		case PaymentRejected:
			result, resolve = domain.QueryRejected, func() (domain.Event, error) { return p.Reject(res.Code) }
		}
	}
	queried, err := p.RecordStatusQuery(result)
	if err != nil {
		return nil, false, fmt.Errorf("instrução %s: %w", p.ID(), err)
	}
	outcome, err := resolve()
	if err != nil {
		return nil, false, fmt.Errorf("instrução %s: %w", p.ID(), err)
	}
	if outcome == nil {
		return []domain.Event{queried}, false, nil
	}
	return []domain.Event{queried, outcome}, true, nil
}

// finish retira a tarefa da fila numa transação própria.
func (r *Reconcile) finish(ctx context.Context, id domain.InstructionID) error {
	return r.uow.Do(ctx, func(ctx context.Context, tx Tx) error {
		return tx.Reconciliation().Complete(ctx, id)
	})
}
