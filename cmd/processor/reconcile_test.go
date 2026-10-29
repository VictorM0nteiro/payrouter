// reconcile_test.go
// Testes da conciliação contra um Postgres real (testcontainers), com um participante falso.
// Objetivo: provar a sessão 14. Uma instrução em unknown é resolvida pela RESPOSTA do
// participante à consulta de status (accepted ou rejected), registrando a consulta e o
// desfecho juntos e retirando a tarefa; uma consulta sem conclusão (not_found, falha) deixa a
// instrução em unknown e reagenda a tarefa mais tarde; e nada é reenviado em nenhum caso.

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/adapters/postgres"
	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

// unknownDue cria uma instrução em unknown (o envio deu timeout) com a tarefa já vencida.
func unknownDue(t *testing.T, e2e string) domain.InstructionID {
	t.Helper()
	id := newAccepted(t, e2e)
	send := &fakeParticipant{err: errors.New("timeout")}
	if err := newUseCase(send).Handle(context.Background(), acceptedMsg(id)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if _, err := testPool.Exec(context.Background(),
		`UPDATE reconciliation_tasks SET next_attempt_at = now() - interval '1 second' WHERE aggregate_id = $1::uuid`, string(id)); err != nil {
		t.Fatal(err)
	}
	return id
}

func reconciler(p app.Participant) *app.Reconcile {
	return app.NewReconcile(postgres.NewUnitOfWork(testPool), p, app.ReconcileConfig{
		Batch: 10, Lease: 30 * time.Second, Poll: 10 * time.Millisecond,
		BackoffBase: 2 * time.Second, BackoffMax: 10 * time.Second,
	})
}

func eventTypes(t *testing.T, id domain.InstructionID) []string {
	t.Helper()
	rows, err := testPool.Query(context.Background(),
		`SELECT event_type FROM events WHERE aggregate_id = $1::uuid ORDER BY version`, string(id))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func lastPayload(t *testing.T, id domain.InstructionID) string {
	t.Helper()
	var s string
	if err := testPool.QueryRow(context.Background(),
		`SELECT payload::text FROM events WHERE aggregate_id = $1::uuid ORDER BY version DESC LIMIT 1`, string(id)).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func join(s []string) string {
	out := ""
	for i, v := range s {
		if i > 0 {
			out += ","
		}
		out += v
	}
	return out
}

func TestConsultaAceitaResolveOUnknownParaSettled(t *testing.T) {
	requireInfra(t)
	id := unknownDue(t, "rc-1")
	fake := &fakeParticipant{statusResult: app.PaymentResult{Status: app.PaymentAccepted}}

	n, err := reconciler(fake).RunOnce(context.Background())

	if err != nil || n < 1 {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	if p := load(t, id); p.Status() != domain.StatusSettled {
		t.Errorf("status = %s, esperado settled", p.Status())
	}
	want := "InstructionAccepted,RoutingAttempted,RoutingTimedOut,StatusQueried,ParticipantAccepted"
	if got := join(eventTypes(t, id)); got != want {
		t.Errorf("eventos = %s, esperado %s", got, want)
	}
	if n, _, _ := tasks(t, id); n != 0 {
		t.Errorf("tarefas = %d, esperado 0", n)
	}
	if fake.calls.Load() != 0 {
		t.Errorf("envios na conciliação = %d, esperado 0: consultar nunca reenvia", fake.calls.Load())
	}
}

func TestConsultaRejeitadaResolveOUnknownParaRejected(t *testing.T) {
	requireInfra(t)
	id := unknownDue(t, "rc-2")
	fake := &fakeParticipant{statusResult: app.PaymentResult{Status: app.PaymentRejected, Code: "account_closed"}}

	if _, err := reconciler(fake).RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	if p := load(t, id); p.Status() != domain.StatusRejected {
		t.Errorf("status = %s, esperado rejected", p.Status())
	}
	if got := lastPayload(t, id); got != `{"code": "account_closed"}` {
		t.Errorf("payload do desfecho = %s", got)
	}
	if n, _, _ := tasks(t, id); n != 0 {
		t.Errorf("tarefas = %d, esperado 0", n)
	}
}

func TestConsultaSemConclusaoMantemUnknownEReagenda(t *testing.T) {
	requireInfra(t)
	tests := []struct {
		name string
		fake *fakeParticipant
	}{
		{"not_found", &fakeParticipant{statusResult: app.PaymentResult{Status: app.PaymentNotFound}}},
		{"consulta_falhou", &fakeParticipant{statusErr: errors.New("timeout")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := unknownDue(t, "rc-3-"+tc.name)
			before := time.Now()
			r := reconciler(tc.fake)

			if _, err := r.RunOnce(context.Background()); err != nil {
				t.Fatal(err)
			}

			if p := load(t, id); p.Status() != domain.StatusUnknown {
				t.Errorf("status = %s, esperado unknown", p.Status())
			}
			want := "InstructionAccepted,RoutingAttempted,RoutingTimedOut,StatusQueried"
			if got := join(eventTypes(t, id)); got != want {
				t.Errorf("eventos = %s, esperado %s", got, want)
			}
			if got := lastPayload(t, id); got != `{"result": "unknown"}` {
				t.Errorf("payload da consulta = %s", got)
			}
			n, attempts, next := tasks(t, id)
			// Backoff da 1ª tentativa: entre base/2 e base (1s a 2s), com jitter.
			if n != 1 || attempts != 1 || next.Before(before.Add(time.Second-100*time.Millisecond)) || next.After(time.Now().Add(2*time.Second)) {
				t.Errorf("tarefas=%d tentativas=%d próxima=%v; esperado 1, 1 e entre 1s e 2s a frente", n, attempts, next)
			}
			// O lease/agendamento tira a tarefa da fila: a rodada seguinte não a consulta de novo.
			calls := tc.fake.statusCalls.Load()
			if _, err := r.RunOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if tc.fake.statusCalls.Load() != calls {
				t.Errorf("a tarefa reagendada foi consultada de novo antes da hora")
			}
		})
	}
}

func TestInstrucaoJaResolvidaApenasRetiraATarefa(t *testing.T) {
	requireInfra(t)
	id := unknownDue(t, "rc-4")
	// A resposta do envio chegou por outro caminho e resolveu a instrução.
	if err := postgres.NewEventStore(testPool).Append(context.Background(), id, 3, domain.ParticipantAccepted{}); err != nil {
		t.Fatal(err)
	}
	fake := &fakeParticipant{statusResult: app.PaymentResult{Status: app.PaymentRejected, Code: "x"}}

	if _, err := reconciler(fake).RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	if n, _, _ := tasks(t, id); n != 0 {
		t.Errorf("tarefas = %d, esperado 0", n)
	}
	if p := load(t, id); p.Status() != domain.StatusSettled || p.Version() != 4 {
		t.Errorf("status=%s versão=%d; esperado settled, 4 (nenhum evento novo)", p.Status(), p.Version())
	}
	if fake.statusCalls.Load() != 0 {
		t.Errorf("consultas = %d, esperado 0", fake.statusCalls.Load())
	}
}
