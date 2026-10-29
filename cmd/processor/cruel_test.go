// cruel_test.go
// O caso cruel, contra o participante simulado de verdade (HTTP) e um Postgres real.
// Objetivo: provar a tese do projeto. O participante PROCESSA a instrução, mas só responde
// depois que o cliente desistiu (timeout). O processor não sabe o que aconteceu, vira unknown e
// NUNCA reenvia; a conciliação consulta o status, descobre que a operação aconteceu e liquida.
// O participante recebe a instrução exatamente uma vez.

package main

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	pclient "github.com/VictorM0nteiro/payrouter/internal/adapters/participant"
	"github.com/VictorM0nteiro/payrouter/internal/adapters/postgres"
	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	sim "github.com/VictorM0nteiro/payrouter/internal/participant"
)

func TestCasoCruelOParticipanteProcessaDepoisDoTimeoutEAConciliacaoDescobre(t *testing.T) {
	requireInfra(t)
	ctx := context.Background()
	srv := httptest.NewServer(sim.New(sim.Config{LateDelay: time.Second}))
	defer srv.Close()
	id := newAccepted(t, "late:cruel")
	client := pclient.NewClient(srv.URL, 200*time.Millisecond) // desiste muito antes do LateDelay

	// 1. O envio estoura o prazo do cliente: resultado desconhecido.
	uc := app.NewProcessInstruction(postgres.NewUnitOfWork(testPool), client, time.Second)
	if err := uc.Handle(ctx, acceptedMsg(id)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if p := load(t, id); p.Status() != domain.StatusUnknown {
		t.Fatalf("status = %s, esperado unknown (o processor não sabe o que houve)", p.Status())
	}

	// 2. O participante já processou, mesmo sem ter respondido a tempo.
	if got := participantReceived(t, srv.URL, id); got != 1 {
		t.Fatalf("participante recebeu %d, esperado 1", got)
	}

	// 3. A conciliação consulta e descobre que aconteceu. Nada é reenviado.
	if _, err := testPool.Exec(ctx,
		`UPDATE reconciliation_tasks SET next_attempt_at = now() - interval '1 second' WHERE aggregate_id = $1::uuid`, string(id)); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler(client).RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	if p := load(t, id); p.Status() != domain.StatusSettled {
		t.Errorf("status = %s, esperado settled", p.Status())
	}
	want := "InstructionAccepted,RoutingAttempted,RoutingTimedOut,StatusQueried,ParticipantAccepted"
	if got := join(eventTypes(t, id)); got != want {
		t.Errorf("eventos = %s, esperado %s", got, want)
	}
	time.Sleep(1500 * time.Millisecond) // passa o LateDelay: uma resposta tardia não pode mudar nada
	if got := participantReceived(t, srv.URL, id); got != 1 {
		t.Errorf("participante recebeu %d vezes, esperado exatamente 1: nunca reenviar", got)
	}
	if n, _, _ := tasks(t, id); n != 0 {
		t.Errorf("tarefas = %d, esperado 0", n)
	}
}
