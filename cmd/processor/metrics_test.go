// metrics_test.go
// Testes das métricas do processor contra Postgres e Redpanda reais e o participante simulado.
// Objetivo: provar que o que o Prometheus enxerga bate com a verdade medida do outro lado. O 429
// contado pelo processor é igual ao que o participante contou, e termina tudo settled; o timeout
// do caso cruel vira unknown, a conciliação o resolve e a fila volta a zero; e os gauges das
// filas leem o banco de verdade (o outbox cresce com uma instrução nova).

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
	"github.com/VictorM0nteiro/payrouter/internal/metrics"
	sim "github.com/VictorM0nteiro/payrouter/internal/participant"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// series lê uma série do registry pelo nome e pelos labels; ausente vale 0.
func series(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !matches(m.GetLabel(), labels) {
				continue
			}
			if m.GetCounter() != nil {
				return m.GetCounter().GetValue()
			}
			return m.GetGauge().GetValue()
		}
	}
	return 0
}

func matches(got []*dto.LabelPair, want map[string]string) bool {
	for k, v := range want {
		ok := false
		for _, l := range got {
			ok = ok || (l.GetName() == k && l.GetValue() == v)
		}
		if !ok {
			return false
		}
	}
	return true
}

func TestOs429DoProcessorBatemComOsDoParticipanteETudoTerminaSettled(t *testing.T) {
	requireInfra(t)
	srv := httptest.NewServer(sim.New(sim.Config{TPS: 3, Burst: 1}))
	defer srv.Close()
	ids := newBatch(t, "mt-429", 6)
	reg := metrics.NewRegistry()
	pm := metrics.NewProcessor(reg)

	stop := pipelineWith(t, srv.URL, ids, nil, pm)
	defer stop()
	for _, id := range ids {
		waitStatus(t, id, domain.StatusSettled)
	}

	seenByParticipant := 0
	for _, id := range ids {
		seenByParticipant += participantThrottled(t, srv.URL, id)
	}
	if seenByParticipant == 0 {
		t.Fatal("nenhum 429 aconteceu, o teste não provaria nada")
	}
	if got := series(t, reg, "payrouter_participant_calls_total", map[string]string{"op": "pay", "result": "throttled"}); int(got) != seenByParticipant {
		t.Errorf("429 no processor = %v, no participante = %d; esperado iguais", got, seenByParticipant)
	}
	if got := series(t, reg, "payrouter_routing_outcomes_total", map[string]string{"outcome": "settled"}); got != 6 {
		t.Errorf("settled = %v, esperado 6", got)
	}
	if got := series(t, reg, "payrouter_participant_calls_total", map[string]string{"op": "pay", "result": "unknown"}); got != 0 {
		t.Errorf("pay unknown = %v, esperado 0: 429 nunca vira unknown", got)
	}
	if got := series(t, reg, "payrouter_consumer_records_total", map[string]string{"result": "backpressure"}); got != 0 {
		t.Errorf("backpressure = %v, esperado 0 (sem limite de saída)", got)
	}
}

func TestCasoCruelApareceNasMetricasEAFilaVoltaAZero(t *testing.T) {
	requireInfra(t)
	ctx := context.Background()
	srv := httptest.NewServer(sim.New(sim.Config{LateDelay: time.Second}))
	defer srv.Close()
	id := newAccepted(t, "late:mt-cruel")
	client := pclient.NewClient(srv.URL, 200*time.Millisecond)
	reg := metrics.NewRegistry()
	pm := metrics.NewProcessor(reg)
	metrics.RegisterBacklog(reg, postgres.NewBacklogReader(testPool).Read)

	uc := app.NewProcessInstruction(postgres.NewUnitOfWork(testPool), client, time.Second).WithMetrics(pm)
	if err := uc.Handle(ctx, acceptedMsg(id)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	pending := series(t, reg, "payrouter_reconciliation_tasks_pending", nil)
	if _, err := testPool.Exec(ctx,
		`UPDATE reconciliation_tasks SET next_attempt_at = now() - interval '1 second' WHERE aggregate_id = $1::uuid`, string(id)); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler(client).WithMetrics(pm).RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	if got := series(t, reg, "payrouter_participant_calls_total", map[string]string{"op": "pay", "result": "unknown"}); got != 1 {
		t.Errorf("pay unknown = %v, esperado 1", got)
	}
	if got := series(t, reg, "payrouter_routing_outcomes_total", map[string]string{"outcome": "unknown"}); got != 1 {
		t.Errorf("outcome unknown = %v, esperado 1", got)
	}
	if got := series(t, reg, "payrouter_participant_calls_total", map[string]string{"op": "status", "result": "ok"}); got < 1 {
		t.Errorf("status ok = %v, esperado pelo menos 1", got)
	}
	if got := series(t, reg, "payrouter_reconciliation_queries_total", map[string]string{"result": "resolved"}); got != 1 {
		t.Errorf("conciliação resolved = %v, esperado 1", got)
	}
	if after := series(t, reg, "payrouter_reconciliation_tasks_pending", nil); after >= pending {
		t.Errorf("fila de conciliação: antes %v, depois %v; esperado menor", pending, after)
	}
}

func TestGaugesDoOutboxLeemOBancoDeVerdade(t *testing.T) {
	requireInfra(t)
	reader := postgres.NewBacklogReader(testPool)
	before, err := reader.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	newAccepted(t, "mt-outbox") // grava o evento e a linha do outbox, que ninguém publica aqui
	time.Sleep(50 * time.Millisecond)
	after, err := reader.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if after.OutboxPending != before.OutboxPending+1 {
		t.Errorf("outbox pendente: antes %d, depois %d; esperado +1", before.OutboxPending, after.OutboxPending)
	}
	if after.OutboxOldestAge <= 0 {
		t.Errorf("idade da mais antiga = %v, esperado > 0", after.OutboxOldestAge)
	}
}
