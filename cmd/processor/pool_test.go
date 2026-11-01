// pool_test.go
// Testes do processor com o pool de workers, de ponta a ponta: outbox, relay, Redpanda, consumidor
// com vários workers, caso de uso e participante simulado de verdade (HTTP), contra Postgres real.
// Objetivo: provar a sessão 19b. Com mais de um worker os testes que já provavam o comportamento
// (os sete comportamentos do participante, o limite de saída, os 429) continuam valendo com os
// mesmos resultados, o limite de saída não gera nenhum 429 mesmo com workers em paralelo, e o limite
// adaptativo de requisições em voo cai quando o participante recusa, sem que nada seja reenviado em
// duplicidade (o participante recebe cada instrução uma vez só).
// Também testa a leitura das variáveis do pool.

package main

import (
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	"github.com/VictorM0nteiro/payrouter/internal/elastic"
	sim "github.com/VictorM0nteiro/payrouter/internal/participant"
)

// testWorkers e testConcurrency dimensionam o consumidor e o caso de uso dos pipelines de teste.
// O padrão é um worker e nenhum limite de requisições em voo, como antes da sessão 19.
var (
	testWorkers     = elastic.Config{Min: 1, Max: 1}
	testConcurrency app.Concurrency
)

// withPool dimensiona o pipeline só durante o teste.
func withPool(t *testing.T, cfg elastic.Config, c app.Concurrency) {
	t.Helper()
	prevWorkers, prevConc := testWorkers, testConcurrency
	testWorkers, testConcurrency = cfg, c
	t.Cleanup(func() { testWorkers, testConcurrency = prevWorkers, prevConc })
}

func TestPontaAPontaDosComportamentosComPool(t *testing.T) {
	withPool(t, elastic.Config{Min: 2, Max: 4}, nil)

	TestPontaAPontaDosComportamentos(t)
}

func TestLimiteDeSaidaComPoolSegueOTPSDoParticipanteESemNenhum429(t *testing.T) {
	withPool(t, elastic.Config{Min: 4, Max: 4}, nil)

	TestLimiteDeSaidaSegueOTPSDoParticipanteESemNenhum429(t)
}

func TestSemLimiteDeSaidaComPoolOs429AcontecemEATudoAindaTerminaSettled(t *testing.T) {
	withPool(t, elastic.Config{Min: 4, Max: 4}, nil)

	TestSemLimiteDeSaidaOs429AcontecemEATudoAindaTerminaSettled(t)
}

func TestPoolComLimiteDeRequisicoesEmVooCaiNos429ENadaEEnviadoDuasVezes(t *testing.T) {
	requireInfra(t)
	var downs atomic.Int32
	lim := elastic.NewLimiter(elastic.LimiterConfig{Min: 1, Max: 8, Initial: 8, OnChange: func(from, to int) {
		if to < from {
			downs.Add(1)
		}
	}})
	withPool(t, elastic.Config{Min: 8, Max: 8}, concurrency{lim})
	// O participante aguenta 3/s e o processor não limita a taxa: haverá 429, e cada um divide o limite.
	srv := httptest.NewServer(sim.New(sim.Config{TPS: 3, Burst: 1}))
	defer srv.Close()
	ids := newBatch(t, "pool-lim", 10)

	stop := pipeline(t, srv.URL, ids, nil)
	defer stop()
	for _, id := range ids {
		waitStatus(t, id, domain.StatusSettled)
	}

	total := 0
	for _, id := range ids {
		if got := participantReceived(t, srv.URL, id); got != 1 {
			t.Errorf("%s: o participante recebeu %d, esperado 1 (admitido uma vez só)", id, got)
		}
		total += participantThrottled(t, srv.URL, id)
	}
	if total == 0 {
		t.Fatal("nenhum 429 aconteceu, o teste não provaria nada")
	}
	if downs.Load() == 0 {
		t.Errorf("o limite nunca caiu apesar de %d respostas 429", total)
	}
	if s := lim.Stats(); s.InFlight != 0 || s.Waiting != 0 {
		t.Errorf("sobrou estado no limitador: %+v", s)
	}
}

func TestWorkersConfig(t *testing.T) {
	cases := []struct {
		name, min, max, queue string
		want                  elastic.Config
		wantErr               bool
	}{
		{"padrões: um worker, fila 64", "", "", "", elastic.Config{Min: 1, Max: 1, Queue: 64}, false},
		{"sem máximo, o máximo é o piso", "3", "", "", elastic.Config{Min: 3, Max: 3, Queue: 64}, false},
		{"os três valores", "2", "8", "128", elastic.Config{Min: 2, Max: 8, Queue: 128}, false},
		{"máximo menor que o piso", "4", "2", "", elastic.Config{}, true},
		{"piso zero", "0", "", "", elastic.Config{}, true},
		{"piso não numérico", "muitos", "", "", elastic.Config{}, true},
		{"fila negativa", "", "", "-5", elastic.Config{}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("PROCESSOR_WORKERS_MIN", c.min)
			t.Setenv("PROCESSOR_WORKERS_MAX", c.max)
			t.Setenv("PROCESSOR_QUEUE", c.queue)

			got, err := workersConfig()

			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, esperado erro = %v", err, c.wantErr)
			}
			if !c.wantErr && (got.Min != c.want.Min || got.Max != c.want.Max || got.Queue != c.want.Queue) {
				t.Errorf("min/max/queue = %d/%d/%d, esperado %d/%d/%d",
					got.Min, got.Max, got.Queue, c.want.Min, c.want.Max, c.want.Queue)
			}
		})
	}
}
