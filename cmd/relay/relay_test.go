// relay_test.go
// Teste de integração do relay inteiro (Postgres + OutboxStore + Producer + Redpanda) com a
// queda no pior momento: depois de o broker confirmar o lote e antes de o outbox ser marcado.
// Objetivo: provar o critério da sessão 10. Nada se perde (toda versão de todo agregado chega
// ao tópico), a duplicata existe e é a assumida (at-least-once), e a ordem por agregado se
// mantém. Quem consome é idempotente pela versão, então a duplicata é inofensiva.

package main

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/VictorM0nteiro/payrouter/internal/adapters/kafka"
	"github.com/VictorM0nteiro/payrouter/internal/adapters/postgres"
	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	"github.com/VictorM0nteiro/payrouter/internal/testutil/kafkatest"
	"github.com/VictorM0nteiro/payrouter/internal/testutil/pgtest"
)

const topic = "payrouter.instructions.events"

var (
	testPool    *pgxpool.Pool
	testBrokers []string
	setupErr    error
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var stopPG, stopKafka func()
	testPool, stopPG, setupErr = pgtest.Start(ctx)
	if setupErr == nil {
		testBrokers, stopKafka, setupErr = kafkatest.Start(ctx)
	}
	if setupErr == nil {
		setupErr = kafkatest.CreateTopic(ctx, testBrokers, topic, 3)
	}
	code := m.Run()
	if stopKafka != nil {
		stopKafka()
	}
	if stopPG != nil {
		stopPG()
	}
	os.Exit(code)
}

// crashAfterPublish publica de verdade e então devolve erro na primeira chamada, imitando o
// relay que morre depois de o broker confirmar e antes de marcar o outbox.
type crashAfterPublish struct {
	inner   app.Publisher
	crashed atomic.Bool
}

func (c *crashAfterPublish) Publish(ctx context.Context, msgs []app.OutboxMessage) error {
	if err := c.inner.Publish(ctx, msgs); err != nil {
		return err
	}
	if c.crashed.CompareAndSwap(false, true) {
		return errors.New("relay morreu depois de publicar e antes de marcar")
	}
	return nil
}

func pending(t *testing.T) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRelayMortoNoMeioNaoPerdeNadaEPreservaAOrdem(t *testing.T) {
	if setupErr != nil {
		t.Fatalf("infraestrutura de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
	ctx := context.Background()

	// 6 agregados com 3 eventos cada, no outbox.
	const aggregates, perAggregate = 6, 3
	events := postgres.NewEventStore(testPool)
	ids := make(map[string]bool)
	for range aggregates {
		var id string
		if err := testPool.QueryRow(ctx, `SELECT gen_random_uuid()::text`).Scan(&id); err != nil {
			t.Fatal(err)
		}
		agg := domain.InstructionID(id)
		ids[id] = true
		steps := []domain.Event{domain.InstructionAccepted{ID: agg, Amount: 100, Debtor: "d", Creditor: "c", EndToEndID: "e2e-" + id}, domain.RoutingAttempted{}, domain.RoutingTimedOut{}}
		for v, e := range steps {
			if err := events.Append(ctx, agg, v, e); err != nil {
				t.Fatal(err)
			}
		}
	}
	total := aggregates * perAggregate
	if got := pending(t); got != total {
		t.Fatalf("pendentes antes = %d, esperado %d", got, total)
	}

	// Relay 1: o broker confirma a rodada e o relay "morre" antes de marcar.
	p1, err := kafka.NewProducer(testBrokers)
	if err != nil {
		t.Fatal(err)
	}
	relay1 := app.NewRelay(postgres.NewOutboxStore(testPool), &crashAfterPublish{inner: p1}, 100, time.Millisecond)
	if _, err := relay1.RunOnce(ctx); err == nil {
		t.Fatal("a rodada deveria ter falhado pela queda simulada")
	}
	p1.Close()
	if got := pending(t); got != total {
		t.Fatalf("pendentes depois da queda = %d, esperado %d: nada pode ter sido marcado", got, total)
	}

	// Relay 2 (processo novo, produtor novo) esvazia o outbox.
	p2, err := kafka.NewProducer(testBrokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	relay2 := app.NewRelay(postgres.NewOutboxStore(testPool), p2, 100, time.Millisecond)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- relay2.Run(runCtx) }()
	deadline := time.Now().Add(30 * time.Second)
	for pending(t) > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("relay 2: %v", err)
	}
	if got := pending(t); got != 0 {
		t.Fatalf("pendentes no fim = %d, esperado 0", got)
	}

	// O que chegou ao tópico.
	records, err := kafkatest.ReadAll(ctx, testBrokers, topic, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	firstSeen := make(map[string][]int) // agregado -> versões na ordem da primeira aparição
	seen := make(map[string]bool)
	for _, r := range records {
		v, err := strconv.Atoi(r.Headers["version"])
		if err != nil {
			t.Fatalf("header version inválido: %+v", r)
		}
		key := r.Key + "/" + r.Headers["version"]
		if !seen[key] {
			seen[key] = true
			firstSeen[r.Key] = append(firstSeen[r.Key], v)
		}
	}

	// 1. Nada se perdeu: toda versão de todo agregado apareceu ao menos uma vez.
	for id := range ids {
		got := firstSeen[id]
		if len(got) != perAggregate {
			t.Errorf("agregado %s: versões vistas %v, esperado 1..%d", id, got, perAggregate)
		}
		// 3. A ordem por agregado foi preservada apesar da queda e da republicação.
		for i, v := range got {
			if v != i+1 {
				t.Errorf("agregado %s: versões na ordem de chegada %v, esperado 1..%d", id, got, perAggregate)
				break
			}
		}
	}
	// 2. Houve duplicata: a rodada publicada antes da queda foi republicada (at-least-once).
	if len(records) <= total {
		t.Errorf("registros no tópico = %d; esperado mais que %d, a duplicata assumida do at-least-once", len(records), total)
	}
}
