// outbox_store_test.go
// Testes do OutboxStore (reivindicar, publicar, marcar) contra um Postgres real.
// Objetivo: provar o que sustenta o relay: só a cabeça de cada agregado é elegível (ordem por
// agregado mesmo com vários relays), SKIP LOCKED impede dois relays de pegarem a mesma linha,
// e a marcação só acontece depois de o publish dar certo (erro = rollback, tudo pendente).

package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

// bigLimit é maior que qualquer quantidade de linhas pendentes nos testes.
const bigLimit = 1000

// resetOutbox marca como publicado tudo o que outros testes deixaram pendente, para cada
// teste começar com o outbox limpo. Os testes do pacote rodam em sequência.
func resetOutbox(t *testing.T) *OutboxStore {
	t.Helper()
	if setupErr != nil {
		t.Fatalf("Postgres de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
	if _, err := testPool.Exec(context.Background(), `UPDATE outbox SET published_at = now() WHERE published_at IS NULL`); err != nil {
		t.Fatalf("limpando outbox: %v", err)
	}
	return NewOutboxStore(testPool)
}

// seed grava n eventos para o agregado, um Append por evento, e devolve o id.
func seed(t *testing.T, n int) domain.InstructionID {
	t.Helper()
	store := NewEventStore(testPool)
	ctx := ctxWithTimeout(t)
	id := newID(t)
	if err := store.Append(ctx, id, 0, accepted(id)); err != nil {
		t.Fatal(err)
	}
	for v := 1; v < n; v++ {
		if err := store.Append(ctx, id, v, domain.RoutingAttempted{}); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

// collect devolve um publish que acumula tudo o que recebe.
func collect(into *[]app.OutboxMessage) func(context.Context, []app.OutboxMessage) error {
	return func(_ context.Context, msgs []app.OutboxMessage) error {
		*into = append(*into, msgs...)
		return nil
	}
}

func TestDrainSoDevolveACabecaDeCadaAgregado(t *testing.T) {
	store := resetOutbox(t)
	ctx := ctxWithTimeout(t)
	a, b := seed(t, 3), seed(t, 2)

	var got []app.OutboxMessage
	n, err := store.Drain(ctx, bigLimit, collect(&got))
	if err != nil || n != 2 || len(got) != 2 {
		t.Fatalf("1ª rodada: n=%d err=%v msgs=%d; esperado 2 (v1 de cada)", n, err, len(got))
	}
	for _, m := range got {
		if m.Version != 1 || m.EventType != "InstructionAccepted" || m.Topic != instructionsTopic {
			t.Errorf("mensagem inesperada na 1ª rodada: %+v", m)
		}
	}
	if got[0].AggregateID != string(a) || got[1].AggregateID != string(b) {
		t.Errorf("ordem das cabeças = %s, %s; esperado a ordem de gravação (id do outbox)", got[0].AggregateID, got[1].AggregateID)
	}

	// 2ª rodada: v2 de ambos. 3ª: só a v3 de a. 4ª: nada.
	for round, want := range []int{2, 1, 0} {
		got = nil
		n, err := store.Drain(ctx, bigLimit, collect(&got))
		if err != nil || n != want {
			t.Fatalf("rodada %d: n=%d err=%v; esperado %d", round+2, n, err, want)
		}
	}
}

func TestDrainMarcaPublishedAtSoDepoisDoPublish(t *testing.T) {
	store := resetOutbox(t)
	id := seed(t, 1)

	var duranteOPublish bool
	n, err := store.Drain(ctxWithTimeout(t), bigLimit, func(ctx context.Context, _ []app.OutboxMessage) error {
		// Outra conexão ainda enxerga a linha como pendente: a marcação não veio antes.
		return testPool.QueryRow(ctx,
			`SELECT published_at IS NULL FROM outbox WHERE aggregate_id = $1::uuid`, string(id)).Scan(&duranteOPublish)
	})
	if err != nil || n != 1 {
		t.Fatalf("Drain = %d, %v", n, err)
	}
	if !duranteOPublish {
		t.Error("a linha estava marcada como publicada antes de o publish terminar")
	}
	if rows := outboxOf(t, id); len(rows) != 1 || !rows[0].Published {
		t.Errorf("depois do Drain: %+v, esperado published_at preenchido", rows)
	}
}

func TestDrainComPublishQueFalhaFazRollbackETudoContinuaPendente(t *testing.T) {
	store := resetOutbox(t)
	id := seed(t, 2)
	boom := errors.New("broker fora do ar")

	n, err := store.Drain(ctxWithTimeout(t), bigLimit, func(context.Context, []app.OutboxMessage) error { return boom })

	if !errors.Is(err, boom) || n != 0 {
		t.Fatalf("Drain = %d, %v; esperado 0 e o erro do publish", n, err)
	}
	for i, r := range outboxOf(t, id) {
		if r.Published {
			t.Errorf("linha %d marcada como publicada apesar da falha", i)
		}
	}
	// E a mesma cabeça volta a ser elegível na próxima rodada.
	var got []app.OutboxMessage
	if n, err := store.Drain(ctxWithTimeout(t), bigLimit, collect(&got)); err != nil || n != 1 || got[0].Version != 1 {
		t.Errorf("nova rodada = %d, %v, %+v; esperado a v1 de novo", n, err, got)
	}
}

func TestDrainConcorrenteNuncaPegaAMesmaLinhaDuasVezes(t *testing.T) {
	store := resetOutbox(t)
	const aggregates = 24
	want := make(map[string]bool)
	for range aggregates {
		want[string(seed(t, 1))] = true
	}

	var mu sync.Mutex
	seen := make(map[int64]int)
	publish := func(_ context.Context, msgs []app.OutboxMessage) error {
		time.Sleep(40 * time.Millisecond) // segura os locks para forçar a sobreposição
		mu.Lock()
		defer mu.Unlock()
		for _, m := range msgs {
			seen[m.ID]++
		}
		return nil
	}

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n, err := store.Drain(ctxWithTimeout(t), 6, publish)
				if err != nil {
					t.Errorf("Drain: %v", err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	if len(seen) != aggregates {
		t.Errorf("linhas publicadas = %d, esperado %d", len(seen), aggregates)
	}
	for id, times := range seen {
		if times != 1 {
			t.Errorf("linha %d publicada %d vezes, esperado 1", id, times)
		}
	}
}

func TestCabecaTravadaPorOutroRelayNaoLiberaASeguinte(t *testing.T) {
	store := resetOutbox(t)
	id := seed(t, 2)

	holding := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := store.Drain(ctxWithTimeout(t), bigLimit, func(context.Context, []app.OutboxMessage) error {
			close(holding) // este "relay" segura a v1 travada
			<-release
			return nil
		})
		firstDone <- err
	}()
	<-holding

	// O segundo relay não pode pegar a v1 (travada) nem pular para a v2 (reordenaria).
	var got []app.OutboxMessage
	n, err := store.Drain(ctxWithTimeout(t), bigLimit, collect(&got))
	if err != nil || n != 0 {
		t.Fatalf("2º relay: n=%d err=%v msgs=%+v; esperado nada enquanto a cabeça está travada", n, err, got)
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	n, err = store.Drain(ctxWithTimeout(t), bigLimit, collect(&got))
	if err != nil || n != 1 || got[0].Version != 2 || got[0].AggregateID != string(id) {
		t.Errorf("depois de liberar: n=%d err=%v msgs=%+v; esperado a v2", n, err, got)
	}
}
