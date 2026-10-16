// concurrency_test.go
// Testes de concorrência otimista do EventStore: N goroutines disputando o mesmo agregado
// contra um Postgres real.
// Objetivo: provar a tese do ADR 4. Sem lock de linha e sem SERIALIZABLE, a versão esperada
// garante que, de N comandos que leram a mesma versão, exatamente um vence e os outros
// recebem ErrConcurrencyConflict; e que, com releitura e nova tentativa, nenhuma escrita se
// perde. Devem passar sob `go test -race` vinte vezes seguidas sem flake.

package postgres

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

// contenders é o número de goroutines disputando o mesmo agregado.
const contenders = 32

func TestConcorrenciaExatamenteUmVence(t *testing.T) {
	store := newStore(t)
	ctx := ctxWithTimeout(t)
	id := newID(t)

	if err := store.Append(ctx, id, 0, accepted(id)); err != nil {
		t.Fatalf("append v1: %v", err)
	}

	// Todas leram "versão 1" e tentam gravar a versão 2 ao mesmo tempo. O canal start
	// solta todas juntas, para a disputa acontecer no banco e não na criação das goroutines.
	start := make(chan struct{})
	errs := make([]error, contenders)
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = store.Append(ctx, id, 1, domain.ParticipantRejected{Code: fmt.Sprintf("G%d", i)})
		}()
	}
	close(start)
	wg.Wait()

	winner := -1
	conflicts := 0
	for i, err := range errs {
		switch {
		case err == nil:
			if winner != -1 {
				t.Errorf("mais de um vencedor: goroutines %d e %d", winner, i)
			}
			winner = i
		case errors.Is(err, app.ErrConcurrencyConflict):
			conflicts++
		default:
			t.Errorf("goroutine %d: erro inesperado: %v", i, err)
		}
	}
	if winner == -1 {
		t.Fatal("nenhuma goroutine venceu")
	}
	if conflicts != contenders-1 {
		t.Errorf("conflitos = %d, esperado %d", conflicts, contenders-1)
	}

	// O que ficou gravado é exatamente a escrita do vencedor, e só ela.
	events, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("eventos gravados = %d, esperado 2", len(events))
	}
	want := domain.ParticipantRejected{Code: fmt.Sprintf("G%d", winner)}
	if events[1] != want {
		t.Errorf("evento v2 = %#v, esperado %#v", events[1], want)
	}
	assertVersoesContiguas(t, id, 2)
}

func TestConcorrenciaComRetryNaoPerdeNenhumaEscrita(t *testing.T) {
	store := newStore(t)
	ctx := ctxWithTimeout(t)
	id := newID(t)

	// Leva o agregado até unknown (v3), estado em que StatusQueried é legal repetidamente.
	if err := store.Append(ctx, id, 0, accepted(id), domain.RoutingAttempted{}, domain.RoutingTimedOut{}); err != nil {
		t.Fatalf("preparando agregado: %v", err)
	}

	start := make(chan struct{})
	errs := make([]error, contenders)
	retries := make([]int, contenders)
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			retries[i], errs[i] = queryComRetry(t, store, id)
		}()
	}
	close(start)
	wg.Wait()

	totalRetries := 0
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
		totalRetries += retries[i]
	}
	t.Logf("%d goroutines, %d retentativas no total", contenders, totalRetries)

	// Nenhuma escrita perdida: as 32 consultas estão lá, em versões 4..35, sem buraco.
	events, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	p, err := domain.Rehydrate(events)
	if err != nil {
		t.Fatalf("rehydrate: %v", err)
	}
	if want := 3 + contenders; p.Version() != want {
		t.Errorf("versão final = %d, esperada %d", p.Version(), want)
	}
	queries := 0
	for _, e := range events {
		if e.Type() == domain.EventStatusQueried {
			queries++
		}
	}
	if queries != contenders {
		t.Errorf("StatusQueried gravados = %d, esperado %d", queries, contenders)
	}
	assertVersoesContiguas(t, id, 3+contenders)
}

// queryComRetry é o ciclo de um comando real sob concorrência otimista: lê o histórico,
// reconstrói o agregado, decide, tenta gravar com a versão lida; se perder a corrida,
// relê e repete. Devolve quantas vezes precisou repetir.
func queryComRetry(t *testing.T, store *EventStore, id domain.InstructionID) (int, error) {
	t.Helper()
	const maxAttempts = 200
	for attempt := 0; attempt < maxAttempts; attempt++ {
		ctx := ctxWithTimeout(t)
		events, err := store.Load(ctx, id)
		if err != nil {
			return attempt, err
		}
		p, err := domain.Rehydrate(events)
		if err != nil {
			return attempt, err
		}
		// A versão esperada é a lida ANTES do comando: o comando aplica o evento no próprio
		// agregado, então depois dele p.Version() já é a versão nova.
		expected := p.Version()
		ev, err := p.RecordStatusQuery(domain.QueryUnknown)
		if err != nil {
			return attempt, err
		}
		err = store.Append(ctx, id, expected, ev)
		if err == nil {
			return attempt, nil
		}
		if !errors.Is(err, app.ErrConcurrencyConflict) {
			return attempt, err
		}
	}
	return maxAttempts, fmt.Errorf("sem sucesso após %d tentativas", maxAttempts)
}

// assertVersoesContiguas confere que o agregado tem as versões 1..want, sem repetição
// nem buraco.
func assertVersoesContiguas(t *testing.T, id domain.InstructionID, want int) {
	t.Helper()
	rows, err := testPool.Query(ctxWithTimeout(t),
		`SELECT version FROM events WHERE aggregate_id = $1::uuid ORDER BY version`, string(id))
	if err != nil {
		t.Fatalf("consultando versões: %v", err)
	}
	defer rows.Close()

	expected := 1
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		if v != expected {
			t.Fatalf("versão %d onde se esperava %d: histórico com buraco ou repetição", v, expected)
		}
		expected++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if expected-1 != want {
		t.Fatalf("versões gravadas = %d, esperado %d", expected-1, want)
	}
}
