// outbox_test.go
// Testes do outbox transacional contra um Postgres real: o Append grava a linha de outbox na
// mesma transação do evento.
// Objetivo: provar a invariante 2 (todo evento tem exatamente uma linha de outbox) nos
// caminhos felizes e nos de falha: conflito de versão e rollback não deixam outbox órfão, e
// a repetição idempotente não duplica mensagem.

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

type outboxRow struct {
	Topic       string
	AggregateID string
	Envelope    map[string]any
	Published   bool
}

// outboxOf devolve as linhas de outbox do agregado na ordem de gravação.
func outboxOf(t *testing.T, id domain.InstructionID) []outboxRow {
	t.Helper()
	rows, err := testPool.Query(context.Background(),
		`SELECT topic, aggregate_id::text, payload, published_at IS NOT NULL
		 FROM outbox WHERE aggregate_id = $1::uuid ORDER BY id`, string(id))
	if err != nil {
		t.Fatalf("consultando outbox: %v", err)
	}
	defer rows.Close()
	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		var raw []byte
		if err := rows.Scan(&r.Topic, &r.AggregateID, &raw, &r.Published); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &r.Envelope); err != nil {
			t.Fatalf("envelope inválido: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestAppendGravaUmaLinhaDeOutboxPorEvento(t *testing.T) {
	store := newStore(t)
	ctx := ctxWithTimeout(t)
	id := newID(t)

	if err := store.Append(ctx, id, 0, accepted(id)); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, id, 1, domain.RoutingAttempted{}, domain.RoutingTimedOut{}); err != nil {
		t.Fatal(err)
	}

	got := outboxOf(t, id)
	if len(got) != 3 {
		t.Fatalf("linhas de outbox = %d, esperado 3", len(got))
	}
	wantTypes := []string{"InstructionAccepted", "RoutingAttempted", "RoutingTimedOut"}
	for i, r := range got {
		if r.Topic != instructionsTopic || r.AggregateID != string(id) || r.Published {
			t.Errorf("linha %d = %+v: tópico, chave ou published_at errados", i, r)
		}
		if v, _ := r.Envelope["version"].(float64); int(v) != i+1 {
			t.Errorf("linha %d: version = %v, esperado %d", i, r.Envelope["version"], i+1)
		}
		if r.Envelope["event_type"] != wantTypes[i] || r.Envelope["aggregate_id"] != string(id) {
			t.Errorf("linha %d: envelope = %v", i, r.Envelope)
		}
		if _, ok := r.Envelope["data"].(map[string]any); !ok {
			t.Errorf("linha %d: data ausente no envelope: %v", i, r.Envelope)
		}
	}
	if data := got[0].Envelope["data"].(map[string]any); data["amount_cents"] != float64(15000) {
		t.Errorf("data do evento aceito = %v", data)
	}
}

func TestConflitoDeVersaoNaoDeixaOutboxOrfao(t *testing.T) {
	store := newStore(t)
	ctx := ctxWithTimeout(t)
	id := newID(t)
	if err := store.Append(ctx, id, 0, accepted(id)); err != nil {
		t.Fatal(err)
	}

	// Versão esperada desatualizada (colide no UNIQUE) e à frente (barrada pelo WHERE).
	for _, expected := range []int{0, 9} {
		err := store.Append(ctx, id, expected, domain.RoutingAttempted{})
		if !errors.Is(err, app.ErrConcurrencyConflict) {
			t.Fatalf("expected=%d: erro = %v, esperado ErrConcurrencyConflict", expected, err)
		}
	}

	if got := len(outboxOf(t, id)); got != 1 {
		t.Errorf("linhas de outbox = %d, esperado 1 (só a do evento gravado)", got)
	}
}

func TestAppendEmLoteQueFalhaNoMeioDesfazOutboxTambem(t *testing.T) {
	store := newStore(t)
	ctx := ctxWithTimeout(t)
	id := newID(t)
	if err := store.Append(ctx, id, 0, accepted(id)); err != nil {
		t.Fatal(err)
	}
	// Outro processo avança o agregado: o lote inteiro deve falhar, sem meia gravação.
	if err := store.Append(ctx, id, 1, domain.RoutingAttempted{}); err != nil {
		t.Fatal(err)
	}

	err := store.Append(ctx, id, 1, domain.RoutingTimedOut{}, domain.RoutingAttempted{})
	if !errors.Is(err, app.ErrConcurrencyConflict) {
		t.Fatalf("erro = %v, esperado ErrConcurrencyConflict", err)
	}

	if got := len(outboxOf(t, id)); got != 2 {
		t.Errorf("linhas de outbox = %d, esperado 2", got)
	}
}

func TestUnitOfWorkQueFalhaDesfazOutboxJuntoComOEvento(t *testing.T) {
	newUseCase(t)
	uow := NewUnitOfWork(testPool)
	ctx := ctxWithTimeout(t)
	id := newID(t)
	boom := errors.New("falha depois de gravar")

	err := uow.Do(ctx, func(ctx context.Context, tx app.Tx) error {
		if err := tx.Events().Append(ctx, id, 0, accepted(id)); err != nil {
			t.Fatalf("append: %v", err)
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("erro = %v, esperado o da função", err)
	}

	if got := len(outboxOf(t, id)); got != 0 {
		t.Errorf("linhas de outbox = %d, esperado 0 depois do rollback", got)
	}
}

func TestRepeticaoIdempotenteNaoDuplicaOutbox(t *testing.T) {
	uc := newUseCase(t)
	in := uniqueInput()

	first, err := uc.Execute(ctxWithTimeout(t), in)
	if err != nil {
		t.Fatal(err)
	}
	again, err := uc.Execute(ctxWithTimeout(t), in)
	if err != nil || !again.Replayed || again.ID != first.ID {
		t.Fatalf("repetição = %+v, %v", again, err)
	}

	if got := len(outboxOf(t, first.ID)); got != 1 {
		t.Errorf("linhas de outbox = %d, esperado 1", got)
	}
}

func TestOutboxRejeitaSegundaLinhaParaOMesmoEvento(t *testing.T) {
	store := newStore(t)
	ctx := ctxWithTimeout(t)
	id := newID(t)
	if err := store.Append(ctx, id, 0, accepted(id)); err != nil {
		t.Fatal(err)
	}

	_, err := testPool.Exec(ctx,
		`INSERT INTO outbox (event_id, aggregate_id, topic, payload)
		 SELECT event_id, aggregate_id, topic, payload FROM outbox WHERE aggregate_id = $1::uuid`, string(id))

	if !isUniqueViolation(err) {
		t.Errorf("erro = %v, esperado violação do UNIQUE (event_id)", err)
	}
}
