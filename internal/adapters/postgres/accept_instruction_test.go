// accept_instruction_test.go
// Testes de integração do caso de uso AcceptInstruction e da UnitOfWork contra um Postgres
// real. Ficam neste pacote para reaproveitar o container do TestMain.
// Objetivo: provar o contrato de idempotência (repetição devolve a mesma resposta, corpo
// diferente é conflito, escopos são independentes, entrada inválida não consome a chave), a
// atomicidade entre evento e chave, e que 32 requisições simultâneas com a mesma chave criam
// exatamente uma instrução.

package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
)

func newUseCase(t *testing.T) *app.AcceptInstruction {
	t.Helper()
	if setupErr != nil {
		t.Fatalf("Postgres de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
	return app.NewAcceptInstruction(NewUnitOfWork(testPool), func() domain.InstructionID {
		return domain.InstructionID(uuid.NewString())
	})
}

// uniqueInput gera um pedido com escopo, chave e end-to-end únicos, para os testes não
// colidirem entre si no mesmo banco.
func uniqueInput() app.AcceptInstructionInput {
	return app.AcceptInstructionInput{
		Scope:          "cliente-" + uuid.NewString(),
		IdempotencyKey: "key-" + uuid.NewString(),
		Debtor:         "d",
		Creditor:       "c",
		AmountCents:    15000,
		EndToEndID:     "e2e-" + uuid.NewString(),
	}
}

func countAccepted(t *testing.T, endToEndID string) int {
	t.Helper()
	var n int
	err := testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM events WHERE event_type = 'InstructionAccepted' AND payload->>'end_to_end_id' = $1`,
		endToEndID).Scan(&n)
	if err != nil {
		t.Fatalf("contando eventos: %v", err)
	}
	return n
}

func countKeys(t *testing.T, scope string) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM idempotency_keys WHERE scope = $1`, scope).Scan(&n); err != nil {
		t.Fatalf("contando chaves: %v", err)
	}
	return n
}

func TestAcceptRequisicaoNovaCriaInstrucaoEChave(t *testing.T) {
	uc := newUseCase(t)
	in := uniqueInput()

	res, err := uc.Execute(ctxWithTimeout(t), in)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Replayed || res.Status != domain.StatusAccepted || res.ID == "" {
		t.Fatalf("resultado = %+v, esperado nova instrução accepted", res)
	}

	events, err := NewEventStore(testPool).Load(ctxWithTimeout(t), res.ID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	p, err := domain.Rehydrate(events)
	if err != nil || p.Status() != domain.StatusAccepted || p.Version() != 1 {
		t.Fatalf("agregado = %v/%v, erro %v", p, events, err)
	}
	if got := countKeys(t, in.Scope); got != 1 {
		t.Errorf("chaves gravadas = %d, esperado 1", got)
	}
}

func TestAcceptMesmaChaveMesmoCorpoDevolveAMesmaResposta(t *testing.T) {
	uc := newUseCase(t)
	in := uniqueInput()

	first, err := uc.Execute(ctxWithTimeout(t), in)
	if err != nil {
		t.Fatalf("primeira: %v", err)
	}
	second, err := uc.Execute(ctxWithTimeout(t), in)
	if err != nil {
		t.Fatalf("segunda: %v", err)
	}

	if second.ID != first.ID || second.Status != first.Status {
		t.Errorf("repetição devolveu %+v, esperado o mesmo que %+v", second, first)
	}
	if !second.Replayed || first.Replayed {
		t.Errorf("Replayed: primeira=%v segunda=%v, esperado false e true", first.Replayed, second.Replayed)
	}
	if got := countAccepted(t, in.EndToEndID); got != 1 {
		t.Errorf("instruções criadas = %d, esperado 1", got)
	}
}

func TestAcceptMesmaChaveCorpoDiferenteEConflito(t *testing.T) {
	uc := newUseCase(t)
	in := uniqueInput()
	if _, err := uc.Execute(ctxWithTimeout(t), in); err != nil {
		t.Fatalf("primeira: %v", err)
	}

	changed := in
	changed.AmountCents = 99999
	_, err := uc.Execute(ctxWithTimeout(t), changed)
	if !errors.Is(err, app.ErrIdempotencyKeyReuse) {
		t.Fatalf("erro = %v, esperado ErrIdempotencyKeyReuse", err)
	}
	if got := countAccepted(t, in.EndToEndID); got != 1 {
		t.Errorf("instruções criadas = %d, esperado 1", got)
	}
}

func TestAcceptEscoposDiferentesSaoIndependentes(t *testing.T) {
	uc := newUseCase(t)
	a := uniqueInput()
	b := a
	b.Scope = "outro-" + uuid.NewString()

	resA, err := uc.Execute(ctxWithTimeout(t), a)
	if err != nil {
		t.Fatalf("a: %v", err)
	}
	resB, err := uc.Execute(ctxWithTimeout(t), b)
	if err != nil {
		t.Fatalf("b: %v", err)
	}
	if resA.ID == resB.ID || resB.Replayed {
		t.Errorf("a mesma chave em escopos diferentes deveria criar duas instruções: %+v e %+v", resA, resB)
	}
}

func TestAcceptEntradaInvalidaNaoConsomeAChave(t *testing.T) {
	uc := newUseCase(t)
	in := uniqueInput()
	in.AmountCents = 0

	if _, err := uc.Execute(ctxWithTimeout(t), in); !errors.Is(err, domain.ErrInvalidAmount) {
		t.Fatalf("erro = %v, esperado ErrInvalidAmount", err)
	}
	if got := countKeys(t, in.Scope); got != 0 {
		t.Fatalf("chaves gravadas = %d: a requisição inválida não pode consumir a chave", got)
	}

	// O cliente corrige o valor e reusa a mesma chave: tem que funcionar.
	in.AmountCents = 15000
	res, err := uc.Execute(ctxWithTimeout(t), in)
	if err != nil || res.Replayed {
		t.Fatalf("depois de corrigir: res=%+v err=%v", res, err)
	}
}

func TestAcceptMesmaChaveSimultaneaCriaExatamenteUmaInstrucao(t *testing.T) {
	uc := newUseCase(t)
	in := uniqueInput()

	start := make(chan struct{})
	results := make([]app.AcceptInstructionResult, contenders)
	errs := make([]error, contenders)
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = uc.Execute(ctxWithTimeout(t), in)
		}()
	}
	close(start)
	wg.Wait()

	replayed := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
		if results[i].ID != results[0].ID {
			t.Errorf("goroutine %d devolveu id %s, esperado %s", i, results[i].ID, results[0].ID)
		}
		if results[i].Replayed {
			replayed++
		}
	}
	if replayed != contenders-1 {
		t.Errorf("repetições = %d, esperado %d (exatamente uma criou)", replayed, contenders-1)
	}
	if got := countAccepted(t, in.EndToEndID); got != 1 {
		t.Errorf("instruções criadas = %d, esperado 1", got)
	}
	if got := countKeys(t, in.Scope); got != 1 {
		t.Errorf("chaves gravadas = %d, esperado 1", got)
	}
}

func TestUnitOfWorkDesfazEventoEChaveQuandoAFuncaoFalha(t *testing.T) {
	newUseCase(t) // garante o container de pé
	uow := NewUnitOfWork(testPool)
	ctx := ctxWithTimeout(t)
	id := newID(t)
	key := app.IdempotencyKey{Scope: "s-" + uuid.NewString(), Endpoint: "POST /x", Key: "k"}
	boom := errors.New("falha depois de gravar")

	err := uow.Do(ctx, func(ctx context.Context, tx app.Tx) error {
		if _, claimed, err := tx.Idempotency().Claim(ctx, key, "fp"); err != nil || !claimed {
			t.Fatalf("claim: claimed=%v err=%v", claimed, err)
		}
		if err := tx.Events().Append(ctx, id, 0, accepted(id)); err != nil {
			t.Fatalf("append: %v", err)
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("erro = %v, esperado o da função", err)
	}

	if _, err := NewEventStore(testPool).Load(ctx, id); !errors.Is(err, app.ErrNotFound) {
		t.Errorf("o evento deveria ter sido desfeito, load = %v", err)
	}
	if got := countKeys(t, key.Scope); got != 0 {
		t.Errorf("a chave deveria ter sido desfeita, chaves = %d", got)
	}
}

func TestConflitoDentroDeTransacaoNaoEnvenenaATransacao(t *testing.T) {
	newUseCase(t)
	uow := NewUnitOfWork(testPool)
	ctx := ctxWithTimeout(t)
	id := newID(t)

	err := uow.Do(ctx, func(ctx context.Context, tx app.Tx) error {
		events := tx.Events()
		if err := events.Append(ctx, id, 0, accepted(id)); err != nil {
			return err
		}
		// Conflito proposital: o savepoint desfaz só este Append.
		if err := events.Append(ctx, id, 0, domain.RoutingAttempted{}); !errors.Is(err, app.ErrConcurrencyConflict) {
			t.Fatalf("erro = %v, esperado ErrConcurrencyConflict", err)
		}
		// A transação continua viva e o próximo Append correto funciona.
		return events.Append(ctx, id, 1, domain.RoutingAttempted{})
	})
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	events, err := NewEventStore(testPool).Load(ctx, id)
	if err != nil || len(events) != 2 {
		t.Fatalf("eventos = %d, erro %v, esperado 2", len(events), err)
	}
}
