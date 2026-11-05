// projection_test.go
// Testes do adapter Mongo da projeção, contra um MongoDB real em container.
// Objetivo: provar que a versão sozinha cobre repetição e desordem, sem tabela de deduplicação. Em
// ordem normal o documento avança; o mesmo evento duas vezes não muda nada nem soma tentativa; um
// evento adiantado fica guardado e é aplicado quando o anterior chega; e, o essencial, TODA ordem de
// chegada, com ou sem repetição, sequencial ou concorrente, termina no mesmo documento que a ordem
// normal, sem pending sobrando.

package mongo

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/domain"
	"github.com/VictorM0nteiro/payrouter/internal/testutil/mongotest"
)

var (
	testURL  string
	setupErr error
)

func TestMain(m *testing.M) {
	var stop func()
	testURL, stop, setupErr = mongotest.Start(context.Background())
	code := m.Run()
	if stop != nil {
		stop()
	}
	os.Exit(code)
}

var seq int

// newStore devolve uma store sobre um banco novo, para os testes não se enxergarem.
func newStore(t *testing.T) *ProjectionStore {
	t.Helper()
	if setupErr != nil {
		t.Fatalf("infraestrutura de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
	seq++
	s, err := Connect(context.Background(), testURL, fmt.Sprintf("t%d", seq))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	if err := s.EnsureIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

var at = time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)

// patch devolve o evento da versão v do histórico de teste:
// 1 accepted, 2 attempted, 3 throttled (volta a accepted), 4 attempted, 5 rejected.
func patch(id string, v int) app.Patch {
	p := app.Patch{AggregateID: id, Version: v, OccurredAt: at.Add(time.Duration(v) * time.Second)}
	switch v {
	case 1:
		p.Status = domain.StatusAccepted
		p.Instruction = &app.InstructionData{AmountCents: 15000, Debtor: "a", Creditor: "b", EndToEndID: "e1"}
	case 2, 4:
		p.Status, p.AddAttempt = domain.StatusInFlight, true
	case 3:
		p.Status = domain.StatusAccepted
	case 5:
		p.Status, p.RejectCode = domain.StatusRejected, "AC04"
	}
	return p
}

const histLen = 5

func apply(t *testing.T, s *ProjectionStore, p app.Patch) (app.ProjectionOutcome, int) {
	t.Helper()
	out, drained, err := s.Apply(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	return out, drained
}

func get(t *testing.T, s *ProjectionStore, id string) Instruction {
	t.Helper()
	doc, found, err := s.Get(context.Background(), id)
	if err != nil || !found {
		t.Fatalf("Get(%s) = found %v, err %v", id, found, err)
	}
	if len(doc.Pending) == 0 {
		doc.Pending = nil // o Mongo deixa pending: {} depois do último $unset; vazio e ausente são o mesmo
	}
	return doc
}

// want é o documento que a ordem normal produz: rejeitada na versão 5 com duas tentativas.
func want(id string) Instruction {
	return Instruction{ID: id, Status: "rejected", Version: histLen, AmountCents: 15000, Debtor: "a", Creditor: "b",
		EndToEndID: "e1", Attempts: 2, RejectCode: "AC04", UpdatedAt: at.Add(histLen * time.Second)}
}

func requireWant(t *testing.T, got Instruction, id string, context string) {
	t.Helper()
	if !reflect.DeepEqual(got, want(id)) {
		t.Fatalf("%s: documento = %+v, esperado %+v", context, got, want(id))
	}
}

func TestEmOrdemAplicaECarregaOsCamposDoEvento(t *testing.T) {
	s := newStore(t)

	for v := 1; v <= histLen; v++ {
		if out, drained := apply(t, s, patch("i-1", v)); out != app.ProjectionApplied || drained != 0 {
			t.Fatalf("versão %d: resultado %d, drenados %d, esperado Applied e 0", v, out, drained)
		}
	}

	requireWant(t, get(t, s, "i-1"), "i-1", "em ordem")
}

func TestMesmoEventoDuasVezesNaoMudaNada(t *testing.T) {
	s := newStore(t)
	apply(t, s, patch("i-1", 1))
	apply(t, s, patch("i-1", 2))
	before := get(t, s, "i-1")

	for _, v := range []int{1, 2, 1, 2} {
		if out, _ := apply(t, s, patch("i-1", v)); out != app.ProjectionDuplicate {
			t.Errorf("versão %d repetida: %d, esperado Duplicate", v, out)
		}
	}

	if after := get(t, s, "i-1"); !reflect.DeepEqual(after, before) || after.Attempts != 1 {
		t.Errorf("documento mudou: antes %+v, depois %+v (tentativas devem seguir em 1)", before, after)
	}
}

func TestEventoAdiantadoFicaGuardadoEEhAplicadoQuandoOAnteriorChega(t *testing.T) {
	s := newStore(t)
	apply(t, s, patch("i-1", 1))

	out, drained := apply(t, s, patch("i-1", 3))
	if out != app.ProjectionBuffered || drained != 0 {
		t.Fatalf("v3 com o documento em v1: %d e %d drenados, esperado Buffered e 0", out, drained)
	}
	if doc := get(t, s, "i-1"); doc.Version != 1 || len(doc.Pending) != 1 {
		t.Fatalf("depois de guardar: versão %d, pending %v; esperado versão 1 e um pending", doc.Version, doc.Pending)
	}

	out, drained = apply(t, s, patch("i-1", 2))
	if out != app.ProjectionApplied || drained != 1 {
		t.Fatalf("v2: %d e %d drenados, esperado Applied e 1 (o v3 guardado)", out, drained)
	}
	doc := get(t, s, "i-1")
	if doc.Version != 3 || doc.Attempts != 1 || len(doc.Pending) != 0 {
		t.Errorf("documento = %+v, esperado versão 3, 1 tentativa e nenhum pending", doc)
	}
}

func TestEventosAntesDaVersaoUmCriamACascaEDrenamQuandoElaChega(t *testing.T) {
	s := newStore(t)

	for _, v := range []int{3, 2} {
		if out, _ := apply(t, s, patch("i-1", v)); out != app.ProjectionBuffered {
			t.Fatalf("v%d sem v1: %d, esperado Buffered", v, out)
		}
	}
	shell := get(t, s, "i-1")
	if shell.Version != 0 || shell.Status != "" || len(shell.Pending) != 2 {
		t.Fatalf("casca = %+v, esperado versão 0, sem status e dois pending", shell)
	}

	if out, drained := apply(t, s, patch("i-1", 1)); out != app.ProjectionApplied || drained != 2 {
		t.Fatalf("v1: %d e %d drenados, esperado Applied e 2", out, drained)
	}
	if doc := get(t, s, "i-1"); doc.Version != 3 || doc.Status != "accepted" || doc.Attempts != 1 || doc.AmountCents != 15000 || len(doc.Pending) != 0 {
		t.Errorf("documento = %+v", doc)
	}
}

// permutations devolve todas as ordens possíveis de 1..n.
func permutations(n int) [][]int {
	var out [][]int
	var rec func(cur, rest []int)
	rec = func(cur, rest []int) {
		if len(rest) == 0 {
			out = append(out, append([]int(nil), cur...))
			return
		}
		for i := range rest {
			next := append(append([]int(nil), rest[:i]...), rest[i+1:]...)
			rec(append(cur, rest[i]), next)
		}
	}
	all := make([]int, n)
	for i := range all {
		all[i] = i + 1
	}
	rec(nil, all)
	return out
}

func TestTodasAsOrdensDeChegadaConvergemParaOMesmoDocumento(t *testing.T) {
	s := newStore(t)
	orders := permutations(histLen)
	if len(orders) != 120 {
		t.Fatalf("permutações = %d, esperado 120", len(orders))
	}

	for i, order := range orders {
		id := fmt.Sprintf("perm-%d", i)
		for _, v := range order {
			apply(t, s, patch(id, v))
		}
		requireWant(t, get(t, s, id), id, fmt.Sprintf("ordem %v", order))
	}
}

func TestEntregasRepetidasEEmbaralhadasConvergem(t *testing.T) {
	s := newStore(t)

	for seed := int64(1); seed <= 40; seed++ {
		rnd := rand.New(rand.NewSource(seed))
		var deliveries []int
		for v := 1; v <= histLen; v++ {
			for range 1 + rnd.Intn(3) { // cada evento chega de uma a três vezes
				deliveries = append(deliveries, v)
			}
		}
		rnd.Shuffle(len(deliveries), func(i, j int) { deliveries[i], deliveries[j] = deliveries[j], deliveries[i] })

		id := fmt.Sprintf("rep-%d", seed)
		for _, v := range deliveries {
			apply(t, s, patch(id, v))
		}
		requireWant(t, get(t, s, id), id, fmt.Sprintf("semente %d, chegadas %v", seed, deliveries))
	}
}

func TestEntregasSimultaneasEmbaralhadasConvergem(t *testing.T) {
	s := newStore(t)

	for round := range 15 {
		id := fmt.Sprintf("conc-%d", round)
		var wg sync.WaitGroup
		for worker := range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				order := rand.New(rand.NewSource(int64(round*100 + worker))).Perm(histLen)
				for _, i := range order {
					if _, _, err := s.Apply(context.Background(), patch(id, i+1)); err != nil {
						t.Error(err)
					}
				}
			}()
		}
		wg.Wait()
		requireWant(t, get(t, s, id), id, fmt.Sprintf("rodada %d", round))
	}
}

func TestEntregasSimultaneasDaMesmaVersaoAplicamUmaVezSo(t *testing.T) {
	s := newStore(t)
	apply(t, s, patch("i-1", 1))

	const n = 16
	var wg sync.WaitGroup
	results := make(chan app.ProjectionOutcome, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, _, err := s.Apply(context.Background(), patch("i-1", 2))
			if err != nil {
				t.Error(err)
			}
			results <- out
		}()
	}
	wg.Wait()
	close(results)

	counts := map[app.ProjectionOutcome]int{}
	for out := range results {
		counts[out]++
	}
	if counts[app.ProjectionApplied] != 1 || counts[app.ProjectionDuplicate] != n-1 {
		t.Errorf("resultados = %v, esperado 1 Applied e %d Duplicate", counts, n-1)
	}
	if doc := get(t, s, "i-1"); doc.Attempts != 1 {
		t.Errorf("tentativas = %d, esperado 1", doc.Attempts)
	}
}

func TestVersaoUmSemDadosDaInstrucaoEhErro(t *testing.T) {
	s := newStore(t)
	p := patch("i-1", 1)
	p.Instruction = nil

	if _, _, err := s.Apply(context.Background(), p); err == nil {
		t.Error("esperado erro: um patch da versão 1 sem dados não pode criar documento vazio")
	}
}

// Um drain interrompido por erro deixa pending esperando um anterior que JÁ foi aplicado. O próximo
// registro repetido desse anterior tem de destravá-lo, senão ele ficaria órfão para sempre.
func TestDuplicadoDestravaOPendingQueUmDrainInterrompidoDeixou(t *testing.T) {
	s := newStore(t)
	apply(t, s, patch("i-1", 1))
	apply(t, s, patch("i-1", 2))
	// Estado deixado por um drain que caiu: o documento está em v2 e o v3 continua guardado.
	pp := PendingPatch{Status: "accepted", OccurredAt: patch("i-1", 3).OccurredAt}
	if _, err := s.coll.UpdateOne(context.Background(), map[string]any{"_id": "i-1"}, map[string]any{"$set": map[string]any{"pending.3": pp}}); err != nil {
		t.Fatal(err)
	}

	out, drained := apply(t, s, patch("i-1", 2)) // o v2 chega de novo

	if out != app.ProjectionDuplicate || drained != 1 {
		t.Fatalf("v2 repetido: %d e %d drenados, esperado Duplicate e 1", out, drained)
	}
	if doc := get(t, s, "i-1"); doc.Version != 3 || len(doc.Pending) != 0 {
		t.Errorf("documento = %+v, esperado versão 3 sem pending", doc)
	}
}

// Cada evento chega UMA vez só, todos ao mesmo tempo. Sem redundância nenhuma entrega repetida pode
// compensar: se quem guarda não conferir de novo depois de guardar, e quem aplica não olhar o pending do
// documento que gravou, um evento adiantado fica órfão para sempre. É este teste que prende essa corrida.
func TestCadaEventoUmaVezEmParaleloNaoDeixaPendingOrfao(t *testing.T) {
	s := newStore(t)

	for round := range 300 {
		id := fmt.Sprintf("once-%d", round)
		apply(t, s, patch(id, 1))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for v := 2; v <= histLen; v++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, _, err := s.Apply(context.Background(), patch(id, v)); err != nil {
					t.Error(err)
				}
			}()
		}
		close(start)
		wg.Wait()
		if doc := get(t, s, id); doc.Version != histLen || len(doc.Pending) != 0 {
			t.Fatalf("rodada %d: versão %d, pending %v; esperado versão %d e nenhum pending (evento órfão)", round, doc.Version, doc.Pending, histLen)
		}
	}
}

// Um evento longe demais do documento é recusado como inválido, e nada é guardado: sem isso um registro
// corrompido faria o pending crescer sem limite.
func TestEventoLongeDemaisDoDocumentoEhRecusadoESemGuardarNada(t *testing.T) {
	s := newStore(t)
	apply(t, s, patch("i-1", 1))

	_, _, err := s.Apply(context.Background(), patch("i-1", 2+maxAhead+1))

	if !errors.Is(err, app.ErrInvalidEvent) {
		t.Fatalf("erro = %v, esperado ErrInvalidEvent", err)
	}
	if doc := get(t, s, "i-1"); len(doc.Pending) != 0 || doc.Version != 1 {
		t.Errorf("documento = %+v, esperado intocado", doc)
	}
	// No limite exato ainda é aceito.
	if out, _ := apply(t, s, patch("i-1", 2+maxAhead)); out != app.ProjectionBuffered {
		t.Errorf("no limite: %d, esperado Buffered", out)
	}
	// E sem documento nenhum, o mesmo limite vale a partir da versão zero.
	if _, _, err := s.Apply(context.Background(), patch("novo", maxAhead+2)); !errors.Is(err, app.ErrInvalidEvent) {
		t.Errorf("sem documento: erro = %v, esperado ErrInvalidEvent", err)
	}
}
