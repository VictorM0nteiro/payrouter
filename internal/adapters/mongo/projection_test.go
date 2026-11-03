// projection_test.go
// Testes do adapter Mongo da projeção, contra um MongoDB real em container.
// Objetivo: provar a idempotência pela versão sem tabela de deduplicação: a ordem normal aplica,
// o mesmo evento duas vezes não muda nada (nem soma tentativa), um evento adiantado vira Gap e
// passa a aplicar quando o anterior chega, e duas entregas simultâneas da mesma versão aplicam
// exatamente uma vez.

package mongo

import (
	"context"
	"fmt"
	"os"
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

func accepted() app.Patch {
	return app.Patch{AggregateID: "i-1", Version: 1, OccurredAt: at, Status: domain.StatusAccepted,
		Instruction: &app.InstructionData{AmountCents: 15000, Debtor: "a", Creditor: "b", EndToEndID: "e1"}}
}

func attempted(v int) app.Patch {
	return app.Patch{AggregateID: "i-1", Version: v, OccurredAt: at.Add(time.Duration(v) * time.Second), Status: domain.StatusInFlight, AddAttempt: true}
}

func apply(t *testing.T, s *ProjectionStore, p app.Patch) app.ProjectionOutcome {
	t.Helper()
	out, err := s.Apply(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestEmOrdemAplicaECarregaOsCamposDoEvento(t *testing.T) {
	s := newStore(t)

	for _, p := range []app.Patch{
		accepted(),
		attempted(2),
		{AggregateID: "i-1", Version: 3, OccurredAt: at.Add(3 * time.Second), Status: domain.StatusRejected, RejectCode: "AC04"},
	} {
		if out := apply(t, s, p); out != app.ProjectionApplied {
			t.Fatalf("versão %d: resultado %d, esperado Applied", p.Version, out)
		}
	}

	doc, found, err := s.Get(context.Background(), "i-1")
	if err != nil || !found {
		t.Fatalf("Get = %v, %v", found, err)
	}
	if doc.Status != "rejected" || doc.Version != 3 || doc.Attempts != 1 || doc.AmountCents != 15000 ||
		doc.RejectCode != "AC04" || doc.Debtor != "a" || !doc.UpdatedAt.Equal(at.Add(3*time.Second)) {
		t.Errorf("documento = %+v", doc)
	}
}

func TestMesmoEventoDuasVezesNaoMudaNada(t *testing.T) {
	s := newStore(t)
	apply(t, s, accepted())
	apply(t, s, attempted(2))
	before, _, _ := s.Get(context.Background(), "i-1")

	if out := apply(t, s, accepted()); out != app.ProjectionDuplicate {
		t.Errorf("versão 1 repetida: %d, esperado Duplicate", out)
	}
	if out := apply(t, s, attempted(2)); out != app.ProjectionDuplicate {
		t.Errorf("versão 2 repetida: %d, esperado Duplicate", out)
	}

	after, _, _ := s.Get(context.Background(), "i-1")
	if after != before || after.Attempts != 1 {
		t.Errorf("documento mudou: antes %+v, depois %+v (tentativas devem seguir em 1)", before, after)
	}
}

func TestEventoAdiantadoViraGapEAplicaQuandoOAnteriorChega(t *testing.T) {
	s := newStore(t)

	if out := apply(t, s, attempted(2)); out != app.ProjectionGap {
		t.Errorf("v2 sem v1: %d, esperado Gap", out)
	}
	apply(t, s, accepted())
	if out := apply(t, s, attempted(3)); out != app.ProjectionGap {
		t.Errorf("v3 com o documento em v1: %d, esperado Gap", out)
	}
	if out := apply(t, s, attempted(2)); out != app.ProjectionApplied {
		t.Errorf("v2: %d, esperado Applied", out)
	}
	if out := apply(t, s, attempted(3)); out != app.ProjectionApplied {
		t.Errorf("v3 repetida depois do buraco fechar: %d, esperado Applied", out)
	}

	doc, _, _ := s.Get(context.Background(), "i-1")
	if doc.Version != 3 || doc.Attempts != 2 {
		t.Errorf("documento = %+v, esperado versão 3 e 2 tentativas", doc)
	}
}

func TestEntregasSimultaneasDaMesmaVersaoAplicamUmaVezSo(t *testing.T) {
	s := newStore(t)
	apply(t, s, accepted())

	const n = 16
	var wg sync.WaitGroup
	results := make(chan app.ProjectionOutcome, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := s.Apply(context.Background(), attempted(2))
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
	if doc, _, _ := s.Get(context.Background(), "i-1"); doc.Attempts != 1 {
		t.Errorf("tentativas = %d, esperado 1", doc.Attempts)
	}
}

func TestVersaoUmSemDadosDaInstrucaoEhErro(t *testing.T) {
	s := newStore(t)
	p := accepted()
	p.Instruction = nil

	if _, err := s.Apply(context.Background(), p); err == nil {
		t.Error("esperado erro: um patch da versão 1 sem dados não pode criar documento vazio")
	}
}
