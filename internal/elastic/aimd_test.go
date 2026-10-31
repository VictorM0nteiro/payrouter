// aimd_test.go
// Testes da regra AIMD, determinísticos: sem goroutines, sem relógio.
// Objetivo: provar que sucesso soma devagar (1/limite), que 429 e timeout dividem o limite pelo
// mesmo fator, que o piso e o teto valem e que o limite volta a subir depois de cair.

package elastic

import "testing"

func testRule() aimd { return aimd{min: 1, max: 16, beta: 0.5} }

func TestSucessoSomaUmSobreOLimite(t *testing.T) {
	got := testRule().next(4, Success)

	if got != 4.25 {
		t.Errorf("next(4, Success) = %v, esperado 4.25 (soma 1/limite)", got)
	}
}

func TestUmaJanelaDeSucessosSomaCercaDeUm(t *testing.T) {
	r := testRule()
	limit := 4.0

	for range 4 { // uma janela = "limite" respostas
		limit = r.next(limit, Success)
	}

	if limit < 4.8 || limit > 5.1 {
		t.Errorf("depois de 4 sucessos o limite é %v, esperado perto de 5", limit)
	}
}

func TestThrottledETimeoutDividemOLimiteIgual(t *testing.T) {
	r := testRule()

	if a, b := r.next(8, Throttled), r.next(8, Timeout); a != 4 || b != 4 {
		t.Errorf("next(8, Throttled) = %v e next(8, Timeout) = %v, esperado 4 nos dois", a, b)
	}
}

func TestRespeitaOPisoEOTetoDoAIMD(t *testing.T) {
	r := testRule()

	if got := r.next(1.5, Timeout); got != 1 {
		t.Errorf("abaixo do piso: %v, esperado 1", got)
	}
	if got := r.next(16, Success); got != 16 {
		t.Errorf("acima do teto: %v, esperado 16", got)
	}
}

func TestDepoisDeCairOLimiteVoltaASubir(t *testing.T) {
	r := testRule()
	limit := r.next(16, Timeout)

	for range 500 {
		limit = r.next(limit, Success)
	}

	if limit != 16 {
		t.Errorf("limite %v depois de muitos sucessos, esperado voltar ao teto 16", limit)
	}
}
