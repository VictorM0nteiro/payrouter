// limiter_test.go
// Testes das métricas do limitador adaptativo, sem banco e sem Docker.
// Objetivo: provar que as mudanças do limite são contadas por direção (up/down), que o estado do
// limitador (limite, em voo, esperando) é lido na hora da coleta e não congelado, e que o receptor
// nulo segue sem fazer nada.

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMudancasDoLimiteSaoContadasPorDirecao(t *testing.T) {
	reg := NewRegistry()
	m := NewProcessor(reg)

	m.LimitChanged(4, 5)
	m.LimitChanged(8, 4)
	m.LimitChanged(4, 2)

	if got := value(t, reg, "payrouter_participant_limit_changes_total", map[string]string{"direction": "up"}); got != 1 {
		t.Errorf("up = %v, esperado 1", got)
	}
	if got := value(t, reg, "payrouter_participant_limit_changes_total", map[string]string{"direction": "down"}); got != 2 {
		t.Errorf("down = %v, esperado 2", got)
	}
}

func TestEstadoDoLimitadorELidoNaColeta(t *testing.T) {
	reg := prometheus.NewRegistry()
	limit, inflight, waiting := 4, 1, 0
	RegisterLimiter(reg, func() (int, int, int) { return limit, inflight, waiting })

	if got := value(t, reg, "payrouter_participant_limit", nil); got != 4 {
		t.Errorf("limite = %v, esperado 4", got)
	}
	limit, inflight, waiting = 2, 2, 5 // o estado muda entre duas coletas
	for name, want := range map[string]float64{
		"payrouter_participant_limit":         2,
		"payrouter_participant_inflight":      2,
		"payrouter_participant_limit_waiting": 5,
	} {
		if got := value(t, reg, name, nil); got != want {
			t.Errorf("%s = %v, esperado %v", name, got, want)
		}
	}
}

func TestLimitChangedComReceptorNuloNaoFazNada(t *testing.T) {
	var m *Processor

	m.LimitChanged(8, 4) // não pode entrar em pânico
}
