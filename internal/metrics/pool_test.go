// pool_test.go
// Testes das métricas do pool de workers e do commit do consumidor, sem banco e sem Docker.
// Objetivo: provar que o estado do pool é lido na hora da coleta (e não congelado), que os
// redimensionamentos e os commits são contados por direção e por resultado, e que o receptor nulo
// segue sem fazer nada.

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestEstadoDoPoolELidoNaColeta(t *testing.T) {
	reg := prometheus.NewRegistry()
	size, target, ready, pending, running := 2, 2, 0, 1, 1
	RegisterPool(reg, func() (int, int, int, int, int) { return size, target, ready, pending, running })

	size, target, ready, pending, running = 6, 8, 5, 12, 6 // o estado muda entre duas coletas
	for name, want := range map[string]float64{
		"payrouter_pool_workers":        6,
		"payrouter_pool_target_workers": 8,
		"payrouter_pool_ready_tasks":    5,
		"payrouter_pool_pending_tasks":  12,
		"payrouter_pool_running_tasks":  6,
	} {
		if got := value(t, reg, name, nil); got != want {
			t.Errorf("%s = %v, esperado %v", name, got, want)
		}
	}
}

func TestRedimensionamentosEsCommitsSaoContadosPorDirecaoEResultado(t *testing.T) {
	reg := NewRegistry()
	m := NewProcessor(reg)

	m.PoolResized(1, 2)
	m.PoolResized(2, 3)
	m.PoolResized(3, 2)
	m.Commit("ok")
	m.Commit("ok")
	m.Commit("error")

	for _, c := range []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"payrouter_pool_resizes_total", map[string]string{"direction": "up"}, 2},
		{"payrouter_pool_resizes_total", map[string]string{"direction": "down"}, 1},
		{"payrouter_consumer_commits_total", map[string]string{"result": "ok"}, 2},
		{"payrouter_consumer_commits_total", map[string]string{"result": "error"}, 1},
	} {
		if got := value(t, reg, c.name, c.labels); got != c.want {
			t.Errorf("%s%v = %v, esperado %v", c.name, c.labels, got, c.want)
		}
	}
}

func TestPoolResizedECommitComReceptorNuloNaoFazemNada(t *testing.T) {
	var m *Processor

	m.PoolResized(1, 4) // não podem entrar em pânico
	m.Commit("ok")
	m.Rebalance("revoked")
}

func TestRebalancesSaoContadosPorEvento(t *testing.T) {
	reg := NewRegistry()
	m := NewProcessor(reg)

	m.Rebalance("assigned")
	m.Rebalance("revoked")
	m.Rebalance("revoked")
	m.Rebalance("lost")

	for event, want := range map[string]float64{"assigned": 1, "revoked": 2, "lost": 1} {
		if got := value(t, reg, "payrouter_consumer_rebalances_total", map[string]string{"event": event}); got != want {
			t.Errorf("rebalances{%s} = %v, esperado %v", event, got, want)
		}
	}
}
