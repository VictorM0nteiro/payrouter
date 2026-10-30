// backpressure_test.go
// Testes da contrapressão do Consumer contra um Redpanda real (testcontainers).
// Objetivo: provar que, com o limite de saída cheio (app.BackpressureError), o consumidor pausa a
// partição, espera e repete a MESMA mensagem no lugar: nada é pulado nem descartado, a ordem
// vale, e o offset só avança depois do sucesso. E que parar durante a espera não confirma o
// offset (a mensagem volta para quem assumir o grupo) nem deixa a partição pausada.

package kafka

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
)

func TestContrapressaoRepeteAMesmaMensagemNoLugarSemPularNenhuma(t *testing.T) {
	topic := newTopic(t, 1)
	publish(t, topic, "agg-bp", 1, 2, 3)

	var rec recorder
	var waits atomic.Int32
	start := time.Now()
	stop := run(t, group(t), topic, func(_ context.Context, m app.Message) error {
		// A mensagem 2 encontra o limite cheio duas vezes antes de passar.
		if m.Version == 2 && waits.Add(1) <= 2 {
			return &app.BackpressureError{After: 150 * time.Millisecond}
		}
		rec.add(m)
		return nil
	})
	waitFor(t, "3 mensagens", func() bool { return len(rec.versions()) == 3 })
	elapsed := time.Since(start)
	stop()

	if got := fmt.Sprint(rec.versions()); got != "[1 2 3]" {
		t.Errorf("ordem = %s, esperado [1 2 3]: a espera não pode pular nem reordenar", got)
	}
	if elapsed < 300*time.Millisecond {
		t.Errorf("terminou em %v, esperado >= 300ms (duas esperas de 150ms)", elapsed)
	}
}

func TestPararDuranteAEsperaNaoConfirmaOOffsetEAMensagemVoltaParaOGrupo(t *testing.T) {
	topic := newTopic(t, 1)
	publish(t, topic, "agg-bp2", 1, 2)
	g := group(t)

	var first recorder
	var blocked atomic.Bool
	stop := run(t, g, topic, func(_ context.Context, m app.Message) error {
		if m.Version == 2 {
			blocked.Store(true)
			return &app.BackpressureError{After: time.Hour} // nunca passa: o consumidor é parado
		}
		first.add(m)
		return nil
	})
	waitFor(t, "mensagem 2 presa na espera", blocked.Load)
	began := time.Now()
	stop()
	if d := time.Since(began); d > 5*time.Second {
		t.Errorf("parar durante a espera levou %v, esperado quase imediato", d)
	}

	// Quem assume o grupo recebe a mensagem 2 (o offset dela não foi confirmado) e a 1 não volta.
	var second recorder
	stop = run(t, g, topic, func(_ context.Context, m app.Message) error { second.add(m); return nil })
	waitFor(t, "mensagem 2 reentregue", func() bool { return len(second.versions()) >= 1 })
	stop()

	if got := fmt.Sprint(second.versions()); got != "[2]" {
		t.Errorf("reentregues = %s, esperado [2]: a 1 foi confirmada, a 2 ficou pendente", got)
	}
	if got := fmt.Sprint(first.versions()); got != "[1]" {
		t.Errorf("primeiro consumidor viu %s, esperado [1]", got)
	}
}
