// consumer_test.go
// Testes do Consumer contra um Redpanda real (testcontainers).
// Objetivo: provar a política de commit. Mensagem concluída tem o offset confirmado e não
// volta; erro transitório repete a MESMA mensagem, no lugar, sem pular nenhuma; resultado
// desconhecido (ErrOutcomeUnknown) avança o offset, porque reenviar é perigoso; mensagem sem
// headers é descartada e não trava a fila; e, sem commit (consumidor fechado antes de
// concluir), a mensagem é entregue de novo a quem assumir o grupo.

package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/twmb/franz-go/pkg/kgo"
)

// publish grava mensagens de um agregado, na ordem, com os headers do relay.
func publish(t *testing.T, topic, aggregate string, versions ...int) {
	t.Helper()
	p, err := NewProducer(testBrokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, v := range versions {
		err := p.Publish(context.Background(), []app.OutboxMessage{
			{AggregateID: aggregate, Topic: topic, EventType: "InstructionAccepted", Version: v, Payload: []byte(`{}`)},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// run inicia o consumidor e devolve a função que o encerra e espera o Run voltar.
func run(t *testing.T, group, topic string, h Handler) (stop func()) {
	t.Helper()
	c, err := NewConsumer(testBrokers, group, topic)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, h) }()
	return func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
		c.Close()
	}
}

// recorder guarda as mensagens vistas, em ordem.
type recorder struct {
	mu   sync.Mutex
	seen []app.Message
}

func (r *recorder) add(m app.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, m)
}

func (r *recorder) versions() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int, len(r.seen))
	for i, m := range r.seen {
		out[i] = m.Version
	}
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout esperando: %s", what)
}

func group(t *testing.T) string { return fmt.Sprintf("g-%s-%d", t.Name(), time.Now().UnixNano()) }

func TestMensagemConcluidaTemOOffsetConfirmadoENaoVolta(t *testing.T) {
	topic := newTopic(t, 1)
	publish(t, topic, "agg-1", 1, 2, 3)
	g := group(t)

	var rec recorder
	stop := run(t, g, topic, func(_ context.Context, m app.Message) error { rec.add(m); return nil })
	waitFor(t, "3 mensagens", func() bool { return len(rec.versions()) == 3 })
	stop()

	// Um consumidor novo no mesmo grupo recomeça do offset confirmado: nada a ler.
	var again recorder
	stop = run(t, g, topic, func(_ context.Context, m app.Message) error { again.add(m); return nil })
	time.Sleep(3 * time.Second)
	stop()
	if got := again.versions(); len(got) != 0 {
		t.Errorf("mensagens reentregues = %v, esperado nenhuma", got)
	}
	if got := rec.versions(); fmt.Sprint(got) != "[1 2 3]" {
		t.Errorf("ordem = %v, esperado [1 2 3]", got)
	}
}

func TestErroTransitorioRepeteAMesmaMensagemSemPularNenhuma(t *testing.T) {
	topic := newTopic(t, 1)
	publish(t, topic, "agg-1", 1, 2)

	var rec recorder
	var failures int
	stop := run(t, group(t), topic, func(_ context.Context, m app.Message) error {
		rec.add(m)
		if m.Version == 1 && failures < 2 { // a primeira falha duas vezes (banco fora, por exemplo)
			failures++
			return errors.New("banco indisponível")
		}
		return nil
	})
	defer stop()

	waitFor(t, "v1 três vezes e v2 uma", func() bool { return fmt.Sprint(rec.versions()) == "[1 1 1 2]" })
}

func TestResultadoDesconhecidoAvancaOOffset(t *testing.T) {
	topic := newTopic(t, 1)
	publish(t, topic, "agg-1", 1, 2)
	g := group(t)

	var rec recorder
	stop := run(t, g, topic, func(_ context.Context, m app.Message) error {
		rec.add(m)
		if m.Version == 1 {
			return fmt.Errorf("envio: %w", app.ErrOutcomeUnknown)
		}
		return nil
	})
	waitFor(t, "as duas mensagens", func() bool { return len(rec.versions()) == 2 })
	stop()

	var again recorder
	stop = run(t, g, topic, func(_ context.Context, m app.Message) error { again.add(m); return nil })
	time.Sleep(3 * time.Second)
	stop()
	if fmt.Sprint(rec.versions()) != "[1 2]" || len(again.versions()) != 0 {
		t.Errorf("vistas = %v, reentregues = %v; esperado [1 2] e nenhuma (a v1 não pode ser repetida)",
			rec.versions(), again.versions())
	}
}

func TestMensagemSemHeadersEDescartadaENaoTravaAFila(t *testing.T) {
	topic := newTopic(t, 1)
	client, err := kgo.NewClient(kgo.SeedBrokers(testBrokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	bad := &kgo.Record{Topic: topic, Key: []byte("agg-1"), Value: []byte(`{}`)}
	if err := client.ProduceSync(context.Background(), bad).FirstErr(); err != nil {
		t.Fatal(err)
	}
	publish(t, topic, "agg-1", 1)

	var rec recorder
	stop := run(t, group(t), topic, func(_ context.Context, m app.Message) error { rec.add(m); return nil })
	defer stop()

	waitFor(t, "a mensagem válida depois da inválida", func() bool { return fmt.Sprint(rec.versions()) == "[1]" })
}

func TestSemCommitAMensagemVoltaParaQuemAssumirOGrupo(t *testing.T) {
	topic := newTopic(t, 1)
	publish(t, topic, "agg-1", 1)
	g := group(t)

	// O handler fica preso (erro transitório sem fim) e o consumidor é encerrado: nunca confirmou.
	var first recorder
	stop := run(t, g, topic, func(_ context.Context, m app.Message) error {
		first.add(m)
		return errors.New("banco indisponível")
	})
	waitFor(t, "a primeira tentativa", func() bool { return len(first.versions()) >= 1 })
	stop()

	var second recorder
	stop = run(t, g, topic, func(_ context.Context, m app.Message) error { second.add(m); return nil })
	defer stop()
	waitFor(t, "a reentrega ao novo consumidor", func() bool { return fmt.Sprint(second.versions()) == "[1]" })
}
