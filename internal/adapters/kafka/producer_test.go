// producer_test.go
// Testes do Producer contra um Redpanda real (testcontainers).
// Objetivo: provar que cada mensagem chega ao broker com a chave (aggregate_id), o valor
// (envelope) e os headers certos, e que mensagens do mesmo agregado caem na mesma partição,
// que é o que garante a ordem por agregado.

package kafka

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/testutil/kafkatest"
)

var (
	testBrokers []string
	setupErr    error
)

// TestMain sobe um único Redpanda para o pacote inteiro; cada teste usa um tópico próprio.
func TestMain(m *testing.M) {
	brokers, stop, err := kafkatest.Start(context.Background())
	testBrokers, setupErr = brokers, err
	code := m.Run()
	if stop != nil {
		stop()
	}
	os.Exit(code)
}

func newTopic(t *testing.T, partitions int32) string {
	t.Helper()
	if setupErr != nil {
		t.Fatalf("Redpanda de teste indisponível (o Docker está rodando?): %v", setupErr)
	}
	topic := fmt.Sprintf("test.%s.%d", t.Name(), time.Now().UnixNano())
	if err := kafkatest.CreateTopic(context.Background(), testBrokers, topic, partitions); err != nil {
		t.Fatalf("criando tópico: %v", err)
	}
	return topic
}

func TestPublishEntregaChaveValorEHeaders(t *testing.T) {
	topic := newTopic(t, 3)
	p, err := NewProducer(testBrokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	err = p.Publish(context.Background(), []app.OutboxMessage{
		{ID: 1, AggregateID: "agg-1", Topic: topic, EventType: "InstructionAccepted", Version: 1, Payload: []byte(`{"version":1}`)},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got, err := kafkatest.ReadAll(context.Background(), testBrokers, topic, 30*time.Second)
	if err != nil || len(got) != 1 {
		t.Fatalf("lidos = %d, %v; esperado 1", len(got), err)
	}
	r := got[0]
	if r.Key != "agg-1" || string(r.Value) != `{"version":1}` ||
		r.Headers["event_type"] != "InstructionAccepted" || r.Headers["version"] != "1" {
		t.Errorf("registro = %+v", r)
	}
}

func TestMesmoAgregadoCaiNaMesmaParticaoEmOrdem(t *testing.T) {
	topic := newTopic(t, 3)
	p, err := NewProducer(testBrokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var batch []app.OutboxMessage
	for v := 1; v <= 5; v++ {
		for _, agg := range []string{"agg-a", "agg-b", "agg-c", "agg-d"} {
			batch = append(batch, app.OutboxMessage{AggregateID: agg, Topic: topic, EventType: "E", Version: v, Payload: []byte(fmt.Sprintf(`{"v":%d}`, v))})
		}
	}
	if err := p.Publish(context.Background(), batch); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got, err := kafkatest.ReadAll(context.Background(), testBrokers, topic, 30*time.Second)
	if err != nil || len(got) != len(batch) {
		t.Fatalf("lidos = %d, %v; esperado %d", len(got), err, len(batch))
	}
	partition := map[string]int32{}
	lastVersion := map[string]string{}
	for _, r := range got {
		if p, ok := partition[r.Key]; ok && p != r.Partition {
			t.Errorf("%s apareceu nas partições %d e %d", r.Key, p, r.Partition)
		}
		partition[r.Key] = r.Partition
		if v := r.Headers["version"]; v < lastVersion[r.Key] {
			t.Errorf("%s: versão %s depois de %s (fora de ordem)", r.Key, v, lastVersion[r.Key])
		}
		lastVersion[r.Key] = r.Headers["version"]
	}
}

func TestPublishSemBrokerDevolveErroEmVezDeEngolir(t *testing.T) {
	p, err := NewProducer([]string{"127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = p.Publish(ctx, []app.OutboxMessage{{AggregateID: "x", Topic: "t", Payload: []byte(`{}`)}})

	if err == nil {
		t.Fatal("Publish devolveu nil sem broker: o relay marcaria como publicado algo que não saiu")
	}
}
