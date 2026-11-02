// rebalance_test.go
// Testes do Consumer diante de rebalance e de partição presa, contra um Redpanda real (testcontainers).
// Objetivo: provar a sessão 20. Quando um segundo consumidor entra no grupo e leva partições, o
// primeiro espera as tarefas em andamento delas terminarem antes de soltá-las (nenhum agregado roda
// nos dois ao mesmo tempo), nada se perde e o que foi concluído fica confirmado; partição perdida é
// esquecida sem commit; e uma cabeça presa pausa só a leitura da SUA partição, as outras seguem.

package kafka

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/elastic"
	"github.com/twmb/franz-go/pkg/kgo"
)

// overlap guarda, entre todos os consumidores do teste, quem está tratando cada agregado agora e
// quais "agregado/versão" já foram vistos.
type overlap struct {
	mu         sync.Mutex
	running    map[string]string // agregado -> consumidor que o está tratando
	last       map[string]int    // "consumidor/agregado" -> última versão vista por esse consumidor
	violations []string
	handled    map[string]bool
}

func newOverlap() *overlap {
	return &overlap{running: map[string]string{}, last: map[string]int{}, handled: map[string]bool{}}
}

func (o *overlap) begin(who string, m app.Message) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if other, busy := o.running[m.AggregateID]; busy {
		o.violations = append(o.violations, fmt.Sprintf("%s tratou %s enquanto %s ainda o tratava", who, key(m), other))
	}
	o.running[m.AggregateID] = who
	k := who + "/" + m.AggregateID
	if m.Version <= o.last[k] {
		o.violations = append(o.violations, fmt.Sprintf("%s: %s fora de ordem (antes %d)", who, key(m), o.last[k]))
	}
	o.last[k] = m.Version
}

func (o *overlap) end(m app.Message) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.running, m.AggregateID)
	o.handled[key(m)] = true
}

func (o *overlap) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.handled)
}

func (o *overlap) problems() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.violations...)
}

// revokes junta o que o callback de revoke viu: quantas tarefas ainda estavam no handler.
type revokes struct {
	mu      sync.Mutex
	calls   int
	running []int
}

func (r *revokes) hook(_ []int32, running int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.running = append(r.running, running)
}

func (r *revokes) snapshot() (int, []int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls, append([]int(nil), r.running...)
}

func TestSegundoConsumidorEntraNoGrupoSemPerderNemSobreporNemReentregarDepois(t *testing.T) {
	topic := newTopic(t, 4)
	const aggregates, versions = 40, 5
	for a := range aggregates {
		publish(t, topic, fmt.Sprintf("agg-%02d", a), 1, 2, 3, 4, 5)
	}
	g := group(t)
	o := newOverlap()
	handler := func(who string) Handler {
		return func(_ context.Context, m app.Message) error {
			o.begin(who, m)
			time.Sleep(20 * time.Millisecond)
			o.end(m)
			return nil
		}
	}
	var revA, revB revokes

	_, stopA := runWith(t, g, topic, elastic.Config{Min: 4, Max: 4}, handler("A"), func(c *Consumer) { c.afterRevoke = revA.hook })
	waitFor(t, "30% tratadas pelo A", func() bool { return o.count() >= aggregates*versions*3/10 })
	_, stopB := runWith(t, g, topic, elastic.Config{Min: 4, Max: 4}, handler("B"), func(c *Consumer) { c.afterRevoke = revB.hook })
	waitFor(t, "todas tratadas", func() bool { return o.count() == aggregates*versions })
	stopA()
	stopB()

	if p := o.problems(); len(p) > 0 {
		t.Errorf("violações: %v", p[:min(5, len(p))])
	}
	// Repetições entre A e B são esperadas e seguras: a marca d'água contígua para na primeira tarefa
	// descartada, e o que outras chaves concluíram depois dela é repetido pelo novo dono.
	calls, running := revA.snapshot()
	if calls == 0 {
		t.Error("o A nunca teve partição revogada: o B não entrou no grupo?")
	}
	for _, r := range running {
		if r != 0 {
			t.Errorf("o A soltou uma partição com %d tarefas ainda no handler", r)
		}
	}

	// Um terceiro consumidor no grupo não recebe nada: o commit atravessou o rebalance.
	var again seen
	_, stop := runWith(t, g, topic, elastic.Config{Min: 1, Max: 1}, func(_ context.Context, m app.Message) error {
		again.add(m)
		return nil
	})
	time.Sleep(3 * time.Second)
	stop()
	if got := again.all(); len(got) != 0 {
		t.Errorf("reentregues depois do rebalance = %d (%v...), esperado nenhuma", len(got), got[:min(3, len(got))])
	}
}

func TestRevokeEsperaATarefaEmAndamentoAntesDeSoltarAParticao(t *testing.T) {
	topic := newTopic(t, 4)
	const aggregates = 12
	for a := range aggregates {
		publish(t, topic, fmt.Sprintf("agg-%02d", a), 1)
	}
	g := group(t)
	o := newOverlap()
	release := make(chan struct{})
	var started atomic.Int32
	var rev revokes

	_, stopA := runWith(t, g, topic, elastic.Config{Min: aggregates, Max: aggregates}, func(_ context.Context, m app.Message) error {
		o.begin("A", m)
		started.Add(1)
		<-release // todas as tarefas do A ficam presas dentro do handler
		o.end(m)
		return nil
	}, func(c *Consumer) { c.afterRevoke = rev.hook })
	waitFor(t, "as tarefas do A dentro do handler", func() bool { return started.Load() == aggregates })

	// O B entra e quer partições do A; o A não pode soltá-las enquanto suas tarefas estão presas.
	_, stopB := runWith(t, g, topic, elastic.Config{Min: 2, Max: 2}, func(_ context.Context, m app.Message) error {
		o.begin("B", m)
		o.end(m)
		return nil
	})
	time.Sleep(4 * time.Second)
	close(release)
	waitFor(t, "todas tratadas", func() bool { return o.count() == aggregates })
	stopA()
	stopB()

	if p := o.problems(); len(p) > 0 {
		t.Errorf("violações (o B tratou um agregado que o A ainda segurava?): %v", p[:min(5, len(p))])
	}
	if calls, running := rev.snapshot(); calls == 0 {
		t.Error("nenhum revoke aconteceu no A")
	} else {
		for _, r := range running {
			if r != 0 {
				t.Errorf("o A soltou uma partição com %d tarefas ainda no handler", r)
			}
		}
	}
}

func TestPerdaDeParticaoEsqueceOEstadoSemConfirmar(t *testing.T) {
	topic := newTopic(t, 1)
	c, err := NewConsumer(testBrokers, group(t), topic)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p := c.partition(0)
	c.mu.Lock()
	p.next, p.base = 5, 2
	p.recs[2], p.recs[3], p.recs[4] = &kgo.Record{}, &kgo.Record{}, &kgo.Record{}
	p.target, p.hasTarget = 3, true
	p.incomplete, p.hasIncomplete = 4, true
	c.mu.Unlock()

	c.lost(context.Background(), nil, map[string][]int32{topic: {0}})

	c.mu.Lock()
	defer c.mu.Unlock()
	if p.base != 5 || len(p.recs) != 0 || p.hasTarget || p.hasIncomplete {
		t.Errorf("estado depois da perda: base=%d recs=%d hasTarget=%v hasIncomplete=%v, esperado base=5 e tudo limpo",
			p.base, len(p.recs), p.hasTarget, p.hasIncomplete)
	}
	if p.epoch != 1 {
		t.Errorf("época = %d, esperado 1: as tarefas já submetidas não podem ser tratadas", p.epoch)
	}
}

// produceTo grava um registro numa partição escolhida, com os headers do relay.
func produceTo(t *testing.T, topic string, partition int32, aggregate string, version int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(testBrokers...), kgo.DefaultProduceTopic(topic), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	rec := &kgo.Record{Key: []byte(aggregate), Value: []byte(`{}`), Partition: partition, Headers: []kgo.RecordHeader{
		{Key: "event_type", Value: []byte("InstructionAccepted")},
		{Key: "version", Value: []byte(strconv.Itoa(version))},
	}}
	if err := cl.ProduceSync(context.Background(), rec).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func TestCabecaPresaDeUmaParticaoNaoParaAsOutras(t *testing.T) {
	topic := newTopic(t, 2)
	produceTo(t, topic, 0, "stuck", 1) // a cabeça da partição 0
	for i := 1; i <= 6; i++ {
		produceTo(t, topic, 0, fmt.Sprintf("p0-%d", i), 1)
	}
	const others = 20
	for i := range others {
		produceTo(t, topic, 1, fmt.Sprintf("p1-%02d", i), 1)
	}

	release := make(chan struct{})
	var done seen
	_, stop := runWith(t, group(t), topic, elastic.Config{Min: 8, Max: 8}, func(_ context.Context, m app.Message) error {
		if m.AggregateID == "stuck" {
			<-release
		}
		done.add(m)
		return nil
	}, func(c *Consumer) { c.window = 3 })
	defer stop()

	// Com a janela de 3 estourada na partição 0, a partição 1 tem de terminar toda enquanto a cabeça segue presa.
	waitFor(t, "as 20 da partição 1", func() bool {
		n := 0
		for _, k := range done.all() {
			if len(k) > 2 && k[:2] == "p1" {
				n++
			}
		}
		return n == others
	})
	if done.has("stuck/1") {
		t.Fatal("a cabeça devia estar presa")
	}

	close(release)
	waitFor(t, "tudo depois de destravar", func() bool { return len(done.all()) == 1+6+others })
}
