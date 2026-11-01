// pool_test.go
// Testes do Consumer com o pool de workers, contra um Redpanda real (testcontainers).
// Objetivo: provar a sessão 19b. Várias mensagens rodam em paralelo, mas as do mesmo agregado
// nunca juntas nem fora de ordem; o offset confirmado nunca passa de uma mensagem não concluída
// (nem no desligamento, nem com uma mensagem veneno no meio); a contrapressão pausa a partição uma
// vez só e a retoma quando a última espera termina; as tarefas que não começaram não são tratadas
// no desligamento e voltam para o grupo; e uma cabeça de partição presa não deixa a memória crescer.

package kafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/elastic"
	"github.com/twmb/franz-go/pkg/kgo"
)

// runWith inicia o consumidor com o pool dimensionado e devolve o consumidor e o stop.
func runWith(t *testing.T, group, topic string, cfg elastic.Config, h Handler, opts ...func(*Consumer)) (*Consumer, func()) {
	t.Helper()
	c, err := NewConsumer(testBrokers, group, topic)
	if err != nil {
		t.Fatal(err)
	}
	c.WithWorkers(cfg)
	for _, o := range opts {
		o(c)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, h) }()
	return c, func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
		c.Close()
	}
}

func key(m app.Message) string { return fmt.Sprintf("%s/%d", m.AggregateID, m.Version) }

// seen guarda as mensagens vistas como "agregado/versão", em ordem.
type seen struct {
	mu  sync.Mutex
	set []string
}

func (s *seen) add(m app.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set = append(s.set, key(m))
}

func (s *seen) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.set...)
}

func (s *seen) has(k string) bool {
	for _, v := range s.all() {
		if v == k {
			return true
		}
	}
	return false
}

func TestPoolMantemAOrdemEAExclusividadePorAgregadoEParalelizaEntreAgregados(t *testing.T) {
	topic := newTopic(t, 3)
	const aggregates, versions = 20, 5
	for a := range aggregates {
		publish(t, topic, fmt.Sprintf("agg-%02d", a), 1, 2, 3, 4, 5)
	}

	var (
		mu         sync.Mutex
		running    = map[string]bool{}
		last       = map[string]int{}
		violations []string
		cur, peak  int
		handled    atomic.Int32
	)
	_, stop := runWith(t, group(t), topic, elastic.Config{Min: 4, Max: 4}, func(_ context.Context, m app.Message) error {
		mu.Lock()
		if running[m.AggregateID] {
			violations = append(violations, "sobreposição em "+key(m))
		}
		if last[m.AggregateID] != m.Version-1 {
			violations = append(violations, fmt.Sprintf("fora de ordem em %s: antes %d", key(m), last[m.AggregateID]))
		}
		running[m.AggregateID] = true
		cur++
		peak = max(peak, cur)
		mu.Unlock()

		time.Sleep(10 * time.Millisecond)

		mu.Lock()
		running[m.AggregateID] = false
		last[m.AggregateID] = m.Version
		cur--
		mu.Unlock()
		handled.Add(1)
		return nil
	})
	waitFor(t, "todas as mensagens", func() bool { return handled.Load() == aggregates*versions })
	stop()

	mu.Lock()
	defer mu.Unlock()
	if len(violations) > 0 {
		t.Errorf("violações: %v", violations[:min(5, len(violations))])
	}
	if peak < 2 {
		t.Errorf("pico de %d tratamentos simultâneos: o pool não paralelizou entre agregados", peak)
	}
}

func TestConfirmacaoNuncaPassaDeMensagemNaoConcluida(t *testing.T) {
	topic := newTopic(t, 1)
	publish(t, topic, "agg-b", 1)
	publish(t, topic, "agg-a", 1) // fica presa
	publish(t, topic, "agg-b", 2, 3)
	g := group(t)

	var first seen
	_, stop := runWith(t, g, topic, elastic.Config{Min: 3, Max: 3}, func(_ context.Context, m app.Message) error {
		if m.AggregateID == "agg-a" {
			return &app.BackpressureError{After: time.Hour} // nunca conclui: o consumidor é parado
		}
		first.add(m)
		return nil
	})
	waitFor(t, "b1, b2 e b3 concluídas", func() bool { return len(first.all()) == 3 })
	time.Sleep(500 * time.Millisecond) // deixa o committer rodar com a a1 ainda presa
	stop()

	// O offset confirmado para logo antes da a1 (que ficou presa): ela e o que veio depois voltam; a b1 não.
	var second seen
	_, stop = runWith(t, g, topic, elastic.Config{Min: 1, Max: 1}, func(_ context.Context, m app.Message) error {
		second.add(m)
		return nil
	})
	waitFor(t, "reentrega a partir da a1", func() bool { return len(second.all()) >= 3 })
	stop()

	if !second.has("agg-a/1") {
		t.Errorf("a1 não foi reentregue (reentregues: %v): o offset passou de uma mensagem não concluída", second.all())
	}
	if second.has("agg-b/1") {
		t.Errorf("b1 foi reentregue (%v): estava concluída e antes da cabeça presa, devia estar confirmada", second.all())
	}
}

// firstAndLast mede o intervalo entre o primeiro início e o último fim dos tratamentos.
type firstAndLast struct {
	mu          sync.Mutex
	first, last time.Time
	n           int
}

func (f *firstAndLast) start() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.first.IsZero() {
		f.first = time.Now()
	}
}

func (f *firstAndLast) end() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.last = time.Now()
	f.n++
}

func (f *firstAndLast) count() int { f.mu.Lock(); defer f.mu.Unlock(); return f.n }

func (f *firstAndLast) span() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last.Sub(f.first)
}

func throughput(t *testing.T, workers int) time.Duration {
	t.Helper()
	topic := newTopic(t, 3)
	const n = 20
	for a := range n {
		publish(t, topic, fmt.Sprintf("agg-t%02d", a), 1)
	}
	var m firstAndLast
	_, stop := runWith(t, group(t), topic, elastic.Config{Min: workers, Max: workers}, func(context.Context, app.Message) error {
		m.start()
		time.Sleep(100 * time.Millisecond)
		m.end()
		return nil
	})
	waitFor(t, "todas as mensagens", func() bool { return m.count() == n })
	stop()
	return m.span()
}

func TestMaisWorkersTratamMaisRapidoEntreAgregadosDiferentes(t *testing.T) {
	one := throughput(t, 1)
	four := throughput(t, 4)

	if four >= one*6/10 {
		t.Errorf("1 worker levou %v e 4 workers %v; esperado 4 workers em menos de 60%% do tempo", one, four)
	}
}

func TestContrapressaoDeDuasTarefasPausaAParticaoUmaVezEORetomaNaUltima(t *testing.T) {
	topic := newTopic(t, 1)
	publish(t, topic, "agg-p1", 1)
	publish(t, topic, "agg-p2", 1)

	var (
		mu       sync.Mutex
		attempts = map[string]int{}
		began    = map[string]time.Time{}
		third    time.Time
		finished seen
	)
	waits := map[string]time.Duration{"agg-p1": 300 * time.Millisecond, "agg-p2": 1500 * time.Millisecond}
	c, stop := runWith(t, group(t), topic, elastic.Config{Min: 3, Max: 3}, func(_ context.Context, m app.Message) error {
		mu.Lock()
		attempts[m.AggregateID]++
		first := attempts[m.AggregateID] == 1
		if first {
			began[m.AggregateID] = time.Now()
		}
		mu.Unlock()
		if m.AggregateID == "agg-p3" {
			mu.Lock()
			third = time.Now()
			mu.Unlock()
		} else if first {
			return &app.BackpressureError{After: waits[m.AggregateID]}
		}
		finished.add(m)
		return nil
	})
	waitFor(t, "as duas esperas começaram", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(began) == 2
	})
	mu.Lock()
	t0 := began["agg-p2"]
	mu.Unlock()
	c.mu.Lock()
	paused := c.parts[0].pauses
	c.mu.Unlock()
	if paused != 2 {
		t.Errorf("esperas em andamento = %d, esperado 2", paused)
	}
	publish(t, topic, "agg-p3", 1) // chega com a partição pausada pelas duas esperas

	waitFor(t, "as três mensagens", func() bool { return len(finished.all()) == 3 })
	stop()

	mu.Lock()
	defer mu.Unlock()
	if d := third.Sub(t0); d < 1200*time.Millisecond {
		t.Errorf("a p3 foi tratada %v depois da segunda espera começar; esperado >= 1,2 s: a partição foi retomada quando a primeira espera terminou", d)
	}
	if c.parts[0].pauses != 0 {
		t.Errorf("pausas ainda em andamento = %d, esperado 0", c.parts[0].pauses)
	}
}

// produceRaw grava um registro sem os headers do relay (mensagem veneno).
func produceRaw(t *testing.T, topic, key string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(testBrokers...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.ProduceSync(context.Background(), &kgo.Record{Key: []byte(key), Value: []byte(`{}`)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func TestMensagemVenenoNoMeioNaoParaAConfirmacao(t *testing.T) {
	topic := newTopic(t, 1)
	publish(t, topic, "agg-v", 1)
	produceRaw(t, topic, "agg-veneno")
	publish(t, topic, "agg-v", 2)
	g := group(t)

	var first seen
	_, stop := runWith(t, g, topic, elastic.Config{Min: 2, Max: 2}, func(_ context.Context, m app.Message) error {
		first.add(m)
		return nil
	})
	waitFor(t, "as duas válidas", func() bool { return len(first.all()) == 2 })
	time.Sleep(500 * time.Millisecond)
	stop()

	var again seen
	_, stop = runWith(t, g, topic, elastic.Config{Min: 1, Max: 1}, func(_ context.Context, m app.Message) error {
		again.add(m)
		return nil
	})
	time.Sleep(3 * time.Second)
	stop()
	if got := again.all(); len(got) != 0 {
		t.Errorf("reentregues = %v, esperado nenhuma: o veneno não pode travar a confirmação", got)
	}
}

func TestDesligamentoNaoTrataAFilaESoConfirmaOPrefixoConcluido(t *testing.T) {
	topic := newTopic(t, 1)
	publish(t, topic, "agg-s1", 1)
	for i := 2; i <= 6; i++ {
		publish(t, topic, fmt.Sprintf("agg-s%d", i), 1)
	}
	g := group(t)

	var first seen
	var started atomic.Bool
	release := make(chan struct{})
	_, stop := runWith(t, g, topic, elastic.Config{Min: 1, Max: 1}, func(_ context.Context, m app.Message) error {
		if m.AggregateID == "agg-s1" {
			started.Store(true)
			<-release // em andamento no desligamento: termina o que começou
		}
		first.add(m)
		return nil
	})
	waitFor(t, "s1 em andamento", started.Load)
	go func() { time.Sleep(300 * time.Millisecond); close(release) }()
	stop() // cancela com a s1 rodando e as outras cinco na fila

	var second seen
	_, stop = runWith(t, g, topic, elastic.Config{Min: 1, Max: 1}, func(_ context.Context, m app.Message) error {
		second.add(m)
		return nil
	})
	waitFor(t, "reentrega das cinco", func() bool { return len(second.all()) >= 5 })
	stop()

	if got := first.all(); len(got) != 1 || got[0] != "agg-s1/1" {
		t.Errorf("tratadas antes de parar = %v, esperado só [agg-s1/1]: a fila não é tratada no desligamento", got)
	}
	if second.has("agg-s1/1") {
		t.Errorf("s1 foi reentregue (%v): terminou antes de parar e devia estar confirmada", second.all())
	}
	if len(second.all()) != 5 {
		t.Errorf("reentregues = %v, esperado as cinco s2..s6", second.all())
	}
}

func TestCabecaPresaLimitaOQueFicaSemConfirmarEDestravaQuandoConclui(t *testing.T) {
	topic := newTopic(t, 1)
	publish(t, topic, "agg-w0", 1) // a cabeça
	for i := 1; i <= 8; i++ {
		publish(t, topic, fmt.Sprintf("agg-w%d", i), 1)
	}

	release := make(chan struct{})
	var done seen
	_, stop := runWith(t, group(t), topic, elastic.Config{Min: 4, Max: 4}, func(_ context.Context, m app.Message) error {
		if m.AggregateID == "agg-w0" {
			<-release
		}
		done.add(m)
		return nil
	}, func(c *Consumer) { c.window = 3 })
	defer stop()

	// Com a cabeça presa e janela 3, só a cabeça e mais duas são submetidas: espera as duas
	// concluírem e confere que, passado um tempo, nenhuma outra entrou.
	waitFor(t, "as duas depois da cabeça", func() bool { return len(done.all()) >= 2 })
	time.Sleep(time.Second)
	if got := len(done.all()); got != 2 {
		t.Errorf("concluídas com a cabeça presa = %d, esperado 2 (a janela de 3 inclui a cabeça)", got)
	}

	close(release)
	waitFor(t, "as nove depois de destravar", func() bool { return len(done.all()) == 9 })
}
