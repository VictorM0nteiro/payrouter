// pool_test.go
// Testes do pool elástico, sem Kafka, sem HTTP e sem banco; rodam sob -race.
// Objetivo: provar as duas garantias que justificam o pool. (1) Duas tarefas da mesma chave
// nunca rodam juntas nem fora de ordem, e chaves diferentes rodam em paralelo de verdade. (2) A
// marca d'água nunca passa de uma tarefa que não terminou. As duas valem também enquanto o pool
// cresce e encolhe no meio da carga. Também prova a contrapressão (Submit bloqueia com a fila
// cheia), o encerramento que drena, que nada roda duas vezes e que nenhuma goroutine vaza.

package elastic

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// harness observa as tarefas e confere as duas garantias enquanto o pool trabalha.
type harness struct {
	mu         sync.Mutex
	active     map[int]bool // chave -> tem tarefa rodando agora
	lastIdx    map[int]int  // chave -> índice (na ordem de submissão) da última tarefa iniciada
	running    int
	maxRunning int
	problems   []string

	done [][]atomic.Bool // [fluxo][sequência] -> a tarefa terminou
	last []int64         // última marca d'água entregue por fluxo; -1 se nenhuma
}

func newHarness(streams, n int) *harness {
	h := &harness{active: map[int]bool{}, lastIdx: map[int]int{}, last: make([]int64, streams)}
	h.done = make([][]atomic.Bool, streams)
	for i := range h.done {
		h.done[i] = make([]atomic.Bool, n)
		h.last[i] = -1
	}
	return h
}

func (h *harness) problem(format string, a ...any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.problems = append(h.problems, fmt.Sprintf(format, a...))
}

// onWatermark é o OnWatermark do pool: confere que a marca cresce e que TUDO até ela terminou.
func (h *harness) onWatermark(stream int, upTo uint64) {
	if int64(upTo) <= h.last[stream] {
		h.problem("fluxo %d: a marca %d não cresceu (última %d)", stream, upTo, h.last[stream])
	}
	for s := h.last[stream] + 1; s <= int64(upTo); s++ {
		if !h.done[stream][s].Load() {
			h.problem("fluxo %d: marca %d à frente da tarefa %d, que não terminou", stream, upTo, s)
		}
	}
	h.last[stream] = int64(upTo)
}

// run devolve a tarefa. idx é a posição dela entre as da sua chave, a partir de 1.
func (h *harness) run(stream int, seq uint64, key, idx int, sleep time.Duration) func() {
	return func() {
		h.mu.Lock()
		if h.active[key] {
			h.problems = append(h.problems, fmt.Sprintf("chave %d: duas tarefas ao mesmo tempo", key))
		}
		if idx != h.lastIdx[key]+1 {
			h.problems = append(h.problems, fmt.Sprintf("chave %d: tarefa %d começou depois da %d", key, idx, h.lastIdx[key]))
		}
		h.active[key] = true
		h.lastIdx[key] = idx
		h.running++
		h.maxRunning = max(h.maxRunning, h.running)
		h.mu.Unlock()

		time.Sleep(sleep)

		h.mu.Lock()
		h.active[key] = false
		h.running--
		h.mu.Unlock()
		h.done[stream][seq].Store(true)
	}
}

// submit manda n tarefas sobre keys chaves. Cada chave pertence a um fluxo só (key % streams),
// como um agregado pertence a uma partição só. Devolve quantas foram para cada fluxo.
func (h *harness) submit(t *testing.T, p *Pool[int], n, keys, streams int, maxSleep time.Duration, seed int64) []uint64 {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	nextSeq := make([]uint64, streams)
	perKey := make([]int, keys)
	for range n {
		key := rng.Intn(keys)
		stream := key % streams
		perKey[key]++
		seq := nextSeq[stream]
		nextSeq[stream]++
		sleep := time.Duration(rng.Int63n(int64(maxSleep) + 1))
		got, err := p.Submit(context.Background(), Task[int]{Stream: stream, Key: key, Run: h.run(stream, seq, key, perKey[key], sleep)})
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}
		if got != seq {
			t.Fatalf("fluxo %d: Submit devolveu a sequência %d, esperado %d", stream, got, seq)
		}
	}
	return nextSeq
}

// verify confere o fim: nenhum problema anotado e a marca de cada fluxo chegou na última tarefa.
func (h *harness) verify(t *testing.T, issued []uint64) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, p := range h.problems {
		t.Error(p)
	}
	for stream, n := range issued {
		if n > 0 && h.last[stream] != int64(n)-1 {
			t.Errorf("fluxo %d: marca final %d, esperado %d", stream, h.last[stream], n-1)
		}
	}
}

func closeCtx(t *testing.T, p *Pool[int]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestMesmaChaveNuncaSobrepoeEMantemAOrdemEChavesDiferentesRodamJuntas(t *testing.T) {
	const n, keys, streams = 3000, 12, 3
	h := newHarness(streams, n)
	p := New[int](Config{Min: 4, Max: 4, Queue: 16, OnWatermark: h.onWatermark})

	issued := h.submit(t, p, n, keys, streams, 300*time.Microsecond, 1)
	closeCtx(t, p)

	h.verify(t, issued)
	if h.maxRunning < 2 {
		t.Errorf("no máximo %d tarefa(s) ao mesmo tempo: o pool não paralelizou nada", h.maxRunning)
	}
}

func TestMarcaDaguaNuncaPassaDeTarefaNaoConcluidaComConclusaoDesordenada(t *testing.T) {
	const n, keys, streams = 2000, 40, 2
	h := newHarness(streams, n)
	// Muitas chaves e tempos bem diferentes: as tarefas terminam fora da ordem de submissão.
	p := New[int](Config{Min: 8, Max: 8, Queue: 32, OnWatermark: h.onWatermark})

	issued := h.submit(t, p, n, keys, streams, 2*time.Millisecond, 2)
	closeCtx(t, p)

	h.verify(t, issued)
}

func TestRedimensionaNoMeioDaCargaSemQuebrarAOrdemNemAMarcaDagua(t *testing.T) {
	const n, keys, streams = 600, 40, 3
	h := newHarness(streams, n)
	var mu sync.Mutex
	maxSize, resizes := 0, 0
	p := New[int](Config{
		Min: 1, Max: 12, Queue: 32, Interval: 5 * time.Millisecond,
		Policy:      Policy{GrowWait: time.Millisecond, GrowTicks: 1, ShrinkBelow: 0.5, ShrinkTicks: 6},
		OnWatermark: h.onWatermark,
		OnResize: func(_, to int) {
			mu.Lock()
			defer mu.Unlock()
			resizes++
			maxSize = max(maxSize, to)
		},
	})

	issued := h.submit(t, p, n, keys, streams, 3*time.Millisecond, 3)
	waitFor(t, 10*time.Second, "o pool esvaziar", func() bool { return p.Stats().Pending == 0 })
	waitFor(t, 10*time.Second, "o pool voltar ao tamanho mínimo", func() bool { return p.Stats().Target == 1 })
	closeCtx(t, p)

	h.verify(t, issued)
	mu.Lock()
	defer mu.Unlock()
	if maxSize < 3 {
		t.Errorf("tamanho máximo %d depois de %d mudanças: o pool não cresceu com a fila cheia", maxSize, resizes)
	}
}

func TestSubmitBloqueiaComAFilaCheiaEVoltaQuandoOContextoTermina(t *testing.T) {
	p := New[int](Config{Min: 1, Max: 1, Queue: 1})
	release := make(chan struct{})
	started := make(chan struct{})
	var second atomic.Bool

	// A primeira ocupa o único worker; a segunda ocupa a única vaga da fila.
	if _, err := p.Submit(context.Background(), Task[int]{Key: 1, Run: func() { close(started); <-release }}); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := p.Submit(context.Background(), Task[int]{Key: 2, Run: func() { second.Store(true) }}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := p.Submit(ctx, Task[int]{Key: 3, Run: func() { t.Error("a terceira não pode ser aceita") }})

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Submit com a fila cheia = %v, esperado o erro do contexto", err)
	}
	close(release)
	closeCtx(t, p)
	if !second.Load() {
		t.Error("a segunda tarefa, já aceita, tinha de rodar")
	}
}

func TestCloseDrenaOQueFoiAceitoERecusaOResto(t *testing.T) {
	p := New[int](Config{Min: 2, Max: 2, Queue: 8})
	var ran atomic.Int32
	for i := range 50 {
		if _, err := p.Submit(context.Background(), Task[int]{Key: i % 5, Run: func() { ran.Add(1) }}); err != nil {
			t.Fatal(err)
		}
	}

	closeCtx(t, p)

	if ran.Load() != 50 {
		t.Errorf("rodaram %d tarefas, esperado 50: o Close tem de drenar tudo que foi aceito", ran.Load())
	}
	if _, err := p.Submit(context.Background(), Task[int]{Key: 1, Run: func() {}}); !errors.Is(err, ErrClosed) {
		t.Errorf("Submit depois do Close = %v, esperado ErrClosed", err)
	}
}

func TestCloseComContextoCurtoDevolveOErroEOPoolTerminaDepois(t *testing.T) {
	p := New[int](Config{Min: 1, Max: 1, Queue: 4})
	release := make(chan struct{})
	started := make(chan struct{})
	var finished atomic.Bool
	if _, err := p.Submit(context.Background(), Task[int]{Key: 1, Run: func() { close(started); <-release; finished.Store(true) }}); err != nil {
		t.Fatal(err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := p.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Close = %v, esperado o erro do contexto com uma tarefa ainda rodando", err)
	}

	close(release)
	closeCtx(t, p) // um segundo Close espera o fim de verdade
	if !finished.Load() {
		t.Error("a tarefa em andamento tinha de terminar")
	}
}

func TestCadaTarefaRodaUmaVezSoPoisOPoolNuncaReenvia(t *testing.T) {
	const n = 500
	counts := make([]atomic.Int32, n)
	p := New[int](Config{Min: 1, Max: 8, Queue: 16, Interval: 2 * time.Millisecond, Policy: Policy{GrowWait: time.Microsecond, GrowTicks: 1}})
	for i := range n {
		if _, err := p.Submit(context.Background(), Task[int]{Key: i % 7, Run: func() { counts[i].Add(1) }}); err != nil {
			t.Fatal(err)
		}
	}

	closeCtx(t, p)

	for i := range counts {
		if got := counts[i].Load(); got != 1 {
			t.Fatalf("a tarefa %d rodou %d vezes, esperado 1", i, got)
		}
	}
}

func TestNaoVazaGoroutinesDepoisDoClose(t *testing.T) {
	before := runtime.NumGoroutine()
	h := newHarness(2, 300)
	p := New[int](Config{Min: 2, Max: 8, Queue: 8, Interval: 2 * time.Millisecond, OnWatermark: h.onWatermark,
		Policy: Policy{GrowWait: time.Microsecond, GrowTicks: 1}})
	issued := h.submit(t, p, 300, 10, 2, 200*time.Microsecond, 4)

	closeCtx(t, p)

	h.verify(t, issued)
	waitFor(t, 5*time.Second, "as goroutines do pool terminarem", func() bool { return runtime.NumGoroutine() <= before })
}

// waitFor espera cond ficar verdadeira, sem um tempo fixo, e falha se estourar o prazo.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("estourou o prazo de %v esperando %s", d, what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
