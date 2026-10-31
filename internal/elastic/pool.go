// pool.go
// O pool elástico de workers: roda tarefas em paralelo entre Min e Max goroutines, sem nunca
// rodar duas tarefas da mesma chave ao mesmo tempo nem fora de ordem, e avisa até onde cada
// fluxo terminou sem buracos.
// Objetivo: o paralelismo do processor vem de mensagens independentes chegando juntas; o limite
// vem do participante externo. O pool só pode crescer e encolher se isso não custar a ordem por
// agregado, e se o offset nunca for confirmado à frente de uma mensagem não concluída.
//
// Como as duas garantias valem:
//   - Exclusividade e ordem por chave: uma chave presente em keys tem exatamente uma tarefa na
//     fila de prontas ou rodando; as seguintes esperam na fila da própria chave e são liberadas
//     uma a uma, na ordem de submissão. Não importa qual worker roda a tarefa, então redimensionar
//     não reordena nada.
//   - Marca d'água: cada fluxo numera as tarefas na submissão e só avisa (OnWatermark) o prefixo
//     contíguo concluído (watermark.go).
//
// O pool não reenvia nada: uma tarefa que retorna terminou, com ou sem erro. Repetir um erro
// transitório é assunto da própria tarefa. Pânico numa tarefa não é recuperado.

package elastic

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrClosed é o erro de Submit depois do Close.
var ErrClosed = errors.New("elastic: pool encerrado")

// Task é uma unidade de trabalho. Stream é a partição (a marca d'água é por fluxo); Key é o
// agregado (a ordem e a exclusividade são por chave).
type Task[K comparable] struct {
	Stream int
	Key    K
	Run    func()
}

// Config parametriza o pool. Min e Max valem pelo menos 1 e Max >= Min. Campos zerados usam o
// padrão.
type Config struct {
	Min      int           // workers mínimos, e o tamanho inicial (padrão 1)
	Max      int           // workers máximos (padrão = Min)
	Queue    int           // tarefas aceitas e ainda não iniciadas; cheia, Submit bloqueia (padrão 64)
	Interval time.Duration // período de amostragem do controlador (padrão 50ms)
	Policy   Policy        // quando crescer e encolher

	// OnWatermark é chamada, em série e com valores crescentes por fluxo, quando o prefixo
	// contíguo concluído do fluxo cresce: toda tarefa de sequência <= seq terminou. Não deve
	// chamar métodos do pool. Opcional.
	OnWatermark func(stream int, seq uint64)
	// OnResize é chamada pelo controlador quando o tamanho desejado muda. Opcional.
	OnResize func(from, to int)
}

// Stats é um retrato do pool.
type Stats struct {
	Size    int // workers vivos
	Target  int // tamanho desejado
	Ready   int // tarefas prontas que ainda não começaram
	Pending int // tarefas aceitas e não concluídas (esperando, prontas ou rodando)
	Running int // tarefas rodando agora
}

type item[K comparable] struct {
	task Task[K]
	seq  uint64
	enq  time.Time
}

// Pool executa tarefas. Crie com New e encerre com Close.
type Pool[K comparable] struct {
	cfg   Config
	slots chan struct{} // uma vaga por tarefa aceita e ainda não iniciada: o limite da fila
	sizer *sizer

	mu      sync.Mutex
	cond    *sync.Cond
	ready   []*item[K]         // prontas: a chave está livre. FIFO
	keys    map[K][]*item[K]   // chave com tarefa pronta ou rodando -> as que esperam atrás dela, em ordem
	wms     map[int]*watermark // por fluxo
	dirty   map[int]uint64     // marcas novas ainda não entregues a OnWatermark
	workers int                // goroutines vivas
	target  int                // tamanho desejado
	running int                // tarefas rodando
	pending int                // aceitas e não concluídas
	waitSum time.Duration      // soma das esperas das tarefas iniciadas desde a última amostra
	waitN   int                // quantas
	closed  bool

	cbMu    sync.Mutex // serializa as chamadas de OnWatermark
	closing chan struct{}
	stopCtl chan struct{}
	wg      sync.WaitGroup
}

// New cria o pool já com Min workers rodando.
func New[K comparable](cfg Config) *Pool[K] {
	if cfg.Min < 1 {
		cfg.Min = 1
	}
	if cfg.Max < cfg.Min {
		cfg.Max = cfg.Min
	}
	if cfg.Queue < 1 {
		cfg.Queue = 64
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 50 * time.Millisecond
	}
	p := &Pool[K]{
		cfg:     cfg,
		slots:   make(chan struct{}, cfg.Queue),
		sizer:   newSizer(cfg.Policy),
		keys:    make(map[K][]*item[K]),
		wms:     make(map[int]*watermark),
		dirty:   make(map[int]uint64),
		workers: cfg.Min,
		target:  cfg.Min,
		closing: make(chan struct{}),
		stopCtl: make(chan struct{}),
	}
	p.cond = sync.NewCond(&p.mu)
	p.wg.Add(cfg.Min + 1)
	for range cfg.Min {
		go p.worker()
	}
	go p.control()
	return p
}

// Submit aceita a tarefa e devolve a sequência dela no fluxo. Bloqueia enquanto a fila estiver
// cheia (contrapressão: quem chama para de buscar mensagens) e volta com o erro do contexto se ele
// terminar antes. As tarefas de um mesmo fluxo devem ser submetidas em série, na ordem das
// mensagens: é essa ordem que numera a marca d'água e que o pool preserva por chave.
func (p *Pool[K]) Submit(ctx context.Context, t Task[K]) (uint64, error) {
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-p.closing:
		return 0, ErrClosed
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		<-p.slots
		return 0, ErrClosed
	}
	wm := p.wms[t.Stream]
	if wm == nil {
		wm = newWatermark()
		p.wms[t.Stream] = wm
	}
	it := &item[K]{task: t, seq: wm.issue(), enq: time.Now()}
	p.pending++
	if waiting, busy := p.keys[t.Key]; busy {
		p.keys[t.Key] = append(waiting, it)
	} else {
		p.keys[t.Key] = nil
		p.ready = append(p.ready, it)
		p.cond.Signal()
	}
	p.mu.Unlock()
	return it.seq, nil
}

// Stats devolve um retrato do pool.
func (p *Pool[K]) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{Size: p.workers, Target: p.target, Ready: len(p.ready), Pending: p.pending, Running: p.running}
}

// Close recusa novas tarefas, deixa terminar as que já foram aceitas (as da fila e as rodando) e
// entrega as últimas marcas d'água. Se ctx terminar antes, devolve o erro dele e o pool continua
// terminando em segundo plano.
func (p *Pool[K]) Close(ctx context.Context) error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.closing)
		close(p.stopCtl)
		p.cond.Broadcast()
	}
	p.mu.Unlock()

	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		p.flush()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool[K]) worker() {
	defer p.wg.Done()
	p.mu.Lock()
	for {
		for len(p.ready) == 0 {
			// Só um worker ocioso é aposentado: nunca um que está no meio de uma tarefa.
			if p.workers > p.target || (p.closed && p.pending == 0) {
				p.workers--
				p.mu.Unlock()
				return
			}
			p.cond.Wait()
		}
		it := p.ready[0]
		p.ready[0] = nil
		p.ready = p.ready[1:]
		p.waitSum += time.Since(it.enq)
		p.waitN++
		p.running++
		p.mu.Unlock()

		<-p.slots // a tarefa saiu da fila: libera uma vaga para Submit
		it.task.Run()

		p.mu.Lock()
		p.running--
		p.pending--
		p.release(it)
		if p.closed && p.pending == 0 {
			p.cond.Broadcast()
		}
		p.mu.Unlock()
		p.flush()
		p.mu.Lock()
	}
}

// release marca a tarefa como concluída: avança a marca d'água do fluxo e libera a próxima da
// mesma chave, ou a própria chave se ninguém espera. Chamar com p.mu travado.
func (p *Pool[K]) release(it *item[K]) {
	if upTo, advanced := p.wms[it.task.Stream].complete(it.seq); advanced {
		p.dirty[it.task.Stream] = upTo
	}
	waiting := p.keys[it.task.Key]
	if len(waiting) == 0 {
		delete(p.keys, it.task.Key)
		return
	}
	next := waiting[0]
	waiting[0] = nil
	p.keys[it.task.Key] = waiting[1:]
	p.ready = append(p.ready, next)
	p.cond.Signal()
}

// flush entrega a OnWatermark as marcas novas. Os valores são lidos sob p.mu, mas a chamada
// acontece fora dele e dentro de cbMu: assim duas entregas nunca se cruzam e, por fluxo, o valor
// só cresce, sem que o callback possa travar o pool.
func (p *Pool[K]) flush() {
	if p.cfg.OnWatermark == nil {
		return
	}
	p.cbMu.Lock()
	defer p.cbMu.Unlock()
	for {
		p.mu.Lock()
		if len(p.dirty) == 0 {
			p.mu.Unlock()
			return
		}
		batch := p.dirty
		p.dirty = make(map[int]uint64)
		p.mu.Unlock()
		for stream, upTo := range batch {
			p.cfg.OnWatermark(stream, upTo)
		}
	}
}

// control amostra o pool a cada Interval e aplica a decisão da política.
func (p *Pool[K]) control() {
	defer p.wg.Done()
	tick := time.NewTicker(p.cfg.Interval)
	defer tick.Stop()
	for {
		select {
		case <-p.stopCtl:
			return
		case <-tick.C:
		}
		p.mu.Lock()
		if p.closed { // fechando: o tamanho fica como está até esvaziar
			p.mu.Unlock()
			return
		}
		delta := p.sizer.next(p.sampleLocked(time.Now()))
		from := p.target
		switch {
		case delta > 0:
			p.target++
			p.workers++
			p.wg.Add(1)
			go p.worker()
		case delta < 0:
			p.target--
			p.cond.Broadcast() // acorda os ociosos: um deles se aposenta
		}
		to := p.target
		p.mu.Unlock()
		if from != to && p.cfg.OnResize != nil {
			p.cfg.OnResize(from, to)
		}
	}
}

// sampleLocked lê os sinais da política e zera a média de espera. Chamar com p.mu travado.
func (p *Pool[K]) sampleLocked(now time.Time) Sample {
	var wait time.Duration
	if p.waitN > 0 {
		wait = p.waitSum / time.Duration(p.waitN)
	}
	p.waitSum, p.waitN = 0, 0
	// Se nada começou mas há tarefa pronta esperando, a idade dela é o sinal de saturação.
	if len(p.ready) > 0 {
		wait = max(wait, now.Sub(p.ready[0].enq))
	}
	return Sample{
		Size:  p.target,
		Min:   p.cfg.Min,
		Max:   p.cfg.Max,
		Ready: len(p.ready),
		Wait:  wait,
		Busy:  float64(p.running) / float64(p.target),
	}
}
