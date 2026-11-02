// consumer.go
// Adapter Kafka que entrega ao caso de uso as mensagens de um tópico, sobre franz-go, em
// grupo de consumidores, com um pool elástico de workers.
// Objetivo: at-least-once sem nunca descartar e sem nunca confirmar um offset à frente de uma
// mensagem não concluída. Cada registro vira uma tarefa do pool (fluxo = partição, chave =
// aggregate_id): o pool garante que duas mensagens do mesmo agregado nunca rodam juntas nem fora
// de ordem, e a confirmação do offset segue a marca d'água contígua, em lote, por uma goroutine
// própria. Erro transitório repete a MESMA mensagem, no lugar, dentro da tarefa (o worker e a chave
// ficam presos: a ordem do agregado vale). A única exceção é app.ErrOutcomeUnknown, que é uma
// resposta definitiva do handler: o envio já foi feito e reenviar é perigoso, então o offset
// avança. Mensagem sem os headers esperados (veneno) é registrada e vira uma tarefa vazia, para a
// marca d'água não parar atrás dela. Quando o limite de saída está cheio (app.BackpressureError),
// pausa a partição do registro (contada: só retoma quando a última espera termina), espera e repete
// a mesma mensagem: contrapressão, o lag cresce, nada é descartado.
//
// Uma tarefa que o desligamento interrompe NÃO terminou, mesmo que o pool a dê por concluída: ela
// se marca como incompleta e a confirmação nunca passa dela.
//
// Rebalance: quando o grupo tira uma partição, o callback de revoke impede que as tarefas dela que
// ainda não começaram sejam tratadas (época nova), espera as que estão dentro do handler terminarem
// (com prazo), confirma o que a marca d'água permite e esquece a partição; assim o novo dono começa
// do offset confirmado e o que ficou para trás é reentregue (seguro: o claim é um append com versão
// esperada). Partição perdida (erro fatal do grupo) é esquecida sem commit.
//
// Uma partição com mensagens demais sem confirmar (uma cabeça presa) tem só a SUA leitura pausada;
// as outras seguem. O lote já em mãos pode passar um pouco do limite.

package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictorM0nteiro/payrouter/internal/app"
	"github.com/VictorM0nteiro/payrouter/internal/elastic"
	"github.com/VictorM0nteiro/payrouter/internal/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	minRetryBackoff = 200 * time.Millisecond
	maxRetryBackoff = 10 * time.Second

	// handleTimeout é o prazo de cada tentativa do handler (envio + gravações).
	handleTimeout = 30 * time.Second

	// commitEvery é o período do commit em lote. Até esse tempo de mensagens já concluídas pode ser
	// repetido depois de uma queda; o consumidor é idempotente pela versão, então é seguro.
	commitEvery = 100 * time.Millisecond

	// closeGrace é quanto o desligamento espera as tarefas em andamento terminarem.
	closeGrace = handleTimeout + 5*time.Second

	// revokeDrain é quanto o callback de revoke espera as tarefas em andamento da partição. Fica bem
	// abaixo do prazo de rebalance do grupo (60 s): passar dele tiraria este membro do grupo.
	// shortcut: prazo fixo; uma tarefa mais lenta que ele é cortada e a mensagem vai ao novo dono,
	// que a repete (seguro). Tornar configurável se o handler ficar mais lento.
	revokeDrain = 20 * time.Second

	// maxUncommitted é, por partição, quantas mensagens submetidas e ainda não confirmadas fazem a
	// leitura DELA pausar. A fila do pool só conta as que não começaram; sem este limite, uma cabeça
	// de partição presa deixaria as seguintes concluírem e se acumularem em memória sem fim.
	maxUncommitted = 1024
)

// Handler trata uma mensagem. Ver app.ProcessInstruction.Handle para o contrato do erro.
type Handler func(ctx context.Context, msg app.Message) error

// partition é o estado de uma partição: o que foi submetido ao pool e ainda não foi confirmado.
// Protegido por Consumer.mu.
type partition struct {
	id   int32
	next uint64                 // sequência que o próximo Submit recebe (o pool numera 0, 1, 2...)
	base uint64                 // primeira sequência ainda não confirmada
	recs map[uint64]*kgo.Record // sequência -> registro, de base até next-1

	incomplete    uint64 // primeira sequência cortada pelo desligamento ou pelo revoke
	hasIncomplete bool
	target        uint64 // maior sequência que já pode ser confirmada
	hasTarget     bool

	pauses       int  // esperas de contrapressão e do limite de não confirmadas que pausam a partição
	windowPaused bool // a leitura está pausada por excesso de não confirmadas (conta uma vez em pauses)

	epoch    uint64 // sobe a cada revoke/lost; tarefa de época antiga não é tratada
	inflight int    // tarefas dentro do handler agora
}

// Consumer lê um tópico em grupo.
type Consumer struct {
	client  *kgo.Client
	topic   string
	metrics *metrics.Processor // nil = sem métricas
	workers elastic.Config
	window  uint64        // mensagens não confirmadas por partição antes de a leitura dela pausar
	drain   time.Duration // prazo do callback de revoke para as tarefas em andamento

	// afterRevoke, se definido (testes), roda ao fim de cada revoke com as partições e quantas
	// tarefas ainda estavam dentro do handler quando o prazo acabou (0 = esperou todas).
	afterRevoke func(parts []int32, stillRunning int)

	pool atomic.Pointer[elastic.Pool[string]]

	commitMu sync.Mutex // serializa os commits e o esquecimento de partições
	mu       sync.Mutex
	parts    map[int32]*partition
}

// WithMetrics liga as métricas do consumidor. Sem elas, nada é medido.
func (c *Consumer) WithMetrics(m *metrics.Processor) *Consumer {
	c.metrics = m
	return c
}

// WithWorkers dimensiona o pool de workers (Min, Max e Queue; o padrão é 1 worker e fila de 64).
// OnWatermark e OnResize são do consumidor e são ignorados aqui.
func (c *Consumer) WithWorkers(cfg elastic.Config) *Consumer {
	c.workers = cfg
	return c
}

// PoolStats é um retrato do pool; zerado antes do Run.
func (c *Consumer) PoolStats() elastic.Stats {
	if p := c.pool.Load(); p != nil {
		return p.Stats()
	}
	return elastic.Stats{}
}

// NewConsumer entra no grupo para ler o tópico. Sem offset salvo, começa do início, para
// nada que já esteja no tópico ser perdido.
func NewConsumer(brokers []string, group, topic string) (*Consumer, error) {
	c := &Consumer{
		topic:   topic,
		workers: elastic.Config{Min: 1, Max: 1},
		window:  maxUncommitted,
		drain:   revokeDrain,
		parts:   make(map[int32]*partition),
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(c.assigned),
		kgo.OnPartitionsRevoked(c.revoked),
		kgo.OnPartitionsLost(c.lost),
	)
	if err != nil {
		return nil, fmt.Errorf("criando consumidor Kafka: %w", err)
	}
	c.client = client
	return c, nil
}

// Run consome até o contexto terminar. As mensagens em andamento são concluídas antes de sair; as
// que ainda esperam na fila não são tratadas e voltam para quem assumir o grupo.
func (c *Consumer) Run(ctx context.Context, handle Handler) error {
	cfg := c.workers
	cfg.OnWatermark = c.onWatermark
	cfg.OnResize = func(from, to int) { c.metrics.PoolResized(from, to) }
	pool := elastic.New[string](cfg)
	c.pool.Store(pool)

	stopCommit, committerDone := make(chan struct{}), make(chan struct{})
	go c.committer(stopCommit, committerDone)

	for ctx.Err() == nil {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if ctx.Err() == nil {
				slog.ErrorContext(ctx, "consumer: erro no fetch", "topic", topic, "partition", partition, "error", err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			if ctx.Err() == nil {
				c.submit(ctx, pool, handle, r)
			}
		})
	}

	// Drena o que o pool já aceitou. As tarefas em andamento terminam o handler (ele roda sem o
	// cancelamento do ctx); as que não começaram se marcam incompletas e não são tratadas.
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeGrace)
	if err := pool.Close(closeCtx); err != nil {
		slog.ErrorContext(ctx, "consumer: o pool não terminou no prazo, confirmando só o que concluiu", "error", err)
	}
	cancel()
	close(stopCommit)
	<-committerDone // faz o último commit antes de sair
	return nil
}

// submit entrega o registro ao pool. Bloqueia se a fila está cheia; se a partição tem mensagens
// demais sem confirmar, pausa a leitura dela (as outras seguem); volta sem submeter se o contexto
// terminar.
func (c *Consumer) submit(ctx context.Context, pool *elastic.Pool[string], handle Handler, r *kgo.Record) {
	p := c.partition(r.Partition)
	msg, msgErr := toMessage(r)

	// O registro entra no mapa ANTES do Submit: a tarefa pode terminar, e a marca d'água chegar,
	// antes de o Submit voltar. Submit é serial por partição, então a sequência é exata.
	c.mu.Lock()
	if p.next-p.base >= c.window && !p.windowPaused {
		p.windowPaused = true
		c.holdPause(p)
	}
	seq, epoch := p.next, p.epoch
	p.recs[seq] = r
	c.mu.Unlock()

	task := elastic.Task[string]{Stream: int(r.Partition), Key: string(r.Key), Run: func() {
		if msgErr != nil {
			slog.ErrorContext(ctx, "consumer: mensagem inválida descartada", "partition", r.Partition, "offset", r.Offset, "error", msgErr)
			c.metrics.Record("invalid")
			return
		}
		if !c.begin(p, epoch) { // a partição foi revogada depois de submetida: não é mais nossa
			c.markIncomplete(p, seq)
			return
		}
		defer c.end(p)
		if ctx.Err() != nil || !c.process(ctx, p, epoch, handle, msg, r) {
			c.markIncomplete(p, seq)
		}
	}}
	if _, err := pool.Submit(ctx, task); err != nil {
		c.mu.Lock()
		delete(p.recs, seq)
		c.mu.Unlock()
		return
	}
	c.mu.Lock()
	p.next++
	c.mu.Unlock()
}

func (c *Consumer) partition(id int32) *partition {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.parts[id]
	if p == nil {
		p = &partition{id: id, recs: make(map[uint64]*kgo.Record)}
		c.parts[id] = p
	}
	return p
}

// begin marca a tarefa como dentro do handler. Falso se a partição mudou de época (revoke).
func (c *Consumer) begin(p *partition, epoch uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p.epoch != epoch {
		return false
	}
	p.inflight++
	return true
}

func (c *Consumer) end(p *partition) {
	c.mu.Lock()
	p.inflight--
	c.mu.Unlock()
}

func (c *Consumer) stale(p *partition, epoch uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return p.epoch != epoch
}

// holdPause e releasePause contam quem precisa da partição pausada; o cliente só é chamado nas
// bordas (0 -> 1 pausa, 1 -> 0 retoma). Chamar com c.mu preso.
func (c *Consumer) holdPause(p *partition) {
	p.pauses++
	if p.pauses == 1 {
		c.client.PauseFetchPartitions(map[string][]int32{c.topic: {p.id}})
	}
}

func (c *Consumer) releasePause(p *partition) {
	p.pauses--
	if p.pauses == 0 {
		c.client.ResumeFetchPartitions(map[string][]int32{c.topic: {p.id}})
	}
}

// markIncomplete anota que a tarefa seq não terminou. Roda antes de a tarefa voltar ao pool, então
// antes de a marca d'água poder passar por ela. Uma sequência abaixo de base já foi esquecida
// (revoke) e não conta.
func (c *Consumer) markIncomplete(p *partition, seq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seq < p.base {
		return
	}
	if !p.hasIncomplete || seq < p.incomplete {
		p.incomplete, p.hasIncomplete = seq, true
	}
}

// onWatermark guarda até onde a partição já pode ser confirmada: a marca d'água do pool, nunca
// além da primeira tarefa incompleta. Só anota; quem confirma é o committer.
func (c *Consumer) onWatermark(stream int, upTo uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.parts[int32(stream)]
	if p == nil {
		return
	}
	if p.hasIncomplete {
		if p.incomplete == 0 {
			return
		}
		upTo = min(upTo, p.incomplete-1)
	}
	if !p.hasTarget || upTo > p.target {
		p.target, p.hasTarget = upTo, true
	}
}

// committer confirma os offsets em lote a cada commitEvery e uma última vez ao parar.
func (c *Consumer) committer(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	tick := time.NewTicker(commitEvery)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			c.commit()
		case <-stop:
			c.commit()
			return
		}
	}
}

func (c *Consumer) commit() {
	c.commitMu.Lock()
	defer c.commitMu.Unlock()
	c.commitLocked()
}

// commitLocked confirma, de uma vez, o maior registro confirmável de cada partição. Com commitMu preso.
func (c *Consumer) commitLocked() {
	type pending struct {
		p    *partition
		upTo uint64
	}
	var (
		rs   []*kgo.Record
		sent []pending
	)
	c.mu.Lock()
	for _, p := range c.parts {
		if !p.hasTarget || p.target < p.base {
			continue
		}
		if rec := p.recs[p.target]; rec != nil {
			rs = append(rs, rec)
			sent = append(sent, pending{p, p.target})
		}
	}
	c.mu.Unlock()
	if len(rs) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.client.CommitRecords(ctx, rs...); err != nil {
		slog.Error("consumer: commit falhou, as mensagens serão repetidas", "error", err)
		c.metrics.Commit("error")
		return
	}
	c.metrics.Commit("ok")
	c.mu.Lock()
	for _, s := range sent {
		for seq := s.p.base; seq <= s.upTo; seq++ {
			delete(s.p.recs, seq)
		}
		if s.upTo+1 > s.p.base {
			s.p.base = s.upTo + 1
		}
		if s.p.windowPaused && s.p.next-s.p.base < c.window {
			s.p.windowPaused = false
			c.releasePause(s.p)
		}
	}
	c.mu.Unlock()
}

// assigned só conta o evento: a partição nova começa vazia e o pool numera a partir de onde estava.
func (c *Consumer) assigned(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	if len(m[c.topic]) > 0 {
		c.metrics.Rebalance("assigned")
	}
}

// revoked é o callback de revoke do grupo. Ver o comentário do arquivo.
func (c *Consumer) revoked(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	ids := m[c.topic]
	if len(ids) == 0 {
		return
	}
	c.bumpEpoch(ids)
	running := c.waitIdle(ids)
	if running > 0 {
		slog.Warn("consumer: partição revogada com tarefas ainda no handler; serão repetidas pelo novo dono",
			"partitions", ids, "running", running)
	}
	c.commitMu.Lock()
	c.commitLocked()
	c.forget(ids)
	c.commitMu.Unlock()
	c.metrics.Rebalance("revoked")
	if c.afterRevoke != nil {
		c.afterRevoke(ids, running)
	}
}

// lost é o callback de partição perdida (erro fatal do grupo): o commit não vai valer, então só esquece.
func (c *Consumer) lost(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	ids := m[c.topic]
	if len(ids) == 0 {
		return
	}
	c.bumpEpoch(ids)
	c.commitMu.Lock()
	c.forget(ids)
	c.commitMu.Unlock()
	c.metrics.Rebalance("lost")
}

// bumpEpoch faz as tarefas já submetidas dessas partições deixarem de ser tratadas.
func (c *Consumer) bumpEpoch(ids []int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range ids {
		if p := c.parts[id]; p != nil {
			p.epoch++
		}
	}
}

// waitIdle espera as tarefas dentro do handler dessas partições terminarem, até c.drain. Devolve
// quantas ainda estavam rodando.
func (c *Consumer) waitIdle(ids []int32) int {
	deadline := time.Now().Add(c.drain)
	for {
		c.mu.Lock()
		running := 0
		for _, id := range ids {
			if p := c.parts[id]; p != nil {
				running += p.inflight
			}
		}
		c.mu.Unlock()
		if running == 0 || time.Now().After(deadline) {
			return running
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// forget joga fora o estado de confirmação dessas partições: o que foi confirmado já saiu, o resto
// volta com quem assumir. Com commitMu preso, para nenhum commit em andamento desfazer isto.
func (c *Consumer) forget(ids []int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range ids {
		p := c.parts[id]
		if p == nil {
			continue
		}
		p.recs = make(map[uint64]*kgo.Record)
		p.base = p.next
		p.hasTarget, p.hasIncomplete = false, false
		if p.windowPaused {
			p.windowPaused = false
			c.releasePause(p)
		}
	}
}

// process devolve true se o registro foi concluído (seu offset pode ser confirmado) e false
// apenas se o contexto terminou ou a partição foi revogada antes de concluí-lo.
func (c *Consumer) process(ctx context.Context, p *partition, epoch uint64, handle Handler, msg app.Message, r *kgo.Record) bool {
	backoff := minRetryBackoff
	for {
		if c.stale(p, epoch) {
			return false
		}
		// O handler termina o que começou mesmo durante o desligamento: depois do envio,
		// abandonar no meio deixaria a instrução sem resposta gravada. O prazo impede que um
		// banco travado prenda o consumidor para sempre; o estouro é erro transitório.
		hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), handleTimeout)
		err := handle(hctx, msg)
		cancel()
		switch {
		case err == nil:
			c.metrics.Record("ok")
			return true
		case errors.Is(err, app.ErrOutcomeUnknown):
			slog.WarnContext(ctx, "consumer: resultado desconhecido, não será reenviado", "instruction_id", msg.AggregateID, "error", err)
			c.metrics.Record("unknown")
			return true
		}
		var bp *app.BackpressureError
		if errors.As(err, &bp) {
			c.metrics.Record("backpressure")
			c.metrics.Pause()
			if !c.pauseAndWait(ctx, p, bp.After) {
				return false
			}
			continue
		}
		c.metrics.Record("error")
		slog.ErrorContext(ctx, "consumer: erro transitório, repetindo a mensagem", "instruction_id", msg.AggregateID,
			"error", err, "retry_in", backoff.String())
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxRetryBackoff)
	}
}

// pauseAndWait pausa a partição, espera d e a retoma. Várias tarefas da mesma partição podem
// esperar ao mesmo tempo: a partição é pausada pela primeira e só retomada quando a última termina.
// Devolve false se o contexto terminou antes; a retomada acontece de qualquer forma, para a
// partição nunca ficar pausada.
func (c *Consumer) pauseAndWait(ctx context.Context, p *partition, d time.Duration) bool {
	c.mu.Lock()
	c.holdPause(p)
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.releasePause(p)
		c.mu.Unlock()
	}()
	slog.DebugContext(ctx, "consumer: limite de saída cheio, partição pausada", "partition", p.id, "wait", d.String())
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// toMessage extrai chave e headers; o corpo não é lido.
func toMessage(r *kgo.Record) (app.Message, error) {
	msg := app.Message{AggregateID: string(r.Key)}
	if msg.AggregateID == "" {
		return app.Message{}, errors.New("registro sem chave")
	}
	for _, h := range r.Headers {
		switch h.Key {
		case "event_type":
			msg.EventType = string(h.Value)
		case "version":
			v, err := strconv.Atoi(string(h.Value))
			if err != nil || v <= 0 {
				return app.Message{}, fmt.Errorf("header version inválido: %q", h.Value)
			}
			msg.Version = v
		}
	}
	if msg.EventType == "" || msg.Version == 0 {
		return app.Message{}, errors.New("registro sem os headers event_type e version")
	}
	return msg, nil
}

// Close sai do grupo e encerra o cliente.
func (c *Consumer) Close() {
	c.client.Close()
}
